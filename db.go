package rosedb

import (
	"encoding/binary"
	"errors"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/ds/zset"
	"github.com/flower-corp/rosedb/flock"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrKeyNotFound key not found
	ErrKeyNotFound = errors.New("key not found")

	// ErrLogFileNotFound log file not found
	ErrLogFileNotFound = errors.New("log file not found")

	// ErrWrongNumberOfArgs doesn't match key-value pair numbers
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")

	// ErrIntegerOverflow overflows int64 limitations
	ErrIntegerOverflow = errors.New("increment or decrement overflow")

	// ErrWrongValueType value is not a number
	ErrWrongValueType = errors.New("value is not an integer")

	// ErrWrongIndex index is out of range
	ErrWrongIndex = errors.New("index is out of range")

	// ErrGCRunning log file gc is running
	ErrGCRunning = errors.New("log file gc is running, retry later")
)

const (
	logFileTypeNum   = 5
	encodeHeaderSize = 10
	initialListSeq   = math.MaxUint32 / 2
	discardFilePath  = "DISCARD"
	lockFileName     = "FLOCK"
)

type (
	// RoseDB a db instance. 作为一个数据库，提供给外界的接口 API ，简单的例如 增删改查
	RoseDB struct {
		//每个数据类型都有 对应的 活跃文件 单个 头指针 < DataType,*LogFile>
		activeLogFiles map[DataType]*logfile.LogFile
		//每个数据类型都有 对应的非活跃(磁盘)文件数组 < DataType,<uint32,*LogFile> >
		archivedLogFiles map[DataType]archivedFiles
		//仅在启动时使用，即使日志文件更改也不会更新，Fid map < DataType ,[uint32] >
		fidMap map[DataType][]uint32 // only used at startup, never update even though log files changed.
		//记录冗余数据的文件 < DataType,*discard >
		discards map[DataType]*discard // 多个类型的过期字典

		opts      Options              // 参数
		strIndex  *strIndex            // String indexes(adaptive-radix-tree).
		listIndex *listIndex           // List indexes.
		hashIndex *hashIndex           // Hash indexes.
		setIndex  *setIndex            // Set indexes.
		zsetIndex *zsetIndex           // Sorted set indexes.
		mu        sync.RWMutex         // 数据库粒度-读写锁
		fileLock  *flock.FileLockGuard // 数据库粒度-文件锁
		closed    uint32               // 是否关闭
		gcState   int32                // gc 标志  1  0
	}

	// 活跃文件 数组
	archivedFiles map[uint32]*logfile.LogFile

	// 由于 key 在内存中，value 在文件中，因此需要 保存 value 的文件位移索引值，以便去寻找
	valuePos struct {
		fid       uint32 // 所在哪个id的文件中
		offset    int64  // 在文件中的位移
		entrySize int    // 数据结构大小
	}

	// 字符串 索引
	strIndex struct {
		mu      *sync.RWMutex          // 索引粒度-读写锁
		idxTree *art.AdaptiveRadixTree // 自适应基数树
	}

	// 内存中索引类型的结构  String || List
	indexNode struct {
		value     []byte // 字节数组，根据配置 可有可无
		fid       uint32 // 所在文件 id
		offset    int64  // 所在文件中的位移量
		entrySize int    // 实际数据结构大小
		expiredAt int64  // 存活时间
	}

	// list 结构 索引 key value1 value2 value3 value4
	listIndex struct {
		mu    *sync.RWMutex                     // 读写锁
		trees map[string]*art.AdaptiveRadixTree // [key][value1,value2,value3,value4]，将之前的value类型从map换成了tree，有什么提升点?
		// go 中的 map 底层实现原理是 hash 结构，并使用链地址法解决 hash 冲突，key相同就走覆盖，key不同就是冲突，tree 相比 普通的链表，是否有了短暂提升？
	}

	// hash 结构 索引 key filed1 value1 filed2 value2 filed3 value3 filed4 value4
	hashIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}

	// set 结构 索引 key value1 value2 value3 value4 无重复，怎么达到？使用hash
	setIndex struct {
		mu      *sync.RWMutex
		murhash *util.Murmur128 // hash 函数
		trees   map[string]*art.AdaptiveRadixTree
	}

	// 有序 set 数据结构 key value1 value2 value3 value4 怎么保证有序时，还无重复
	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
}

func newListIdx() *listIndex {
	return &listIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newHashIdx() *hashIndex {
	return &hashIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newSetIdx() *setIndex {
	return &setIndex{
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
}

func newZSetIdx() *zsetIndex {
	return &zsetIndex{
		indexes: zset.New(),
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
}

// Open a roseDB instance. You must call Close after using it
func Open(opts Options) (*RoseDB, error) {
	// create the dir path if not exists.
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// acquire file lock to prevent multiple processes from accessing the same directory.
	lockPath := filepath.Join(opts.DBPath, lockFileName)
	// 尝试获得文件锁
	lockGuard, err := flock.AcquireFileLock(lockPath, false)
	if err != nil {
		return nil, err
	}

	db := &RoseDB{
		activeLogFiles:   make(map[DataType]*logfile.LogFile),
		archivedLogFiles: make(map[DataType]archivedFiles),
		opts:             opts,
		fileLock:         lockGuard,
		strIndex:         newStrsIndex(),
		listIndex:        newListIdx(),
		hashIndex:        newHashIdx(),
		setIndex:         newSetIdx(),
		zsetIndex:        newZSetIdx(),
	}

	// 目的是用来记录每个数据文件中 冗余数据量
	// init discard file. 初始化各个数据类型的 过期字典，假如存在的话，就打开
	if err := db.initDiscard(); err != nil {
		return nil, err
	}

	// load the log files from disk. 加载每个类型的所有文件的句柄到内存中
	if err := db.loadLogFiles(); err != nil {
		return nil, err
	}

	// load indexes from log files. 构建索引
	if err := db.loadIndexFromLogFiles(); err != nil {
		return nil, err
	}

	// handle log files garbage collection.
	go db.handleLogFileGC()
	return db, nil
}

// Close db and save relative configs.
func (db *RoseDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 公用 文件锁
	if db.fileLock != nil {
		_ = db.fileLock.Release()
	}
	// close and sync the active file.
	for _, activeFile := range db.activeLogFiles {
		_ = activeFile.Close()
	}
	// close the archived files.
	for _, archived := range db.archivedLogFiles {
		for _, file := range archived {
			_ = file.Sync()
			_ = file.Close()
		}
	}
	// close discard channel.
	for _, dis := range db.discards {
		dis.closeChan()
	}
	atomic.StoreUint32(&db.closed, 1)
	db.strIndex = nil
	db.hashIndex = nil
	db.listIndex = nil
	db.zsetIndex = nil
	db.setIndex = nil
	return nil
}

// Sync persist the db files to stable storage.
func (db *RoseDB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// iterate and sync all the active files.
	for _, activeFile := range db.activeLogFiles {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	// sync discard file.
	for _, dis := range db.discards {
		if err := dis.sync(); err != nil {
			return err
		}
	}
	return nil
}

// Backup copies the db directory to the given path for backup. 备份
// It will create the path if it does not exist.
func (db *RoseDB) Backup(path string) error {
	// if log file gc is running, can not back Up the db.
	if atomic.LoadInt32(&db.gcState) > 0 {
		return ErrGCRunning
	}

	if err := db.Sync(); err != nil {
		return err
	}
	if !util.PathExist(path) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return err
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return util.CopyDir(db.opts.DBPath, path)
}

// RunLogFileGC run log file garbage collection manually.
func (db *RoseDB) RunLogFileGC(dataType DataType, fid int, gcRatio float64) error {
	if atomic.LoadInt32(&db.gcState) > 0 {
		return ErrGCRunning
	}
	return db.doRunGC(dataType, fid, gcRatio)
}

func (db *RoseDB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

func (db *RoseDB) getActiveLogFile(dataType DataType) *logfile.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFiles[dataType]
}

func (db *RoseDB) getArchivedLogFile(dataType DataType, fid uint32) *logfile.LogFile {
	var lf *logfile.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.archivedLogFiles[dataType] != nil {
		lf = db.archivedLogFiles[dataType][fid]
	}
	return lf
}

// write entry to log file.
// todo string 数据类型 写流程
func (db *RoseDB) writeLogEntry(ent *logfile.LogEntry, dataType DataType) (*valuePos, error) {
	// 初始化 插入类型 文件
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}
	// 获得 活跃文件 句柄 || mmap 映射的内存地址区间
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil, ErrLogFileNotFound
	}
	// 相关参数
	opts := db.opts
	// 进行 str 数据结构 编码 成 字节数组
	entBuf, esize := logfile.EncodeEntry(ent)
	// 假如活跃文件大小 + 将要填充的文件大小 达到了阈值，则关闭该文件，并重新打开一个文件
	if activeLogFile.WriteAt+int64(esize) > opts.LogFileSizeThreshold {
		// 刷盘一波，会一直阻塞
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
		// 数据库 层面的锁
		db.mu.Lock()
		// save the old log file in archived files.
		activeFileId := activeLogFile.Fid
		// 并没有 相关的不活跃文件，说明第一次落盘
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}

		// 新增不活跃的数组索引
		// map[string][File_id] = activeLogFile
		db.archivedLogFiles[dataType][activeFileId] = activeLogFile

		// open a new log file.
		ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
		// 重新打开 创建文件
		lf, err := logfile.OpenLogFile(opts.DBPath, activeFileId+1, opts.LogFileSizeThreshold, ftype, iotype)
		// 打开文件报错，记得释放锁，返回错误
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}

		// 每次新建文件时，初始化一次过期字典（注意：什么时机去进行文件合并的）
		db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
		// 更新新活跃文件
		db.activeLogFiles[dataType] = lf
		activeLogFile = lf
		db.mu.Unlock()
	} // 文件大小不够 创建新文件 over

	// 取地址 原子加载？
	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	// write entry and sync(if necessary)
	if err := activeLogFile.Write(entBuf); err != nil {
		return nil, err
	}
	// 是否写一个数据就刷盘一次？
	if opts.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 写完后，返回一个 记录数据的完整信息，没有大小？？？
	return &valuePos{fid: activeLogFile.Fid, offset: writeAt}, nil
}

// 加载数据文件到内存中，恢复 活跃文件 非活跃文件 使用
func (db *RoseDB) loadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// 多个文件
	fileInfos, err := os.ReadDir(db.opts.DBPath)

	if err != nil {
		return err
	}

	fidMap := make(map[DataType][]uint32)
	// 记录所有文件名字序号
	for _, file := range fileInfos {
		// 前缀是 log.
		if strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := DataType(logfile.FileTypesMap[splitNames[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}

	db.fidMap = fidMap
	// 遍历 不同的数据类型 下的 多个文件
	for dataType, fids := range fidMap {
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}
		// load log file in order. 对磁盘中的 不同数据类型的文件进行排序
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		opts := db.opts
		for i, fid := range fids {
			ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
			// 只是打开文件 或者 mmap 映射文件 来获得文件fd 文件buf 文件大小
			logFile, err := logfile.OpenLogFile(opts.DBPath, fid, opts.LogFileSizeThreshold, ftype, iotype)
			if err != nil {
				return err
			}
			// latest one is active log file. 最大的 fid 就是 活跃文件
			// 活跃的 非活跃的 全都在内存中
			if i == len(fids)-1 {
				// 每个类型的最大 FID 作为这个类型的 活跃日志文件
				db.activeLogFiles[dataType] = logFile
			} else {
				// 其他文件则作为 数组 保存起来
				db.archivedLogFiles[dataType][fid] = logFile
			}
		}
	}
	return nil
}

func (db *RoseDB) initLogFile(dataType DataType) error {
	db.mu.Lock() // 数据库层面的锁
	defer db.mu.Unlock()
	// 已经初始化完毕了，直接返回
	if db.activeLogFiles[dataType] != nil {
		return nil
	}
	// 获得 db 的启动参数
	opts := db.opts
	// 表示不同数据类型的文件 || 文件读写类型
	ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
	// 打开文件 || 新建文件
	logFile, err := logfile.OpenLogFile(opts.DBPath, logfile.InitialLogFileId, opts.LogFileSizeThreshold, ftype, iotype)
	if err != nil {
		return err
	}
	// 针对 string 类型的活跃文件，初始化 并 统计冗余数据 文件
	db.discards[dataType].setTotal(logFile.Fid, uint32(opts.LogFileSizeThreshold))
	// 设置类型的活跃文件
	db.activeLogFiles[dataType] = logFile
	return nil
}

// 初始化 过期字典
func (db *RoseDB) initDiscard() error {
	discardPath := filepath.Join(db.opts.DBPath, discardFilePath)
	// 不存在就 新建
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}
	// 新建 动态空间 <DataType , discard>
	discards := make(map[DataType]*discard)
	// 不同的数据类型 拥有 不同的 过期字典文件 5种文件
	for i := String; i < logFileTypeNum; i++ {
		// log.strs.discard 不同类型的文件等等
		name := logfile.FileNamesMap[logfile.FileType(i)] + discardFileName
		dis, err := newDiscard(discardPath, name, db.opts.DiscardBufferSize)
		if err != nil {
			return err
		}
		discards[i] = dis
	}
	// 数据库层面的 过期字典
	db.discards = discards
	return nil
}

func (db *RoseDB) encodeKey(key, subKey []byte) []byte {
	header := make([]byte, encodeHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(subKey)))
	length := len(key) + len(subKey)
	if length > 0 {
		buf := make([]byte, length+index)
		copy(buf[:index], header[:index])
		copy(buf[index:index+len(key)], key)
		copy(buf[index+len(key):], subKey)
		return buf
	}
	return header[:index]
}

func (db *RoseDB) decodeKey(key []byte) ([]byte, []byte) {
	var index int
	keySize, i := binary.Varint(key[index:])
	index += i
	_, i = binary.Varint(key[index:])
	index += i
	sep := index + int(keySize)
	return key[index:sep], key[sep:]
}

func (db *RoseDB) sendDiscard(oldVal interface{}, updated bool, dataType DataType) {
	// updated 是 false 时说明是插入的，并不是更新的，因此可以直接返回;
	// 否则就是 更新 || 删除
	if !updated || oldVal == nil {
		return
	}
	// 更新 || 删除
	// 强制转类型
	node, _ := oldVal.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	select {
	// 向对应的类型丢弃通道中 发送数据
	case db.discards[dataType].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
}

func (db *RoseDB) handleLogFileGC() {
	if db.opts.LogFileGCInterval <= 0 {
		return
	}
	// 创建信号
	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// 周期器
	ticker := time.NewTicker(db.opts.LogFileGCInterval)

	defer ticker.Stop()
	for {
		select {
		// 周期性唤醒
		case <-ticker.C:
			if atomic.LoadInt32(&db.gcState) > 0 {
				logger.Warn("log file gc is running, skip it")
				break
			}
			for dType := String; dType < logFileTypeNum; dType++ {
				// 每个类型 开一个 GC 协程去做
				go func(dataType DataType) {
					// 文件合并GC
					err := db.doRunGC(dataType, -1, db.opts.LogFileGCRatio)
					if err != nil {
						logger.Errorf("log file gc err, dataType: [%v], err: [%v]", dataType, err)
					}
				}(dType)
			}
		// 这是啥？？？
		case <-quitSig:
			return
		}
	}
}

func (db *RoseDB) doRunGC(dataType DataType, specifiedFid int, gcRatio float64) error {
	atomic.AddInt32(&db.gcState, 1)
	defer atomic.AddInt32(&db.gcState, -1)

	maybeRewriteStrs := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.strIndex.mu.Lock()
		defer db.strIndex.mu.Unlock()
		indexVal := db.strIndex.idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, String)
			if err != nil {
				return err
			}
			// update index
			if err = db.updateIndexTree(db.strIndex.idxTree, ent, valuePos, false, String); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteList := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.listIndex.mu.Lock()
		defer db.listIndex.mu.Unlock()
		var listKey = ent.Key
		if ent.Type != logfile.TypeListMeta {
			listKey, _ = db.decodeListKey(ent.Key)
		}
		if db.listIndex.trees[string(listKey)] == nil {
			return nil
		}
		idxTree := db.listIndex.trees[string(listKey)]
		indexVal := idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, List)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, ent, valuePos, false, List); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteHash := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.hashIndex.mu.Lock()
		defer db.hashIndex.mu.Unlock()
		key, field := db.decodeKey(ent.Key)
		if db.hashIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.hashIndex.trees[string(key)]
		indexVal := idxTree.Get(field)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, Hash)
			if err != nil {
				return err
			}
			// update index
			entry := &logfile.LogEntry{Key: field, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, Hash); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteSets := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.setIndex.mu.Lock()
		defer db.setIndex.mu.Unlock()
		if db.setIndex.trees[string(ent.Key)] == nil {
			return nil
		}
		idxTree := db.setIndex.trees[string(ent.Key)]
		if err := db.setIndex.murhash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, Set)
			if err != nil {
				return err
			}
			// update index
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, Set); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteZSet := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.zsetIndex.mu.Lock()
		defer db.zsetIndex.mu.Unlock()
		key, _ := db.decodeKey(ent.Key)
		if db.zsetIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.zsetIndex.trees[string(key)]
		if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.zsetIndex.murhash.EncodeSum128()
		db.zsetIndex.murhash.Reset()

		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, ZSet)
			if err != nil {
				return err
			}
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, ZSet); err != nil {
				return err
			}
		}
		return nil
	}

	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil
	}
	// 保存无效数据文件 的 刷盘
	if err := db.discards[dataType].sync(); err != nil {
		return err
	}
	// 从 discards[dataType] 获得 符合 被丢弃文件占比的文件
	ccl, err := db.discards[dataType].getCCL(activeLogFile.Fid, gcRatio)
	if err != nil {
		return err
	}
	// 按升序排序，确保较旧的文件先压缩
	for _, fid := range ccl {
		// 找到特定的 GC 文件Fid
		if specifiedFid >= 0 && uint32(specifiedFid) != fid {
			// 遍历下一个文件
			continue
		}
		// 获得不活跃文件 句柄
		archivedFile := db.getArchivedLogFile(dataType, fid)
		if archivedFile == nil {
			continue
		}

		var offset int64
		for {
			// 不活跃文件中，从 0 开始读取，逐一读取 entry
			ent, size, err := archivedFile.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}
			var off = offset
			offset += size
			if ent.Type == logfile.TypeDelete {
				// 下一条记录
				continue
			}
			ts := time.Now().Unix()
			if ent.ExpiredAt != 0 && ent.ExpiredAt <= ts {
				// 当前数据过期了，也执行下一条数据
				continue
			}
			var rewriteErr error
			switch dataType {
			case String:
				rewriteErr = maybeRewriteStrs(archivedFile.Fid, off, ent)
			case List:
				rewriteErr = maybeRewriteList(archivedFile.Fid, off, ent)
			case Hash:
				rewriteErr = maybeRewriteHash(archivedFile.Fid, off, ent)
			case Set:
				rewriteErr = maybeRewriteSets(archivedFile.Fid, off, ent)
			case ZSet:
				rewriteErr = maybeRewriteZSet(archivedFile.Fid, off, ent)
			}
			if rewriteErr != nil {
				return rewriteErr
			}
		} // for over

		// delete older log file.
		db.mu.Lock()
		// 内存中进行删除 旧fid文件
		archivedFiles := db.archivedLogFiles[dataType]
		delete(archivedFiles, fid)
		// 文件删除
		_ = archivedFile.Delete()
		db.mu.Unlock()
		// clear discard state. 过期文件中也进行删除
		db.discards[dataType].clear(fid)
	}
	return nil
}
