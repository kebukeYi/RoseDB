package rosedb

import (
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DataType Define the data structure type.
type DataType = int8

// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

func (db *RoseDB) buildIndex(dataType DataType, ent *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrsIndex(ent, pos)
	case List:
		db.buildListIndex(ent, pos)
	case Hash:
		db.buildHashIndex(ent, pos)
	case Set:
		db.buildSetsIndex(ent, pos)
	case ZSet:
		db.buildZSetIndex(ent, pos)
	}
}

func (db *RoseDB) buildStrsIndex(ent *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(ent.Key)
		return
	}
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.strIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildListIndex(ent *logfile.LogEntry, pos *valuePos) {
	var listKey = ent.Key
	// 打开 list 文件 读取记录，发现不是 元数据，那么就是 key
	if ent.Type != logfile.TypeListMeta {
		listKey, _ = db.decodeListKey(ent.Key)
	}
	if db.listIndex.trees[string(listKey)] == nil {
		db.listIndex.trees[string(listKey)] = art.NewART()
	}
	idxTree := db.listIndex.trees[string(listKey)]

	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(ent.Key)
		return
	}
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildHashIndex(ent *logfile.LogEntry, pos *valuePos) {
	key, field := db.decodeKey(ent.Key)
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]

	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(field)
		return
	}

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(field, idxNode)
}

func (db *RoseDB) buildSetsIndex(ent *logfile.LogEntry, pos *valuePos) {
	if db.setIndex.trees[string(ent.Key)] == nil {
		db.setIndex.trees[string(ent.Key)] = art.NewART()
	}
	idxTree := db.setIndex.trees[string(ent.Key)]

	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(ent.Value)
		return
	}

	if err := db.setIndex.murhash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash: %v", err)
	}
	//
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	// 此时的索引树 是
	idxTree.Put(sum, idxNode)
}

func (db *RoseDB) buildZSetIndex(ent *logfile.LogEntry, pos *valuePos) {
	if ent.Type == logfile.TypeDelete {
		db.zsetIndex.indexes.ZRem(string(ent.Key), string(ent.Value))
		if db.zsetIndex.trees[string(ent.Key)] != nil {
			db.zsetIndex.trees[string(ent.Key)].Delete(ent.Value)
		}
		return
	}

	key, scoreBuf := db.decodeKey(ent.Key)
	score, _ := util.StrToFloat64(string(scoreBuf))
	if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash: %v", err)
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	idxTree := db.zsetIndex.trees[string(key)]
	if idxTree == nil {
		idxTree = art.NewART()
		db.zsetIndex.trees[string(key)] = idxTree
	}

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	idxTree.Put(sum, idxNode)
}

func (db *RoseDB) loadIndexFromLogFiles() error {
	// 当前 活跃文件 的编号一定是 所有文件编号中的最大的那一个
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()
		// 当前数据类型的 所有文件 id 集合
		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}

		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			var logFile *logfile.LogFile
			// 最后一个文件是 作为活跃文件
			if i == len(fids)-1 {
				logFile = db.activeLogFiles[dataType]
			} else {
				logFile = db.archivedLogFiles[dataType][fid]
			}

			if logFile == nil {
				logger.Fatalf("log file is nil, failed to open db")
			}

			var offset int64
			for {
				// 从文件中依次读取 entry
				entry, esize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == logfile.ErrEndOfEntry {
						break
					}
					logger.Fatalf("read log entry from file err, failed to open db")
				}
				pos := &valuePos{fid: fid, offset: offset}
				// 遍历文件中的 entry 逐一进行构建索引值
				db.buildIndex(dataType, entry, pos)
				offset = offset + esize
			}
			// set latest log file`s WriteAt.
			if i == len(fids)-1 {
				// 到了最后一个文件 说明是活跃文件，保存其文件可写位移处
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}
	}
	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for i := 0; i < logFileTypeNum; i++ {
		// 可以并发构建不同的文件索引树
		go iterateAndHandle(DataType(i), wg)
	}
	// 阻塞等待
	wg.Wait()
	return nil
}

func (db *RoseDB) updateIndexTree(idxTree *art.AdaptiveRadixTree, ent *logfile.LogEntry, pos *valuePos, sendDiscard bool, dType DataType) error {
	// 数据大小，size还有可能是 null 值
	var size = pos.entrySize
	// 数据类型 String || List，why？
	if dType == String || dType == List {
		_, size = logfile.EncodeEntry(ent)
	}
	// 构建索引值
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	// 构建索引，索引可原地更新
	// 假如存在旧值，那么 oldValue 不为空，updated 为 true；否则就是 nil false
	oldVal, updated := idxTree.Put(ent.Key, idxNode)
	// 旧值是否 记录抛弃冗余量？
	if sendDiscard {
		db.sendDiscard(oldVal, updated, dType)
	}
	return nil
}

// get index node info from an adaptive radix tree in memory.
func (db *RoseDB) getIndexNode(idxTree *art.AdaptiveRadixTree, key []byte) (*indexNode, error) {
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}
	return idxNode, nil
}

// getVal 共同函数
func (db *RoseDB) getVal(idxTree *art.AdaptiveRadixTree, key []byte, dataType DataType) ([]byte, error) {
	// Get index info from an adaptive radix tree in memory.
	// 先去内存中 尝试获得数据的保存信息
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}

	ts := time.Now().Unix()
	if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
		return nil, ErrKeyNotFound
	}
	// In KeyValueMemMode, the value will be stored in memory.
	// So get the value from the index info.
	if db.opts.IndexMode == KeyValueMemMode && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}

	// In KeyOnlyMemMode, the value not in memory, so get the value from log file at the offset.
	// 获得这个数据类型的 内存活跃文件
	logFile := db.getActiveLogFile(dataType)
	// 判断是否跟 在内存中的一致，有可能之前的活跃文件 已经 存储 转成 非活跃文件
	if logFile.Fid != idxNode.fid {
		// 不一致的话，就从 非活跃文件中寻找
		logFile = db.getArchivedLogFile(dataType, idxNode.fid)
	}

	if logFile == nil {
		return nil, ErrLogFileNotFound
	}
	// 从文件中 获得数据
	ent, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}

	// key exists, but is invalid(deleted or expired)
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		return nil, ErrKeyNotFound
	}
	return ent.Value, nil
}
