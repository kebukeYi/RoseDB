package rosedb

import (
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"sync"

	"github.com/flower-corp/rosedb/ioselector"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
)

const (
	// 12 字节
	discardRecordSize = 12
	// 8kb, contains mostly 682 records in file.
	discardFileSize int64 = 2 << 12
	discardFileName       = "discard"
)

// ErrDiscardNoSpace no enough space for discard file.
var ErrDiscardNoSpace = errors.New("not enough space can be allocated for the discard file")

// Discard is used to record total size and discarded size in a log file.
// Discard 用于记录 日志文件中的总大小和丢弃大小
// Mainly for log files compaction.
type discard struct {
	sync.Mutex                       // 过期字典的所有操作 都加锁
	once       *sync.Once            // 用来关闭 通道
	valChan    chan *indexNode       // 传送数据，假如不用通道的话，那么是不是得需要一个 容器，当很多线程想add时，是不是就是得 加锁阻塞等待
	file       ioselector.IOSelector // 过期字典的 文件映射方式
	freeList   []int64               // contains file offset that can be allocated 包含可分配的文件偏移量，原地置空+freeOffsetList 虽然牺牲了空间，但是优化了时间
	location   map[uint32]int64      // offset of each fid 每个fid在discard文件中的的偏移量(索引值)，因为他记录了很多 fid 文件
}

func newDiscard(path, name string, bufferSize int) (*discard, error) {
	// log.strs.discard
	fname := filepath.Join(path, name)
	// 建立内存虚拟映射文件 8KB 最多容纳682个记录
	file, err := ioselector.NewMMapSelector(fname, discardFileSize)
	if err != nil {
		return nil, err
	}
	// 保存 可用元素 首地址 数组
	var freeItemOffsetArray []int64
	// 元素首地址
	var itemOffset int64
	// <fid_1,offset>,<fid_2,offset>,<fid_3,offset>
	location := make(map[uint32]int64)
	for {
		// 只是每次读取 8 字节，说明什么？说明首次打开过期字典，只需这两个信息构建索引数组，后面4字节的信息，当前不需要
		// read fid and total is enough.
		buf := make([]byte, 8)
		// 读取 8 字节
		if _, err := file.Read(buf, itemOffset); err != nil {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		// 前 4 字节 转换为 文件ID
		fid := binary.LittleEndian.Uint32(buf[:4])
		// 后 4 字节 转换为 fid文件总大小，默认 512MB
		total := binary.LittleEndian.Uint32(buf[4:8])
		// 说明当前offset处没有用来记录(但是下一个位移处 不见得也没有,因此继续向下遍历)
		if fid == 0 && total == 0 {
			// 保存起来 log.strs.discard 文件的 offset
			// 如果没有冗余记录，那么就将当前 offset 记录在空闲数组中，以备下次使用
			freeItemOffsetArray = append(freeItemOffsetArray, itemOffset)
		} else {
			// 存在冗余数据记录，建立 <fid_* ,offset> 映射
			location[fid] = itemOffset
		}
		// 下一个 Item entry
		// log.strs.discard 文件位移递增
		itemOffset = itemOffset + discardRecordSize
	}
	// 读到文件末尾了
	d := &discard{
		valChan:  make(chan *indexNode, bufferSize), // 接收错误信息 通道 数据类型是 indexNode
		once:     new(sync.Once),                    // 待定
		file:     file,
		freeList: freeItemOffsetArray,
		location: location,
	}
	// 由于每种数据类型都 new 一个新的过期字典，因此就开启一个协程来专一监听 过期消息，总共5个协程
	go d.listenUpdates()
	return d, nil
}

func (d *discard) sync() error {
	return d.file.Sync()
}

func (d *discard) close() error {
	return d.file.Close()
}

// CCL means compaction candidate list. CCL表示压缩候选列表
// iterate and find the file with most discarded data, 迭代并找到丢弃数据最多的文件
// there are 682 records at most, no need to worry about the performance. 最多682条记录，不用担心性能问题
func (d *discard) getCCL(activeFid uint32, ratio float64) ([]uint32, error) {
	var offset int64
	var ccl []uint32
	d.Lock()
	defer d.Unlock()
	for {
		buf := make([]byte, discardRecordSize)
		// 读取 12 字节
		_, err := d.file.Read(buf, offset)
		if err != nil {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		offset += discardRecordSize

		fid := binary.LittleEndian.Uint32(buf[:4])
		totalSize := binary.LittleEndian.Uint32(buf[4:8])
		discardSize := binary.LittleEndian.Uint32(buf[8:12])
		var curRatio float64
		if totalSize != 0 && discardSize != 0 {
			curRatio = float64(discardSize) / float64(totalSize)
		}
		// 仅仅针对 不活跃文件 GC ，活跃文件不进行 GC
		if curRatio >= ratio && fid != activeFid {
			ccl = append(ccl, fid)
		}
	}

	// sort in ascending order, guarantee the older file will compact firstly.
	// 按升序排序，确保 冗余数据多的 的文件先压缩
	sort.Slice(ccl, func(i, j int) bool {
		return ccl[i] < ccl[j]
	})
	return ccl, nil
}

func (d *discard) listenUpdates() {
	for {
		select {
		// 执行更新 ||删除 key 时，将 oldValue 发送到这里
		case idxNode, ok := <-d.valChan:
			if !ok {
				if err := d.file.Close(); err != nil {
					logger.Errorf("close discard file err: %v", err)
				}
				return
			}
			// 增加 冗余数据值
			d.incrDiscard(idxNode.fid, idxNode.entrySize)
		}
	}
}

func (d *discard) closeChan() {
	// 用来关闭 通道
	d.once.Do(func() {
		close(d.valChan)
	})
}

// 设置 fid 数据中的 total 字段数据
func (d *discard) setTotal(fid uint32, totalSize uint32) {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.location[fid]; ok {
		return
	}
	offset, err := d.alloc(fid)
	if err != nil {
		logger.Errorf("discard file allocate err: %+v", err)
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], fid)
	binary.LittleEndian.PutUint32(buf[4:8], totalSize)
	if _, err = d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err: %v", err)
		return
	}
}

func (d *discard) clear(fid uint32) {
	// 过期文件中进行 原地数据置位空，但是并没有 使得空闲元素 增加
	d.incr(fid, -1)
	d.Lock()
	// 将删除后的 offset 添加到 空闲元素 队列中，方便下次利用
	if offset, ok := d.location[fid]; ok {
		d.freeList = append(d.freeList, offset)
		// 主要是删除这个映射，下次使用时再去创建新的
		delete(d.location, fid)
	}
	d.Unlock()
}

func (d *discard) incrDiscard(fid uint32, delta int) {
	// delta 一般是 数据的大小长度，用于统计 冗余数据 总量大小
	if delta > 0 {
		d.incr(fid, delta)
	}
}

// format of discard file` record:
// +-------+--------------+----------------+  +-------+--------------+----------------+
// |  fid  |  total size  | discarded size |  |  fid  |  total size  | discarded size |
// +-------+--------------+----------------+  +-------+--------------+----------------+
// 0-------4--------------8---------------12  12------16------------20----------------24
func (d *discard) incr(fid uint32, delta int) {
	// 直接当前 数据类型字典 上锁
	d.Lock()
	defer d.Unlock()
	// 这个 fid 文件中存在了过期数据
	// 分配这个 fid 的元素位移处 || 可能存在 可能不存在
	offset, err := d.alloc(fid)
	if err != nil {
		logger.Errorf("discard file allocate err: %+v", err)
		return
	}
	var buf []byte
	// 正常 更新||删除操作 下 delta 代表的是数据的长度 > 0
	if delta > 0 {
		buf = make([]byte, 4)
		// |  fid  |  total size  | discarded size |
		offset = offset + 8
		// discarded size
		if _, err := d.file.Read(buf, offset); err != nil {
			logger.Errorf("incr value in discard err:%v", err)
			return
		}
		// 原来的 字节转为 uint32
		v := binary.LittleEndian.Uint32(buf)
		// 小端: 旧的 + 新的 大小
		binary.LittleEndian.PutUint32(buf, v+uint32(delta))
	} else {
		// clear() 操作
		// delta == -1
		buf = make([]byte, discardRecordSize)
	}

	// 再写入文件中
	// 1. 普通 过期数据 追加
	// 2. 文件被合并||删除 执行原地清空操作(freeList就会增加)，原地清空方便了下次直接利用
	if _, err := d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err:%v", err)
		return
	}
}

// must hold the lock before invoking 再被调用前 必须加锁
// 在 discard 文件中，分配一个记录位置，用来统计记录当前 fid 文件中的冗余数据量
func (d *discard) alloc(fid uint32) (int64, error) {
	// 判断是否存在 登记过 这个文件
	// 存在的话，返回这个 文件所在的过期字典中的 offset
	if offset, ok := d.location[fid]; ok {
		return offset, nil
	}
	// 说明 不存在 location[fid]
	// 再来判断是否 有空余的元素 位置
	if len(d.freeList) == 0 {
		// 当前文件中的元素，填充完毕了，怎么办？
		return 0, ErrDiscardNoSpace
	}
	// 需要新登记 location[fid] && 存在空余元素位置 len(d.freeList) != 0
	// 获得空闲列表中的最后一个元素所代表的位置
	offset := d.freeList[len(d.freeList)-1]
	// 从空闲列表中，拿去一个空闲元素，-1
	d.freeList = d.freeList[0 : len(d.freeList)-1]
	// 建立 fid 文件 location[fid]
	d.location[fid] = offset
	return offset, nil
}
