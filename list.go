package rosedb

import (
	"bytes"
	"encoding/binary"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"math"
)

// LPush insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *RoseDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	// 每一个 原生key 都设置 一个 tree
	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	// key value1 value2 value3 value4 value5
	// key1 value1 key2 value2 key3 value3
	for _, val := range values {
		// 逐个 value put
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// LPushX insert specified values at the head of the list stored at key,
// only if key already exists and holds a list.
// In contrary to LPUSH, no operation will be performed when key does not yet exist.
func (db *RoseDB) LPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	// key 不存在，直接返回报错
	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}

	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// RPush insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (db *RoseDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

// RPushX insert specified values at the tail of the list stored at key,
// only if key already exists and holds a list.
// In contrary to RPUSH, no operation will be performed when key does not yet exist.
func (db *RoseDB) RPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

// LPop removes and returns the first elements of the list stored at key.
func (db *RoseDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, true)
}

// RPop Removes and returns the last elements of the list stored at key.
func (db *RoseDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, false)
}

// LMove atomically returns and removes the first/last element of the list stored at source,
// and pushes the element at the first/last element of the list stored at destination. 移动大法
func (db *RoseDB) LMove(srcKey, dstKey []byte, srcIsLeft, dstIsLeft bool) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	// 源树
	popValue, err := db.popInternal(srcKey, srcIsLeft)
	if err != nil {
		return nil, err
	}
	if popValue == nil {
		return nil, nil
	}
	// 目标树
	if db.listIndex.trees[string(dstKey)] == nil {
		db.listIndex.trees[string(dstKey)] = art.NewART()
	}
	if err = db.pushInternal(dstKey, popValue, dstIsLeft); err != nil {
		return nil, err
	}

	return popValue, nil
}

// LLen returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (db *RoseDB) LLen(key []byte) int {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return 0
	}
	return int(tailSeq - headSeq - 1)
}

// LIndex returns the element at index in the list stored at key.
// If index is out of range, it returns nil.
func (db *RoseDB) LIndex(key []byte, index int) ([]byte, error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}

	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return nil, err
	}

	if seq >= tailSeq || seq <= headSeq {
		return nil, ErrWrongIndex
	}

	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// LSet Sets the list element at index to element.
func (db *RoseDB) LSet(key []byte, index int, value []byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}

	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return err
	}

	if seq >= tailSeq || seq <= headSeq {
		return ErrWrongIndex
	}

	encKey := db.encodeListKey(key, seq)
	ent := &logfile.LogEntry{Key: encKey, Value: value}
	valuePos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	if err = db.updateIndexTree(idxTree, ent, valuePos, true, List); err != nil {
		return err
	}
	return nil
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element 0 表示列表的第一个元素
// of the list (the head of the list), 1 being the next element and so on. 1 表示列表的第二个元素
// These offsets can also be negative numbers indicating offsets starting at the end of the list. -1 表示列表的最后一个元素
// For example, -1 is the last element of the list, -2 the penultimate, and so on. -2 表示列表的倒数第二个元素
// If start is larger than the end of the list, an empty list is returned. 如果 开始 索引号 大于 列表长度，就返回空
// If stop is larger than the actual end of the list, Redis will treat it like the last element of the list. 结束索引号大于的话，就会修正
func (db *RoseDB) LRange(key []byte, start, end int) (values [][]byte, err error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}

	idxTree := db.listIndex.trees[string(key)]
	// get List DataType meta info
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}

	var startSeq, endSeq uint32

	// logical address to physical address
	startSeq, err = db.listSequence(headSeq, tailSeq, start)
	if err != nil {
		return nil, err
	}
	endSeq, err = db.listSequence(headSeq, tailSeq, end)
	if err != nil {
		return nil, err
	}
	// normalize startSeq
	if startSeq <= headSeq {
		startSeq = headSeq + 1
	}
	// normalize endSeq
	if endSeq >= tailSeq {
		endSeq = tailSeq - 1
	}

	if startSeq >= tailSeq || endSeq <= headSeq || startSeq > endSeq {
		return nil, ErrWrongIndex
	}

	// the endSeq value is included
	for seq := startSeq; seq < endSeq+1; seq++ {
		encKey := db.encodeListKey(key, seq)
		val, err := db.getVal(idxTree, encKey, List)

		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func (db *RoseDB) encodeListKey(key []byte, seq uint32) []byte {
	buf := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key[:])
	return buf
}

func (db *RoseDB) decodeListKey(buf []byte) ([]byte, uint32) {
	seq := binary.LittleEndian.Uint32(buf[:4])
	key := make([]byte, len(buf[4:]))
	copy(key[:], buf[4:])
	return key, seq
}

func (db *RoseDB) listMeta(idxTree *art.AdaptiveRadixTree, key []byte) (uint32, uint32, error) {
	// 从内存中 || 从活跃文件中 || 从非活跃文件中 获得 value
	val, err := db.getVal(idxTree, key, List)
	// 说明是其他错误
	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}

	// 先初始化一下
	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = initialListSeq + 1
	// 不为空 说明之前 放入过，否则返回上面初始化的值
	if len(val) != 0 {
		// 前 4 个字节 转为 32 位 bit
		headSeq = binary.LittleEndian.Uint32(val[:4])
		// 再 4 个字节 转为 32 位 bit
		tailSeq = binary.LittleEndian.Uint32(val[4:8])
	}
	return headSeq, tailSeq, nil
}

func (db *RoseDB) saveListMeta(idxTree *art.AdaptiveRadixTree, key []byte, headSeq, tailSeq uint32) error {
	buf := make([]byte, 8)
	// 提供后续 list 的 put 使用
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)
	// 构建元数据
	ent := &logfile.LogEntry{Key: key, Value: buf, Type: logfile.TypeListMeta}
	// 将 list 的所占序号 填入文件中
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	// 内存中存放的是 key:原生key  value:seq
	err = db.updateIndexTree(idxTree, ent, pos, true, List)
	return err
}

func (db *RoseDB) pushInternal(key []byte, val []byte, isLeft bool) error {
	// 每一个 List 原生Key 都有一个 Tree
	idxTree := db.listIndex.trees[string(key)]
	// 一: 从文件中获得这个 原生key 的 头节点序号 尾节点序号
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}
	// 左插入
	var seq = headSeq
	if !isLeft {
		// 右插入
		seq = tailSeq
	}
	// 1️⃣ 编码 List 的包装Key HeadSeq+Key，前4字节是 Seq ，val 就是 其中一个 value
	encKey := db.encodeListKey(key, seq)
	// 包装key eny:{key_1 , value_1}
	ent := &logfile.LogEntry{Key: encKey, Value: val}
	// value1 写入 List 文件中, 此时的 Key 是 包含了 Seq的 包装Key , 并返回存储的位置
	valuePos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	// 2️⃣ 更新 包装Key 内存索引, 类似与 eny:{key_1 , value_1}
	if err = db.updateIndexTree(idxTree, ent, valuePos, true, List); err != nil {
		return err
	}
	// 左边添加
	if isLeft {
		headSeq--
	} else {
		tailSeq++
	}
	// 二: 更新 原生key 元数据 在内存中 和 文件中
	err = db.saveListMeta(idxTree, key, headSeq, tailSeq)
	return err
}

func (db *RoseDB) popInternal(key []byte, isLeft bool) ([]byte, error) {
	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	// 获得 原生Key 的Tree
	idxTree := db.listIndex.trees[string(key)]
	// 原生的key head tail seq
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	// 说明 list 中没有数据了
	if tailSeq-headSeq-1 <= 0 {
		return nil, nil
	}
	// 默认 headSeq 是下一个位置，++ 与之前的 -- 相抵消
	var seq = headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}
	// 包装key 来获得 上次写入的 key的文件位置
	encKey := db.encodeListKey(key, seq)
	// 尝试获得 上次 写入的 key 的位置
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	// 构建删除 这个 包装key 所对应的元素的记录
	ent := &logfile.LogEntry{Key: encKey, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}
	// 内存中删除 这个 包装key 位置信息
	oldVal, updated := idxTree.Delete(encKey)
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	// 更新 当前 list 的 seq 数据
	if err = db.saveListMeta(idxTree, key, headSeq, tailSeq); err != nil {
		return nil, err
	}
	// send discard
	db.sendDiscard(oldVal, updated, List)
	_, entrySize := logfile.EncodeEntry(ent)
	node := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[List].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	// 说明没有 元素了
	if tailSeq-headSeq-1 == 0 {
		// reset meta
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			// 重置这个 list 的 seq 元数据
			_ = db.saveListMeta(idxTree, key, headSeq, tailSeq)
		}
		// 删除内存树
		delete(db.listIndex.trees, string(key))
	}
	return val, nil
}

// listSequence just convert logical index to physical seq.
// whether physical seq is legal or not, just convert it
func (db *RoseDB) listSequence(headSeq, tailSeq uint32, index int) (uint32, error) {
	var seq uint32

	if index >= 0 {
		seq = headSeq + uint32(index) + 1
	} else {
		seq = tailSeq - uint32(-index)
	}
	return seq, nil
}

// LRem removes the first count occurrences of elements equal to element from the list stored at key.
// 根据参数 COUNT 的值，移除列表中与参数 VALUE 相等的元素
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element. 移除表中所有与 VALUE 相等的值
// Note that this method will rewrite the values, so it maybe very slow.
func (db *RoseDB) LRem(key []byte, count int, value []byte) (int, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if count == 0 {
		count = math.MaxUint32 // 直接顶穿
	}
	// 实际需要删除的元素个数
	var discardCount int
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return discardCount, nil
	}
	// get List DataType meta info 获得当前 list 的使用情况吧 head tail
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return discardCount, err
	}
	// 重新置位数组 需要删除数据数组 重新置位数组的实际数值
	reserveSeq, discardSeq, reserveValueSeq := make([]uint32, 0), make([]uint32, 0), make([][]byte, 0)
	notFound := make([]uint32, 0)

	classifyData := func(key []byte, seq uint32) error {
		encKey := db.encodeListKey(key, seq)         // 编码为 包装key
		val, err := db.getVal(idxTree, encKey, List) // 从文件中 或者 内存中 获得 实际val
		if err != nil {
			return err
		}
		if bytes.Equal(value, val) {
			discardSeq = append(discardSeq, seq)
			discardCount++
		} else {
			// 重新置位数组 转移到 重新置位序号数组中
			reserveSeq = append(reserveSeq, seq)
			tempVal := make([]byte, len(val))
			copy(tempVal, val)
			reserveValueSeq = append(reserveValueSeq, tempVal)
		}
		return nil
	}

	addReserveData := func(key []byte, value []byte, isLeft bool) error {
		if db.listIndex.trees[string(key)] == nil {
			db.listIndex.trees[string(key)] = art.NewART()
		}
		if err := db.pushInternal(key, value, isLeft); err != nil {
			return err
		}
		return nil
	}

	if count > 0 {
		// record discard data and reserve data
		// 从左向右开始遍历 seq
		for seq := headSeq + 1; seq < tailSeq; seq++ {
			// 保存需要 删除的 seq 和 需要保留的 seq
			if err := classifyData(key, seq); err != nil {
				// 既然存在了 headSeq，那么一般情况下，就会存在 包装key 对应的 value;极端情况下可能会出现磁盘损坏，导致没有找到对应的value，怎么处理？
				notFound = append(notFound, seq)
				// 继续执行
				//return discardCount, err
			}
			if discardCount == count {
				break
			}
		}
		discardSeqLen := len(discardSeq)
		if discardSeqLen > 0 {
			// delete discard data 删除所有 小于等于 value 值seq之前的所有记录，方便移位
			for seq := headSeq + 1; seq <= discardSeq[discardSeqLen-1]; seq++ {
				// 逐一删除 ： 27 28 29 30 =>  27 28 30
				if _, err := db.popInternal(key, true); err != nil {
					return discardCount, err
				}
			}
			// add reserve data
			for i := len(reserveSeq) - 1; i >= 0; i-- {
				if reserveSeq[i] < discardSeq[discardSeqLen-1] {
					// 再次重新添加 之前被删除的 value
					if err := addReserveData(key, reserveValueSeq[i], true); err != nil {
						return discardCount, err
					}
				}
			}
		}
	} else {
		count = -count
		// record discard data and reserve data
		for seq := tailSeq - 1; seq > headSeq; seq-- {
			if err := classifyData(key, seq); err != nil {
				return discardCount, err
			}
			if discardCount == count {
				break
			}
		}

		discardSeqLen := len(discardSeq)
		if discardSeqLen > 0 {
			// delete discard data
			for seq := tailSeq - 1; seq >= discardSeq[discardSeqLen-1]; seq-- {
				if _, err := db.popInternal(key, false); err != nil {
					return discardCount, err
				}
			}
			// add reserve data
			for i := len(reserveSeq) - 1; i >= 0; i-- {
				if reserveSeq[i] > discardSeq[discardSeqLen-1] {
					if err := addReserveData(key, reserveValueSeq[i], false); err != nil {
						return discardCount, err
					}
				}
			}
		}
	}
	return discardCount, nil
}
