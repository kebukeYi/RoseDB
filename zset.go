package rosedb

import (
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
)

// ZAdd adds the specified member with the specified score to the sorted set stored at key.
func (db *RoseDB) ZAdd(key []byte, score float64, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	scoreBuf := []byte(util.Float64ToStr(score))
	// 包装key : 实际key 成绩  写入文件中
	zsetKey := db.encodeKey(key, scoreBuf)
	entry := &logfile.LogEntry{Key: zsetKey, Value: member}
	pos, err := db.writeLogEntry(entry, ZSet)
	if err != nil {
		return err
	}

	_, size := logfile.EncodeEntry(entry)
	pos.entrySize = size
	// hash值 和 实际value 写入到内存中
	ent := &logfile.LogEntry{Key: sum, Value: member}
	if err := db.updateIndexTree(idxTree, ent, pos, true, ZSet); err != nil {
		return err
	}
	// 核心方法
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	return nil
}

// ZScore returns the score of member in the sorted set at key.
func (db *RoseDB) ZScore(key, member []byte) (ok bool, score float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return false, 0
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	//
	return db.zsetIndex.indexes.ZScore(string(key), string(sum))
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (db *RoseDB) ZRem(key, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	// hash表 跳表中都执行删除
	ok := db.zsetIndex.indexes.ZRem(string(key), string(sum))
	if !ok {
		return nil
	}

	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	oldVal, deleted := idxTree.Delete(sum)
	db.sendDiscard(oldVal, deleted, ZSet)
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, ZSet)
	if err != nil {
		return err
	}
	// The deleted entry itself is also invalid.
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[ZSet].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (db *RoseDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	// 返回元素数量
	return db.zsetIndex.indexes.ZCard(string(key))
}

// ZRange returns the specified range of elements in the sorted set stored at key.
// 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
// 也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
func (db *RoseDB) ZRange(key []byte, start, stop int) ([][]byte, error) {
	return db.zRangeInternal(key, start, stop, false)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
func (db *RoseDB) ZRevRange(key []byte, start, stop int) ([][]byte, error) {
	return db.zRangeInternal(key, start, stop, true)
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
// 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从小到大)顺序排列
func (db *RoseDB) ZRank(key []byte, member []byte) (ok bool, rank int) {
	return db.zRankInternal(key, member, false)
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (db *RoseDB) ZRevRank(key []byte, member []byte) (ok bool, rank int) {
	return db.zRankInternal(key, member, true)
}

func (db *RoseDB) zRangeInternal(key []byte, start, stop int, rev bool) ([][]byte, error) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	var res [][]byte
	var values []interface{}
	if rev {
		//
		values = db.zsetIndex.indexes.ZRevRange(string(key), start, stop)
	} else {
		//
		values = db.zsetIndex.indexes.ZRange(string(key), start, stop)
	}

	for _, val := range values {
		v, _ := val.(string)
		if val, err := db.getVal(idxTree, []byte(v), ZSet); err != nil {
			return nil, err
		} else {
			res = append(res, val)
		}
	}
	return res, nil
}

func (db *RoseDB) zRankInternal(key []byte, member []byte, rev bool) (ok bool, rank int) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.zsetIndex.trees[string(key)] == nil {
		return
	}

	// member 转为 sum hash值
	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	var result int64
	if rev {
		result = db.zsetIndex.indexes.ZRevRank(string(key), string(sum))
	} else {
		// 返回当前元素的 排名
		result = db.zsetIndex.indexes.ZRank(string(key), string(sum))
	}
	if result != -1 {
		ok = true
		rank = int(result)
	}
	return
}
