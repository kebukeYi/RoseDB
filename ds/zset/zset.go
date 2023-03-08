package zset

import (
	"math"
	"math/rand"

	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/util"
)

// zset is the implementation of sorted set

const (
	maxLevel    = 32
	probability = 0.25
)

type EncodeKey func(key, subKey []byte) []byte

type (
	// SortedSet sorted set struct
	SortedSet struct {
		record map[string]*SortedSetNode // map[key][sortedSetNode]
	}

	// SortedSetNode node of sorted set
	SortedSetNode struct {
		dict map[string]*sklNode // map[member][sklNode] ，hash表 这是想干嘛？加快访问？
		skl  *skipList           // 跳表 只有 头尾节点
	}

	// 跳表节点
	sklNode struct {
		member   string      // key
		score    float64     // value
		backward *sklNode    // 前驱节点
		level    []*sklLevel // 每层的指针
	}

	// 跳表节点中多层 level 节点
	sklLevel struct {
		forward *sklNode // 下一个节点
		span    uint64   // 跨度
	}

	// 整合跳表API
	skipList struct {
		head   *sklNode
		tail   *sklNode
		length int64
		level  int16
	}
)

// New create a new sorted set.
func New() *SortedSet {
	return &SortedSet{make(map[string]*SortedSetNode)}
}

func (z *SortedSet) IterateAndSend(chn chan *logfile.LogEntry, encode EncodeKey) {
	for key, ss := range z.record {
		zsetKey := []byte(key)
		if ss.skl.head == nil {
			return
		}
		for e := ss.skl.head.level[0].forward; e != nil; e = e.level[0].forward {
			scoreBuf := []byte(util.Float64ToStr(e.score))
			encKey := encode(zsetKey, scoreBuf)
			chn <- &logfile.LogEntry{Key: encKey, Value: []byte(e.member)}
		}
	}
	return
}

// ZAdd Adds the specified member with the specified score to the sorted set stored at key.
func (z *SortedSet) ZAdd(key string, score float64, member string) {
	// zadd myzset mmy 23 kk 22 kkes 45
	// zadd myzset2 mmy 23 kk 22 kkes 45
	// 先在大表中进行 筛选 是否存在这个 myzset(key)，没有则执行新建
	if !z.exist(key) {
		node := &SortedSetNode{
			dict: make(map[string]*sklNode),
			skl:  newSkipList(),
		}
		// 设置新值
		z.record[key] = node
	}

	sortedSetNode := z.record[key] // sortedSetNode
	// 判断这个 key 下的 一些变量成员是否存在(先在内存中的hash表中进行判断)
	dictNode, exist := sortedSetNode.dict[member]

	var node *sklNode
	// 如果 之前已经存在了 那么就走更新之路，或者一点也不变
	if exist {
		// 更新之路
		if score != dictNode.score {
			// 先删除 旧的 成员
			sortedSetNode.skl.sklDelete(dictNode.score, member)
			// 再新增新的成员
			node = sortedSetNode.skl.sklInsert(score, member)
		}
	} else {
		// 直接新增新的成员(member，score) 到跳表中
		node = sortedSetNode.skl.sklInsert(score, member)
	}
	// 新增成功了
	if node != nil {
		// 在内存中 先保存一份
		sortedSetNode.dict[member] = node
	}
}

// ZScore returns the score of member in the sorted set at key.
func (z *SortedSet) ZScore(key string, member string) (ok bool, score float64) {
	if !z.exist(key) {
		return
	}

	node, exist := z.record[key].dict[member]
	if !exist {
		return
	}

	return true, node.score
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
// ZCard 返回存储在 key 处的排序集的排序集基数（元素数）
func (z *SortedSet) ZCard(key string) int {
	if !z.exist(key) {
		return 0
	}
	i := len(z.record[key].dict)
	return i
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
func (z *SortedSet) ZRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)
	rank--

	return rank
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (z *SortedSet) ZRevRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)

	return z.record[key].skl.length - rank
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (z *SortedSet) ZIncrBy(key string, increment float64, member string) float64 {
	if z.exist(key) {
		node, exist := z.record[key].dict[member]
		if exist {
			increment += node.score
		}
	}

	z.ZAdd(key, increment, member)
	return increment
}

// ZRange returns the specified range of elements in the sorted set stored at <key>.
func (z *SortedSet) ZRange(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(stop), false, false)
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at <key>.
func (z *SortedSet) ZRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, true)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (z *SortedSet) ZRevRange(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, false)
}

// ZRevRangeWithScores returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (z *SortedSet) ZRevRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, true)
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (z *SortedSet) ZRem(key, member string) bool {
	if !z.exist(key) {
		return false
	}

	v, exist := z.record[key].dict[member]
	if exist {
		z.record[key].skl.sklDelete(v.score, member)
		delete(z.record[key].dict, member)
		return true
	}

	return false
}

// ZGetByRank get the member at key by rank, the rank is ordered from lowest to highest.
// The rank of lowest is 0 and so on.
func (z *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}

	member, score := z.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return
}

// ZRevGetByRank get the member at key by rank, the rank is ordered from highest to lowest.
// The rank of highest is 0 and so on.
func (z *SortedSet) ZRevGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}

	member, score := z.getByRank(key, int64(rank), true)
	val = append(val, member, score)
	return
}

// ZScoreRange returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
// The elements are considered to be ordered from low to high scores.
func (z *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score < min {
			p = p.level[i].forward
		}
	}

	p = p.level[0].forward
	for p != nil {
		if p.score > max {
			break
		}

		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}

	return
}

// ZRevScoreRange returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min).
// In contrary to the default ordering of sorted sets, for this command the elements are considered to be ordered from high to low scores.
func (z *SortedSet) ZRevScoreRange(key string, max, min float64) (val []interface{}) {
	if !z.exist(key) || max < min {
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score <= max {
			p = p.level[i].forward
		}
	}

	for p != nil {
		if p.score < min {
			break
		}

		val = append(val, p.member, p.score)
		p = p.backward
	}

	return
}

// ZKeyExists check if the key exists in zset.
func (z *SortedSet) ZKeyExists(key string) bool {
	return z.exist(key)
}

// ZClear clear the key in zset.
func (z *SortedSet) ZClear(key string) {
	if z.ZKeyExists(key) {
		delete(z.record, key)
	}
}

func (z *SortedSet) exist(key string) bool {
	_, exist := z.record[key]
	return exist
}

func (z *SortedSet) getByRank(key string, rank int64, reverse bool) (string, float64) {

	skl := z.record[key].skl
	if rank < 0 || rank > skl.length {
		return "", math.MinInt64
	}

	if reverse {
		rank = skl.length - rank
	} else {
		rank++
	}

	n := skl.sklGetElementByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}

	node := z.record[key].dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}

	return node.member, node.score
}

func (z *SortedSet) findRange(key string, start, stop int64, reverse bool, withScores bool) (val []interface{}) {
	skl := z.record[key].skl
	length := skl.length

	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop += length
	}

	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}
	span := (stop - start) + 1

	var node *sklNode
	if reverse {
		node = skl.tail
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}

	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}

	return
}

func sklNewNode(level int16, score float64, member string) *sklNode {
	node := &sklNode{
		score:  score,
		member: member,
		level:  make([]*sklLevel, level),
	}

	for i := range node.level {
		node.level[i] = new(sklLevel)
	}

	return node
}

func newSkipList() *skipList {
	return &skipList{
		level: 1,
		head:  sklNewNode(maxLevel, 0, ""),
	}
}

func randomLevel() int16 {
	var level int16 = 1
	for level < maxLevel {
		if rand.Float64() < probability {
			break
		}
		level++
	}
	return level
}

func (skl *skipList) sklInsert(score float64, member string) *sklNode {
	updates := make([]*sklNode, maxLevel)
	rank := make([]uint64, maxLevel)

	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		if i == skl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		if p.level[i] != nil {
			for p.level[i].forward != nil &&
				(p.level[i].forward.score < score ||
					(p.level[i].forward.score == score && p.level[i].forward.member < member)) {

				rank[i] += p.level[i].span
				p = p.level[i].forward
			}
		}
		updates[i] = p
	}

	level := randomLevel()
	if level > skl.level {
		for i := skl.level; i < level; i++ {
			rank[i] = 0
			updates[i] = skl.head
			updates[i].level[i].span = uint64(skl.length)
		}
		skl.level = level
	}

	p = sklNewNode(level, score, member)
	for i := int16(0); i < level; i++ {
		p.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = p

		p.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		updates[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < skl.level; i++ {
		updates[i].level[i].span++
	}

	if updates[0] == skl.head {
		p.backward = nil
	} else {
		p.backward = updates[0]
	}

	if p.level[0].forward != nil {
		p.level[0].forward.backward = p
	} else {
		skl.tail = p
	}

	skl.length++
	return p
}

func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode) {
	for i := int16(0); i < skl.level; i++ {
		if updates[i].level[i].forward == p {
			updates[i].level[i].span += p.level[i].span - 1
			updates[i].level[i].forward = p.level[i].forward
		} else {
			updates[i].level[i].span--
		}
	}

	if p.level[0].forward != nil {
		p.level[0].forward.backward = p.backward
	} else {
		skl.tail = p.backward
	}

	for skl.level > 1 && skl.head.level[skl.level-1].forward == nil {
		skl.level--
	}

	skl.length--
}

func (skl *skipList) sklDelete(score float64, member string) {
	update := make([]*sklNode, maxLevel)
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
			p = p.level[i].forward
		}
		update[i] = p
	}

	p = p.level[0].forward
	if p != nil && score == p.score && p.member == member {
		skl.sklDeleteNode(p, update)
		return
	}
}

func (skl *skipList) sklGetRank(score float64, member string) int64 {
	var rank uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)) {

			rank += p.level[i].span
			p = p.level[i].forward
		}
		if p.member == member {
			return int64(rank)
		}
	}
	return 0
}

func (skl *skipList) sklGetElementByRank(rank uint64) *sklNode {
	var traversed uint64 = 0
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && (traversed+p.level[i].span) <= rank {
			traversed = traversed + p.level[i].span
			p = p.level[i].forward
		}
		if traversed == rank {
			return p
		}
	}
	return nil
}