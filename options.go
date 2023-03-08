package rosedb

import "time"

// DataIndexMode the data index mode.
type DataIndexMode int

const (
	// KeyValueMemMode key and value are both in memory, read operation will be very fast in this mode.
	// Because there is no disk seek, just get value from the corresponding data structures in memory.
	// This mode is suitable for scenarios where the value are relatively small.
	KeyValueMemMode DataIndexMode = iota

	// KeyOnlyMemMode only key in memory, there is a disk seek while getting a value.
	// Because values are in log file on disk.
	KeyOnlyMemMode
)

// IOType represents different types of file io: FileIO(standard file io) and MMap(Memory Map).
type IOType int8

const (
	// FileIO standard file io.
	FileIO IOType = iota
	// MMap Memory Map.
	MMap
)

// Options for opening a db.
type Options struct {

	// DBPath db path, will be created automatically if not exist.
	DBPath string

	// IndexMode mode of index, support KeyValueMemMode and KeyOnlyMemMode now.
	// 索引模式：只有键在内存中 || 键和值 都在
	// Note that this mode is only for kv pairs, not List, Hash, Set, and ZSet.
	// 没有限制 kv 大小吗？
	// Default value is KeyOnlyMemMode.
	IndexMode DataIndexMode

	// IoType file r/w io type, support FileIO and MMap now.
	// 文件读写类型：FileIO 纯文件方式 || MMap 内存映射方式
	// Default value is FileIO.
	IoType IOType

	// Sync is whether to sync writes from the OS buffer cache through to actual disk.
	// Sync 是否将写入从 OS 缓冲区缓存同步到实际磁盘
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (and the machine does not) then no writes will be lost.
	// 如果只是进程崩溃（而机器没有），那么不会丢失任何写入
	// Default value is false.
	Sync bool

	// LogFileGCInterval a background goroutine will execute log file garbage collection periodically according to the interval.
	// It will pick the log file that meet the conditions for GC, then rewrite the valid data one by one.
	// 它将选择满足GC条件的日志文件，然后将有效数据一一重写，压缩文件周期
	// Default value is 8 hours.
	LogFileGCInterval time.Duration

	// LogFileGCRatio if discarded data in log file exceeds this ratio, it can be picked up for compaction(garbage collection)
	// And if there are many files reached the ratio, we will pick the highest one by one. 如果有很多文件达到了这个比例，我们会一一挑选最高的
	// The recommended ratio is 0.5, half of the file can be compacted.
	// Default value is 0.5.
	LogFileGCRatio float64

	// LogFileSizeThreshold threshold size of each log file, active log file will be closed if reach the threshold.
	// Important!!! This option must be set to the same value as the first startup. 此选项必须设置为与第一次启动相同的值
	// Default value is 512 MB. 合并，临界值点
	LogFileSizeThreshold int64

	// DiscardBufferSize a channel will be created to send the older entry size when a key updated or deleted.
	// 当 key 更新或删除时，将创建一个通道以发送较旧的条目大小
	// Entry size will be saved in the discard file, recording the invalid size in a log file, and be used when log file gc is running.
	// This option represents the size of that channel. 此选项表示该通道的大小
	// If you got errors like `send discard chan fail`, you can increase this option to avoid it.
	DiscardBufferSize int
}

// DefaultOptions default options for opening a RoseDB.
// 针对 RoseDB 默认选项参数 (还是不多的)
func DefaultOptions(path string) Options {
	return Options{
		DBPath:               path,
		IndexMode:            KeyOnlyMemMode,
		IoType:               FileIO,
		Sync:                 false,
		LogFileGCInterval:    time.Hour * 8,
		LogFileGCRatio:       0.5,
		LogFileSizeThreshold: 512 << 20, // 阈值
		DiscardBufferSize:    8 << 20,   //
	}
}
