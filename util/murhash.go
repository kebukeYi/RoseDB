package util

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"io"
)

// 一种非加密型哈希函数
type Murmur128 struct {
	mur murmur3.Hash128
}

func NewMurmur128() *Murmur128 {
	return &Murmur128{mur: murmur3.New128()}
}

func (m *Murmur128) Write(p []byte) error {
	n, err := m.mur.Write(p)
	if n != len(p) {
		return io.ErrShortWrite
	}
	return err
}

func (m *Murmur128) EncodeSum128() []byte {
	// 创建 byte 数组 大小 20 字节
	buf := make([]byte, binary.MaxVarintLen64*2)
	//
	s1, s2 := m.mur.Sum128()
	var index int
	index = index + binary.PutUvarint(buf[index:], s1)
	index = index + binary.PutUvarint(buf[index:], s2)
	return buf[:index]
}

func (m *Murmur128) Reset() {
	m.mur.Reset()
}
