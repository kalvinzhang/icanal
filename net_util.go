package icanal

import (
	"encoding/binary"
)

const (
	lengthOfHeader = 4
)

// generateWriteHeader 获取要写数据的长度
func generateWriteHeader(length int) []byte {
	header := make([]byte, lengthOfHeader)
	binary.BigEndian.PutUint32(header, uint32(length))
	return header
}
