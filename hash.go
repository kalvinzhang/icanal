package icanal

import (
	"crypto/sha1"
)

func scramble411(password, seed []byte) []byte {
	// 第一步：SHA1(Password)
	stage1 := sha1.New()
	stage1.Write(password)
	hash1 := stage1.Sum(nil)

	// 第二步：SHA1(SHA1(Password))
	stage2 := sha1.New()
	stage2.Write(hash1)
	hash2 := stage2.Sum(nil)

	// 第三步：SHA1(seed + SHA1(SHA1(Password)))
	stage3 := sha1.New()
	stage3.Write(seed)
	stage3.Write(hash2)
	result := stage3.Sum(nil)

	return result
}
