package icanal

import (
	"crypto/sha1"
)

func scramble411(password, seed []byte) []byte {
	// 第一步：SHA1(Password)
	hash := sha1.New()
	hash.Write(password)
	stage1 := hash.Sum(nil)

	// 第二步：SHA1(SHA1(Password))
	hash.Reset()
	hash.Write(stage1)
	stage2 := hash.Sum(nil)

	// 第三步：SHA1(seed + SHA1(SHA1(Password)))
	hash.Reset()
	hash.Write(seed)
	hash.Write(stage2)
	stage3 := hash.Sum(nil)
	for i := range stage3 {
		stage3[i] ^= stage1[i]
	}

	return stage3
}
