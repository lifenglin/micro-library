package library

import (
	"crypto/md5"
        "crypto/rand"
	"fmt"
	"math/rand"
	"time"
)

func GetRandStr(n int) string {
	seedsLetters := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	e := make([]byte, n)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < n; i++ {
		e[i] = seedsLetters[rand.Intn(len(seedsLetters))]
	}
	return string(e)
}

func Md5(str string) string {
	data := []byte(str)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	return md5str
}

func UuidGenerator() (uuid string) {
	uuid = ""
        b := make([]byte, 16)
        _, err := rand.Read(b)
        if err == nil {
		uuid = fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
        }
        return uuid
}
