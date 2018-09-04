package utils

import (
	"math/rand"
	"time"
)

func RandomString(len int) string {
	bytes := make([]byte, len)
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < len; i++ {
		bytes[i] = byte('a' + rand.Intn(25))
	}
	return string(bytes)
}
