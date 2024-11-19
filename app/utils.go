package main

import (
	"math/rand"
	"time"
)

const alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandString(n int) string {
	b := make([]byte, n)
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	for i := range b {
		b[i] = alphanumeric[rng.Intn(len(alphanumeric))]
	}

	return string(b)
}
