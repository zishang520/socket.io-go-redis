package adapter

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
)

func RandomId() (string, error) {
	r := make([]byte, 8)
	if _, err := rand.Read(r); err != nil {
		return "", err
	}
	return hex.EncodeToString(r), nil
}

func Uid2(length int) (string, error) {
	r := make([]byte, length)
	if _, err := rand.Read(r); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(r), nil
}
