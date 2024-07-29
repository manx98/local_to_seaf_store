package utils

import (
	"crypto/rand"
	"encoding/hex"
	"github.com/google/uuid"
)

func RandId() string {
	data := make([]byte, 20)
	_, _ = rand.Read(data)
	return hex.EncodeToString(data)
}

func IsValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}

func IsObjectIDValid(objID string) bool {
	if len(objID) != 40 {
		return false
	}
	for i := 0; i < len(objID); i++ {
		c := objID[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return false
	}
	return true
}
