package utils

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"os"
	"os/user"
	"strconv"
	"syscall"
)

func GetFileUsername(file string) string {
	stat, err := os.Stat(file)
	if err != nil {
		return ""
	}
	if t, ok := stat.Sys().(*syscall.Stat_t); !ok {
		return ""
	} else {
		id, err := user.LookupId(strconv.Itoa(int(t.Uid)))
		if err != nil {
			return fmt.Sprintf("UID(%d)", t.Uid)
		} else {
			return id.Username
		}
	}
}

func RandId() string {
	data, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	hash := sha1.New()
	hash.Write(data[:])
	return hex.EncodeToString(hash.Sum(nil))
}
