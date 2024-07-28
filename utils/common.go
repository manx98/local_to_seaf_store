package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
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
	data := make([]byte, 20)
	_, _ = rand.Read(data)
	return hex.EncodeToString(data)
}
