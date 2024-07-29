package main

import (
	"testing"
)

func Test_scanFs(t *testing.T) {
	dataDir = new(string)
	*dataDir = "/tmp"
	commitId = new(string)
	*commitId = "363b24f55f52da85cf9eb7fa0f9c8bf30325da75"
	repoId = new(string)
	*repoId = "00a57a07-79b0-4156-ab36-a556cfa54d57"
	blockSize = new(int64)
	*blockSize = 8 * 1024 * 1024
	scanDir = new(string)
	*scanDir = "/cdrom/pool"
	scanFs(nil, nil)
}

func Test_mount(t *testing.T) {
	mountDataDir = new(string)
	*mountDataDir = "/tmp"
	pathPrefix = new(string)
	*pathPrefix = "/cdrom/pool"
	mountFs(nil, nil)
}
