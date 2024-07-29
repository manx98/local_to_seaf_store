package main

import (
	"testing"
)

func Test_scanFs(t *testing.T) {
	dataDir = new(string)
	*dataDir = "/tmp"
	parentCommitId = new(string)
	*parentCommitId = "7c4be441b8f7f9c999f29837716ea79e44437995"
	scanRepoId = new(string)
	*scanRepoId = "11a6e0ac-a8be-42ea-ac71-67472ce2710a"
	blockSize = new(int64)
	*blockSize = 8 * 1024 * 1024
	scanDir = new(string)
	*scanDir = "/cdrom/pool"
	creator = new(string)
	*creator = "me@qq.com"
	scanFs(nil, nil)
}

func Test_mount(t *testing.T) {
	mountDataDir = new(string)
	*mountDataDir = "/tmp"
	pathPrefix = new(string)
	*pathPrefix = "/cdrom/pool"
	mountRepoId = new(string)
	*mountRepoId = "11a6e0ac-a8be-42ea-ac71-67472ce2710a"
	mountFs(nil, nil)
}
