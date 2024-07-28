package virtualfs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"context"
	"os"
	"path/filepath"
	"syscall"
)

type FileNode struct {
	path       string
	pathPrefix string
	id         uint64
	size       int64
	offset     int64
}

func (f *FileNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = syscall.S_IFREG | 0755
	return nil
}

func (f *FileNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if !req.Flags.IsReadOnly() {
		return nil, syscall.EACCES
	}
	if req.Flags&fuse.OpenDirectory == fuse.OpenDirectory {
		return nil, syscall.ENOTSUP
	}
	path, err := GetRealFilePath(f.id)
	if err != nil {
		return nil, err
	}
	handle := &FileHandle{}
	handle.f, err = os.OpenFile(filepath.Join(f.pathPrefix, path), os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, syscall.ENOENT
		}
		return nil, err
	}
	return handle, nil
}

type FileHandle struct {
	node *FileNode
	f    *os.File
}

func (f *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	n, err := f.f.ReadAt(resp.Data, f.node.offset+req.Offset)
	resp.Data = resp.Data[:n]
	return err
}

func (f *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return f.f.Close()
}
