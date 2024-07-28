package virtualfs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"context"
	"os"
	"path/filepath"
)

type DirNode struct {
	path       string
	pathPrefix string
}

func (f *DirNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0o555
	return nil
}

func (f *DirNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return ListDir(f.path)
}

func (f *DirNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	path := filepath.Join(f.path, name)
	var isDir bool
	var size int64
	var offset int64
	var fId uint64
	err := Lookup(path, &isDir, &size, &offset, &fId)
	if err != nil {
		return nil, err
	}
	if isDir {
		return &DirNode{path: path, pathPrefix: f.pathPrefix}, nil
	} else {
		return &FileNode{pathPrefix: f.pathPrefix, path: path, id: fId, size: size, offset: offset}, nil
	}
}

func (f *DirNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if req.Dir {
		return DeleteDir(filepath.Join(f.path, req.Name))
	}
	return DeleteFile(filepath.Join(f.path, req.Name))
}
