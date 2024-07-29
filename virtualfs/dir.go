package virtualfs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"context"
	"github.com/manx98/local_to_seaf_store/logger"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"syscall"
)

type DirNode struct {
	path  string
	fs    *fuseFs
	mtime int64
}

func (f *DirNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	logger.Debug("attr dir", zap.String("path", f.path))
	attr.Mode = os.ModeDir | 0o555
	return nil
}

func (f *DirNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	logger.Debug("read dir all", zap.String("path", f.path))
	data, err := ListDir(f.path)
	if err != nil {
		logger.Warn("list dir occur error", zap.Error(err), zap.String("path", f.path))
		return nil, err
	}
	logger.Debug("list dir all", zap.Any("data", data), zap.String("path", f.path))
	return data, nil
}

func (f *DirNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	path := filepath.Join(f.path, name)
	var isDir bool
	var size int64
	var offset int64
	var fId uint64
	var mtime int64
	logger.Debug("lookup", zap.String("path", path))
	err := Lookup(path, &isDir, &size, &offset, &fId, &mtime)
	if err != nil {
		logger.Warn("lookup occur error", zap.Error(err), zap.String("path", path))
		return nil, err
	}
	if isDir {
		return &DirNode{path: path, fs: f.fs, mtime: mtime}, nil
	} else {
		if size < 0 || offset < 0 || mtime < 0 {
			logger.Warn("lookup get invalid data", zap.Error(err),
				zap.String("path", path),
				zap.Int64("size", size),
				zap.Int64("offset", offset),
				zap.Int64("mtime", mtime),
			)
			return nil, syscall.EIO
		}
		return &FileNode{fs: f.fs, path: path, id: fId, size: size, offset: offset, mtime: mtime}, nil
	}
}
