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
	"time"
)

type FileNode struct {
	path   string
	fs     *fuseFs
	id     uint64
	size   int64
	offset int64
	mtime  int64
}

func (f *FileNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	logger.Debug("attr file", zap.String("path", f.path))
	attr.Mode = 0o444
	attr.Size = uint64(f.size)
	attr.Mtime = time.Unix(f.mtime, 0)
	attr.Ctime = attr.Mtime
	return nil
}

func (f *FileNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	logger.Debug("get real file path occur error",
		zap.Uint64("id", f.id),
		zap.String("m_path", f.path),
		zap.Int64("size", f.size),
		zap.Int64("offset", f.offset),
	)
	if !req.Flags.IsReadOnly() {
		return nil, syscall.EACCES
	}
	if req.Flags&fuse.OpenDirectory == fuse.OpenDirectory {
		return nil, syscall.ENOTSUP
	}
	path, err := GetRealFilePath(f.id)
	if err != nil {
		logger.Warn("get real file path occur error",
			zap.Uint64("id", f.id),
			zap.String("m_path", f.path),
			zap.Int64("size", f.size),
			zap.Int64("offset", f.offset),
			zap.Error(err),
		)
		return nil, err
	}
	path = filepath.Join(f.fs.pathPrefix, path)
	resp.Flags |= fuse.OpenKeepCache
	handle := &FileHandle{node: f}
	handle.f, err = os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		logger.Warn("open real file path occur error",
			zap.Uint64("id", f.id),
			zap.String("m_path", f.path),
			zap.String("r_path", path),
			zap.Int64("size", f.size),
			zap.Int64("offset", f.offset),
			zap.Error(err),
		)
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
	if req.Offset+int64(req.Size) > f.node.size {
		resp.Data = resp.Data[:f.node.size-req.Offset]
	} else {
		resp.Data = resp.Data[:req.Size]
	}
	n, err := f.f.ReadAt(resp.Data, f.node.offset+req.Offset)
	if err != nil {
		logger.Warn("read file occur error",
			zap.String("m_path", f.node.path),
			zap.String("r_path", f.f.Name()),
			zap.Int64("size", f.node.size),
			zap.Int64("offset", f.node.offset),
			zap.Int64("req_offset", req.Offset),
		)
	}
	resp.Data = resp.Data[:n]
	return err
}

func (f *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	logger.Debug("release file",
		zap.String("m_path", f.node.path),
		zap.String("r_path", f.f.Name()),
		zap.Int64("size", f.node.size),
		zap.Int64("offset", f.node.offset))
	return f.f.Close()
}
