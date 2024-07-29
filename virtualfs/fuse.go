package virtualfs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"context"
	"github.com/manx98/local_to_seaf_store/logger"
	"go.uber.org/zap"
	"log"
	"os"
)

type fuseFs struct {
	path       string
	pathPrefix string
}

func (f *fuseFs) Root() (fs.Node, error) {
	return &DirNode{path: f.path, fs: f}, nil
}

func Mount(ctx context.Context, pathPrefix, mountPoint, repoId string) {
	if _, err := os.Stat(mountPoint); err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(mountPoint, os.ModePerm)
			if err != nil {
				logger.Fatal("mkdir occur error", zap.Error(err))
			}
		} else {
			logger.Fatal("stat occur error", zap.Error(err))
		}
	}
	if err := fuse.Unmount(mountPoint); err != nil {
		log.Println("unmount occur error: ", err)
	}
	mount, err := fuse.Mount(
		mountPoint,
		fuse.FSName("FileMappingFS"),
		fuse.Subtype("FileMappingFS"),
	)
	if err != nil {
		log.Fatal("mount occur error: ", err)
	}
	defer mount.Close()
	go func() {
		<-ctx.Done()
		_ = mount.Close()
	}()
	if err = fs.New(mount, nil).Serve(&fuseFs{pathPrefix: pathPrefix, path: "/" + repoId}); err != nil {
		log.Fatal("serve fs occur error: ", err)
	}
}
