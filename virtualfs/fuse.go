package virtualfs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"context"
	"log"
)

type fuseFs struct {
	pathPrefix string
}

func (f *fuseFs) Root() (fs.Node, error) {
	return &DirNode{path: "/", pathPrefix: f.pathPrefix}, nil
}

func Mount(ctx context.Context, pathPrefix, mountPoint string) {
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
	if err = fs.New(mount, nil).Serve(&fuseFs{pathPrefix: pathPrefix}); err != nil {
		log.Fatal("serve fs occur error: ", err)
	}
}
