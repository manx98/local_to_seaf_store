package virtualfs

import (
	"bazil.org/fuse"
	"encoding/binary"
	"fmt"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"syscall"
)

var db *bbolt.DB
var globalId uint64

const (
	RealPathToIdBucketName = "RTI"
	IdToRealPathBucketName = "ITR"
)

func InitVirtualFs(dbFile string) (err error) {
	db, err = bbolt.Open(dbFile, os.ModePerm, &bbolt.Options{
		NoSync: true,
	})
	if err != nil {
		return fmt.Errorf("create db: %w", err)
	}
	globalId, err = LastRealFileId()
	if err != nil {
		return fmt.Errorf("get last real file globalId: %w", err)
	}
	err = db.Batch(func(tx *bbolt.Tx) error {
		_, cErr := tx.CreateBucketIfNotExists([]byte(RealPathToIdBucketName))
		if cErr != nil {
			return fmt.Errorf("create %s bucket: %w", RealPathToIdBucketName, cErr)
		}
		_, cErr = tx.CreateBucketIfNotExists([]byte(IdToRealPathBucketName))
		if cErr != nil {
			return fmt.Errorf("create %s bucket: %w", IdToRealPathBucketName, cErr)
		}
		return nil
	})
	return
}

func mkdirAll(tx *bbolt.Tx, dir string) (err error) {
	parent := filepath.Dir(dir)
	bucket := tx.Bucket([]byte(parent))
	if bucket == nil {
		bucket, err = tx.CreateBucket([]byte(parent))
		if err != nil {
			return
		}
		if parent != dir {
			err = mkdirAll(tx, filepath.Dir(dir))
		} else {
			return
		}
	}
	file := filepath.Base(dir)
	if file != dir {
		if data := bucket.Get([]byte(file)); data == nil {
			err = bucket.Put([]byte(file), []byte{0})
		} else if data[len(data)-1] != 0 {
			return syscall.EPERM
		}
	}
	return
}

func MkdirAll(tx *bbolt.Tx, dir string) error {
	bucket := tx.Bucket([]byte(dir))
	if bucket != nil {
		return nil
	}
	_, err := tx.CreateBucket([]byte(dir))
	if err != nil {
		return err
	}
	return mkdirAll(tx, dir)
}

func ListDir(parent string) (direntList []fuse.Dirent, err error) {
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(parent))
		if bucket == nil {
			return syscall.ENOENT
		}
		err = bucket.ForEach(func(name, data []byte) error {
			dirent := fuse.Dirent{Name: string(name)}
			if data[len(data)-1] == 0 {
				dirent.Type = fuse.DT_Dir
			} else {
				dirent.Type = fuse.DT_File
			}
			direntList = append(direntList, dirent)
			return nil
		})
		return err
	})
	return
}

func WriteProxyFile(tx *bbolt.Tx, path string, readPathId uint64, offset int64, size int64) error {
	parent := filepath.Dir(path)
	err := MkdirAll(tx, parent)
	if err != nil {
		return err
	}
	bucket := tx.Bucket([]byte(parent))
	if bucket == nil {
		return syscall.ENOENT
	}
	data := make([]byte, 25)
	binary.BigEndian.PutUint64(data, readPathId)
	binary.BigEndian.PutUint64(data[8:], uint64(offset))
	binary.BigEndian.PutUint64(data[16:], uint64(size))
	data[24] = 1
	fileName := []byte(filepath.Base(path))
	if bucket.Get(fileName) != nil {
		return syscall.EEXIST
	}
	return bucket.Put(fileName, data)
}

func DeleteFile(path string) error {
	return db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(filepath.Dir(path)))
		if bucket == nil {
			return nil
		}
		return bucket.Delete([]byte(filepath.Base(path)))
	})
}

func DeleteDir(path string) error {
	return db.Batch(func(tx *bbolt.Tx) error {
		err := tx.DeleteBucket([]byte(path))
		if err == nil {
			bucket := tx.Bucket([]byte(filepath.Dir(path)))
			if bucket != nil {
				return bucket.Delete([]byte(filepath.Base(path)))
			}
		}
		return err
	})
}

func Lookup(path string, isDir *bool, size *int64, offset *int64, fId *uint64) error {
	return db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(filepath.Dir(path)))
		if bucket == nil {
			return syscall.ENOENT
		}
		data := bucket.Get([]byte(filepath.Base(path)))
		if data == nil {
			return syscall.ENOENT
		}
		if data[len(data)-1] == 0 {
			*isDir = true
		} else {
			*fId = binary.BigEndian.Uint64(data)
			*offset = int64(binary.BigEndian.Uint64(data[8:]))
			*size = int64(binary.BigEndian.Uint64(data[16:]))
		}
		return nil
	})
}

func PutRealFilePath(tx *bbolt.Tx, path string) (id uint64, err error) {
	rTiBucket := tx.Bucket([]byte(RealPathToIdBucketName))
	if rTiBucket == nil {
		return 0, syscall.EIO
	}
	idData := rTiBucket.Get([]byte(path))
	if idData != nil {
		id = binary.BigEndian.Uint64(idData)
		return
	} else {
		id = globalId + 1
		idData = make([]byte, 8)
		binary.BigEndian.PutUint64(idData, id)
	}
	if err = rTiBucket.Put([]byte(path), idData); err != nil {
		return
	}
	iTrBucket := tx.Bucket([]byte(IdToRealPathBucketName))
	if iTrBucket == nil {
		return 0, syscall.EIO
	}
	if err = iTrBucket.Put(idData, []byte(path)); err == nil {
		err = UpdateNextId(tx, id)
	}
	return
}

func LastRealFileId() (id uint64, err error) {
	err = db.Batch(func(tx *bbolt.Tx) error {
		bucket, cErr := tx.CreateBucketIfNotExists([]byte("ID"))
		if cErr != nil {
			return cErr
		}
		if lastId := bucket.Get([]byte("ID")); lastId != nil {
			id = binary.BigEndian.Uint64(lastId)
		}
		return nil
	})
	return
}

func UpdateNextId(tx *bbolt.Tx, id uint64) error {
	bucket := tx.Bucket([]byte("ID"))
	if bucket == nil {
		return fmt.Errorf("ID bucket not exist: %w", syscall.EIO)
	}
	err := bucket.Put([]byte("ID"), binary.BigEndian.AppendUint64([]byte{}, id))
	if err == nil {
		globalId = id
	}
	return err
}

func GetRealFilePath(id uint64) (path string, err error) {
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(IdToRealPathBucketName))
		if bucket == nil {
			return syscall.EIO
		}
		path = string(bucket.Get(binary.BigEndian.AppendUint64([]byte{}, id)))
		if path == "" {
			return syscall.ENOENT
		}
		return nil
	})
	return
}

func Sync() error {
	return db.Sync()
}

func Close() {
	if db != nil {
		_ = db.Close()
	}
}

func Batch(fn func(*bbolt.Tx) error) error {
	return db.Batch(fn)
}
