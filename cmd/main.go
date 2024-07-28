// Clockfs implements a file system with the current time in a file.
// It was written to demonstrate kernel cache invalidation.
package main

import (
	_ "bazil.org/fuse/fs/fstestutil"
	"context"
	"errors"
	"github.com/manx98/local_to_seaf_store/commitmgr"
	"github.com/manx98/local_to_seaf_store/fsmgr"
	"github.com/manx98/local_to_seaf_store/utils"
	"github.com/manx98/local_to_seaf_store/virtualfs"
	"github.com/spf13/cobra"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

var appCmd = &cobra.Command{}

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "scan dir to generate fs, commit and block mapping",
	Run:   scanFs,
}
var dataDir *string
var commitId *string
var repoId *string
var blockSize *int64
var scanDir *string

var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "mount block mapping into data dir",
	Run:   mountFs,
}
var mountDataDir *string
var pathPrefix *string

func main() {
	defer virtualfs.Close()
	appCmd.AddCommand(scanCmd)
	dataDir = scanCmd.Flags().StringP("data_dir", "d", "/opt/seafile/seafile-data/storage", "Commit, FS, result will be stored here")
	commitId = scanCmd.Flags().StringP("commit_id", "i", "363b24f55f52da85cf9eb7fa0f9c8bf30325da75", "The completion of the scan will generate a commit with this ID")
	repoId = scanCmd.Flags().StringP("repo_id", "r", "00a57a07-79b0-4156-ab36-a556cfa54d57", "The RepoID corresponding to the scan result fs and commit")
	blockSize = scanCmd.Flags().Int64P("block_size", "s", 8*1024*1024, "block size")
	scanDir = scanCmd.Flags().StringP("scan_dir", "m", ".", "Paths to be scanned")
	appCmd.AddCommand(mountCmd)
	mountDataDir = mountCmd.Flags().StringP("data_dir", "d", "/opt/seafile/seafile-data/storage", "The program will mount the blocks directory in this directory")
	pathPrefix = mountCmd.Flags().StringP("path_prefix", "m", ".", "File mapping parent directory, corresponding to scan_dir in the scan")
	if err := appCmd.Execute(); err != nil {
		log.Fatal("run cmd occur error: ", err)
	}
}

func scanFs(cmd *cobra.Command, args []string) {
	commitmgr.Init(*dataDir)
	fsmgr.Init(*dataDir)
	if err := virtualfs.InitVirtualFs(filepath.Join(*dataDir, "blocks_mapping.db")); err != nil {
		log.Fatal(err)
	}
	sc := DirScanner{}
	rootId, err := sc.Scan(*scanDir, "")
	if err != nil {
		log.Fatal(err)
	}
	commit := commitmgr.NewCommit(*commitId, *repoId, "", rootId, "root", "blocking mapping")
	err = commitmgr.Save(commit)
	if err != nil {
		log.Fatal(err)
	}
	err = virtualfs.Sync()
	if err != nil {
		log.Fatal(err)
	}
}

type DirScanner struct {
}

func (d *DirScanner) saveProxyFile(id uint64, offset, size int64) (blkId string, err error) {
	for {
		blkId = utils.RandId()
		savePath := filepath.Join("/", *repoId, blkId[:2], blkId[2:])
		if offset+*blockSize > size {
			err = virtualfs.WriteProxyFile(savePath, id, offset, size-offset)
		} else {
			err = virtualfs.WriteProxyFile(savePath, id, offset, *blockSize)
		}
		if err != nil {
			if errors.Is(err, syscall.EEXIST) {
				continue
			}
			return "", err
		}
		return
	}

}
func (d *DirScanner) generateFile(size int64, storePath string) (string, error) {
	log.Println("===>", storePath)
	id, err := virtualfs.PutRealFilePath(storePath)
	if err != nil {
		return "", err
	}
	var ids []string
	for i := int64(0); i < size; i += *blockSize {
		blkId, err := d.saveProxyFile(id, i, size)
		if err != nil {
			return "", err
		}
		ids = append(ids, blkId)
	}
	fileObj, err := fsmgr.NewSeafile(1, size, ids)
	if err != nil {
		return "", err
	}
	err = fsmgr.SaveSeafile(*repoId, fileObj)
	if err != nil {
		return "", err
	}
	return fileObj.FileID, nil
}

func (d *DirScanner) Scan(parent string, storePath string) (rootId string, err error) {
	dir, err := os.ReadDir(parent)
	if err != nil {
		return "", err
	}
	entries := make([]*fsmgr.SeafDirent, 0, len(dir))
	for _, file := range dir {
		filePath := filepath.Join(parent, file.Name())
		info, err := file.Info()
		if err != nil {
			return "", err
		}
		if file.IsDir() {
			rootId, err = d.Scan(filePath, storePath+"/"+file.Name())
			if err != nil {
				return "", err
			}
		} else {
			rootId, err = d.generateFile(info.Size(), storePath+"/"+file.Name())
			if err != nil {
				return "", err
			}
		}
		entries = append(entries, fsmgr.NewDirent(rootId, file.Name(), uint32(info.Mode()), info.ModTime().Unix(), utils.GetFileUsername(filePath), info.Size()))
	}
	dirObj, err := fsmgr.NewSeafdir(1, entries)
	if err != nil {
		return "", err
	}
	err = fsmgr.SaveSeafdir(*repoId, dirObj)
	if err != nil {
		return "", err
	}
	return dirObj.DirID, nil
}

func mountFs(cmd *cobra.Command, args []string) {
	if err := virtualfs.InitVirtualFs(filepath.Join(*mountDataDir, "blocks_mapping.db")); err != nil {
		log.Fatal(err)
	}
	virtualfs.Mount(context.Background(), *pathPrefix, *mountDataDir)
}
