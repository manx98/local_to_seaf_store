// Clockfs implements a file system with the current time in a file.
// It was written to demonstrate kernel cache invalidation.
package main

import (
	_ "bazil.org/fuse/fs/fstestutil"
	"context"
	"errors"
	"github.com/manx98/local_to_seaf_store/commitmgr"
	"github.com/manx98/local_to_seaf_store/fsmgr"
	"github.com/manx98/local_to_seaf_store/logger"
	"github.com/manx98/local_to_seaf_store/utils"
	"github.com/manx98/local_to_seaf_store/virtualfs"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
	"log"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

const (
	modeDir  = uint32(syscall.S_IFDIR | 0644)
	modeFile = uint32(syscall.S_IFREG | 0644)
)

var appCmd = &cobra.Command{}

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "scan dir to generate fs, commit and block mapping",
	Run:   scanFs,
}
var dataDir *string
var parentCommitId *string
var scanRepoId *string
var blockSize *int64
var scanDir *string
var creator *string

var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "mount block mapping into data dir",
	Run:   mountFs,
}
var mountDataDir *string
var mountRepoId *string
var pathPrefix *string
var allowOther *bool

func main() {
	defer virtualfs.Close()
	appCmd.AddCommand(scanCmd)
	dataDir = scanCmd.Flags().StringP("data_dir", "d", "/opt/seafile/seafile-data/storage", "Commit, FS, result will be stored here")
	parentCommitId = scanCmd.Flags().StringP("parent_commit_id", "p", "363b24f55f52da85cf9eb7fa0f9c8bf30325da75", "The completion of the scan will generate a commit with this parent ID")
	scanRepoId = scanCmd.Flags().StringP("repo_id", "r", "00a57a07-79b0-4156-ab36-a556cfa54d57", "The RepoID corresponding to the scan result fs and commit")
	blockSize = scanCmd.Flags().Int64P("block_size", "s", 8*1024*1024, "block size")
	scanDir = scanCmd.Flags().StringP("scan_dir", "m", ".", "Paths to be scanned")
	creator = scanCmd.Flags().StringP("creator", "c", "admin", "fs creator")
	appCmd.AddCommand(mountCmd)
	mountDataDir = mountCmd.Flags().StringP("data_dir", "d", "/opt/seafile/seafile-data/storage", "The program will mount the blocks directory in this directory")
	mountRepoId = mountCmd.Flags().StringP("repo_id", "r", "00a57a07-79b0-4156-ab36-a556cfa54d57", "The RepoID corresponding to the scan result fs and commit")
	pathPrefix = mountCmd.Flags().StringP("path_prefix", "m", ".", "File mapping parent directory, corresponding to scan_dir in the scan")
	allowOther = mountCmd.Flags().BoolP("allow_other", "a", false, "allow_other only allowed if 'user_allow_other' is set in /etc/fuse.conf")
	if err := appCmd.Execute(); err != nil {
		log.Fatal("run cmd occur error: ", err)
	}
}

func scanFs(cmd *cobra.Command, args []string) {
	if !utils.IsValidUUID(*scanRepoId) {
		logger.Fatal("repo_id is not uuid", zap.String("repo_id", *scanRepoId))
	}
	if !utils.IsObjectIDValid(*parentCommitId) {
		logger.Fatal("parent_commit_id is not object id", zap.String("parent_commit_id", *parentCommitId))
	}
	commitmgr.Init(*dataDir)
	parentCommit, err := commitmgr.Load(*scanRepoId, *parentCommitId)
	if err != nil {
		logger.Fatal("get parent commit occur error", zap.Error(err), zap.String("scanRepoId", *scanRepoId), zap.String("parent", *parentCommitId))
	}
	fsmgr.Init(*dataDir)
	if err = virtualfs.InitVirtualFs(filepath.Join(*dataDir, "blocks_mapping.db"), false); err != nil {
		logger.Fatal("init virtual fs occur error", zap.Error(err))
	}
	sc := DirScanner{}
	rootId, err := sc.Scan(*scanDir, *scanRepoId)
	if err != nil {
		logger.Fatal("scan occur error", zap.Error(err), zap.String("scanDir", *scanDir))
		log.Fatal(err)
	}
	commit := commitmgr.NewCommit(parentCommit, rootId, *creator, "Auto blocking mapping")
	err = commitmgr.Save(commit)
	if err != nil {
		logger.Fatal("save commit occur error", zap.Error(err), zap.String("scanRepoId", *scanRepoId), zap.String("parent", *parentCommitId))
	}
	err = virtualfs.Sync()
	if err != nil {
		logger.Fatal("sync occur error", zap.Error(err))
	}
	logger.Info("scan success", zap.String("commit_id", commit.CommitID), zap.String("repo_id", *scanRepoId), zap.String("scan_dir", *scanDir))
}

type DirScanner struct {
}

func (d *DirScanner) saveProxyFile(tx *bbolt.Tx, id uint64, offset, size int64, mtime int64) (blkId string, err error) {
	for {
		blkId = utils.RandId()
		savePath := filepath.Join("/", *scanRepoId, blkId[:2], blkId[2:])
		if offset+*blockSize > size {
			err = virtualfs.WriteProxyFile(tx, savePath, id, offset, size-offset, mtime)
		} else {
			err = virtualfs.WriteProxyFile(tx, savePath, id, offset, *blockSize, mtime)
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
func (d *DirScanner) generateFile(size int64, storePath string, mtime int64) (blkId string, err error) {
	logger.Info("generate file", zap.String("path", storePath))
	err = virtualfs.Batch(func(tx *bbolt.Tx) error {
		var id uint64
		id, err = virtualfs.PutRealFilePath(tx, []byte(storePath))
		if err != nil {
			return err
		}
		var ids []string
		for i := int64(0); i < size; i += *blockSize {
			blkId, err = d.saveProxyFile(tx, id, i, size, mtime)
			if err != nil {
				return err
			}
			ids = append(ids, blkId)
		}
		var fileObj *fsmgr.Seafile
		fileObj, err = fsmgr.NewSeafile(1, size, ids)
		if err != nil {
			return err
		}
		err = fsmgr.SaveSeafile(*scanRepoId, fileObj)
		if err != nil {
			return err
		}
		blkId = fileObj.FileID
		return nil
	})
	return
}

func (d *DirScanner) Scan(parent string, storePath string) (rootId string, err error) {
	dir, err := os.ReadDir(parent)
	if err != nil {
		return "", err
	}
	entries := make([]*fsmgr.SeafDirent, 0, len(dir))
	for i := range dir {
		file := dir[i]
		filePath := filepath.Join(parent, file.Name())
		info, iErr := file.Info()
		if iErr != nil {
			return "", iErr
		}
		if file.IsDir() {
			rootId, iErr = d.Scan(filePath, storePath+"/"+file.Name())
			if iErr != nil {
				return "", iErr
			}
			entries = append(entries, fsmgr.NewDirent(rootId, file.Name(), modeDir, info.ModTime().Unix(), *creator, info.Size()))
		} else {
			rootId, iErr = d.generateFile(info.Size(), storePath+"/"+file.Name(), info.ModTime().Unix())
			if iErr != nil {
				return "", iErr
			}
			entries = append(entries, fsmgr.NewDirent(rootId, file.Name(), modeFile, info.ModTime().Unix(), *creator, info.Size()))
		}
	}
	sort.Sort(fsmgr.Dirents(entries))
	dirObj, err := fsmgr.NewSeafdir(1, entries)
	if err != nil {
		return "", err
	}
	err = fsmgr.SaveSeafdir(*scanRepoId, dirObj)
	if err != nil {
		return "", err
	}
	return dirObj.DirID, nil
}

func mountFs(cmd *cobra.Command, args []string) {
	if !utils.IsValidUUID(*mountRepoId) {
		logger.Fatal("repo_id is not uuid", zap.String("repo_id", *scanRepoId))
	}
	if err := virtualfs.InitVirtualFs(filepath.Join(*mountDataDir, "blocks_mapping.db"), true); err != nil {
		logger.Fatal("init virtual fs error", zap.Error(err))
	}
	virtualfs.Mount(context.Background(), *pathPrefix, filepath.Join(*mountDataDir, "storage", "blocks", *mountRepoId), *mountRepoId)
}
