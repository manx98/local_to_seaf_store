// Package commitmgr manages commit objects.
package commitmgr

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/manx98/local_to_seaf_store/objstore"
	"io"
	"time"
)

// Commit is a commit object
type Commit struct {
	CommitID       string `json:"commit_id"`
	RepoID         string `json:"repo_id"`
	RootID         string `json:"root_id"`
	CreatorName    string `json:"creator_name,omitempty"`
	CreatorID      string `json:"creator"`
	Desc           string `json:"description"`
	Ctime          int64  `json:"ctime"`
	ParentID       String `json:"parent_id"`
	SecondParentID String `json:"second_parent_id"`
	RepoName       string `json:"repo_name"`
	RepoDesc       string `json:"repo_desc"`
	RepoCategory   string `json:"repo_category"`
	DeviceName     string `json:"device_name,omitempty"`
	ClientVersion  string `json:"client_version,omitempty"`
	Encrypted      string `json:"encrypted,omitempty"`
	EncVersion     int    `json:"enc_version,omitempty"`
	Magic          string `json:"magic,omitempty"`
	RandomKey      string `json:"key,omitempty"`
	Salt           string `json:"salt,omitempty"`
	PwdHash        string `json:"pwd_hash,omitempty"`
	PwdHashAlgo    string `json:"pwd_hash_algo,omitempty"`
	PwdHashParams  string `json:"pwd_hash_params,omitempty"`
	Version        int    `json:"version,omitempty"`
	Conflict       int    `json:"conflict,omitempty"`
	NewMerge       int    `json:"new_merge,omitempty"`
	Repaired       int    `json:"repaired,omitempty"`
}

var store *objstore.ObjectStore

// Init initializes commit manager and creates underlying object store.
func Init(dataDir string) {
	store = objstore.New(dataDir, "commits")
}

// NewCommit initializes a Commit object.
func NewCommit(id, repoID, parentID, newRoot, user, desc string) *Commit {
	commit := new(Commit)
	commit.RepoID = repoID
	commit.RootID = newRoot
	commit.Desc = desc
	commit.CreatorName = user
	commit.CreatorID = "0000000000000000000000000000000000000000"
	commit.Ctime = time.Now().Unix()
	if id == "" {
		commit.CommitID = computeCommitID(commit)
	} else {
		commit.CommitID = id
	}
	if parentID != "" {
		commit.ParentID.SetValid(parentID)
	}
	return commit
}

func computeCommitID(commit *Commit) string {
	hash := sha1.New()
	hash.Write([]byte(commit.RootID))
	hash.Write([]byte(commit.CreatorID))
	hash.Write([]byte(commit.CreatorName))
	hash.Write([]byte(commit.Desc))
	tmpBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(tmpBuf, uint64(commit.Ctime))
	hash.Write(tmpBuf)

	checkSum := hash.Sum(nil)
	id := hex.EncodeToString(checkSum[:])

	return id
}

// ToData converts commit to JSON-encoded data and writes to w.
func (commit *Commit) ToData(w io.Writer) error {
	jsonstr, err := json.Marshal(commit)
	if err != nil {
		return err
	}

	_, err = w.Write(jsonstr)
	if err != nil {
		return err
	}

	return nil
}

// WriteRaw writes data in binary format to storage backend.
func WriteRaw(repoID string, commitID string, r io.Reader) error {
	err := store.Write(repoID, commitID, r, false)
	if err != nil {
		return err
	}
	return nil
}

// Save commit to storage backend.
func Save(commit *Commit) error {
	var buf bytes.Buffer
	err := commit.ToData(&buf)
	if err != nil {
		return err
	}

	err = WriteRaw(commit.RepoID, commit.CommitID, &buf)
	if err != nil {
		return err
	}

	return err
}
