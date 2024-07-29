package virtualfs

import (
	"go.etcd.io/bbolt"
	"log"
	"testing"
)

func TestInitVirtualFs(t *testing.T) {
	err := InitVirtualFs("/tmp/blocks_mapping.db")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Batch(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(RealPathToIdBucketName)).ForEach(func(k, v []byte) error {
			log.Println(string(k))
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}
