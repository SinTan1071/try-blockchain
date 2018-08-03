package db

import (
	"github.com/dgraph-io/badger"
	"fmt"
)

type Tx struct {
	Txn *badger.Txn
	rootBucket *Bucket
}

func (t *Tx) Commit() error {
	return t.Txn.Commit(nil)
}

func (t *Tx) Discard() {
	t.Txn.Discard()
}

func (t *Tx) CreateBucket(name []byte) (*Bucket, error) {
	if t.Txn == nil {
		return nil, fmt.Errorf("create bucket txn is nil !")
	}
	//先检查bucket是否存在
	if _, err := t.rootBucket.Get(append([]byte(PREFIX_BUCKET)))
	return nil, nil
}