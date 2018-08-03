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
	if _, err := t.rootBucket.Get(append([]byte(PREFIX_BUCKET), name...)); err == nil || err != badger.ErrKeyNotFound {
		if err == nil {
			err = fmt.Errorf("bucket already exist")
		}
		return nil, err
	}
	//Bucket ID 取得
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 1
	iterator := t.Txn.NewIterator(opts)
	prefix := bucketKey(t.rootBucket.bukcetID, []byte(PREFIX_FREEBID))
	iterator.Seek(prefix)
	var bid []byte
	var err error
	if iterator.ValidForPrefix(prefix) {
		bid, err = iterator.Item().Value()
		if err != nil {
			return nil, err
		}
		t.Txn.Delete(iterator.Item().Key())
	} else {
		bid, err = t.rootBucket.Get([]byte(ROOT_KEY_BID))
		if err != nil {
			return nil, err
		}
		numuid := data.ByteToUint32(bid)
		if numuid >= 1<<32-1 {
			return nil, fmt.Errorf("bucketid out of range")
		}
		newbid := data.Uint32ToByte(numuid + 1)
		err = t.rootBucket.Set([]byte(ROOT_KEY_BID), newbid)
		if err != nil {
			return nil, err
		}
	}
	//bucket 信息更新
	err = t.rootBucket.Set(append([]byte(PREFIX_BUCKET), name...), bid)
	if err != nil {
		return nil, err
	}
	return &Bucket{name, bucketID(bid), t}, nil
}

func (t *Tx) Bucket(name []byte) (*Bucket, error) {
	bid, err := t.rootBucket.Get(append([]byte(PREFIX_BUCKET), name...))
	if err != nil {
		return nil, err
	}
	return &Bucket{name, bucketID(bid), t}, nil
}

func (t *Tx) DeleteBucket(name []byte) error {
	b, err := t.Bucket(name)
	if err != nil {
		return err
	}
	//bucket 信息更新
	err = t.rootBucket.Delete(append([]byte(PREFIX_BUCKET), name...))
	if err != nil {
		return err
	}
	err = t.rootBucket.Delete(append([]byte(PREFIX_BID), b.bukcetID[:]...))
	if err != nil {
		return err
	}
	err = t.rootBucket.Set(append([]byte(PREFIX_RECOVERYID), b.bukcetID[:]...), b.bukcetID[:])
	if err != nil {
		return err
	}
	return nil
}

func (t *Tx) CreateBucketIfNotExist(name []byte) (*Bucket, error) {
	_, err := t.rootBucket.Get(append([]byte(PREFIX_BUCKET), name...))
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return nil, err
		} else {
			return t.CreateBucket(name)
		}
	}
	return t.Bucket(name)
}