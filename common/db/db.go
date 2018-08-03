package db

import (
	"github.com/dgraph-io/badger"
	"os"
	"sync"
	"path/filepath"
	"time"
)

type DB struct {
	badgerdb *badger.DB
	rootbucket *Bucket
	closeDone chan struct{}
	closeFile *os.File
	sync.RWMutex
}

const (
	PREFIX_BUCKET = "Bucket_"
	PREFIX_BID = "ID_"
	PREFIX_FREEBID = "FREEBID"
	PREFIX_RECOVERYID = "RECID"
	ROOT_KEY_BID = "BID"
	ROOT_BUCKET_NAME = "rootBucket"
)

func Open(path string) (db *DB, err error) {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	opts.ValueLogFileSize = 1 << 28
	lockFile := filepath.Join(opts.Dir, "LOCK")
	_ = os.Remove(lockFile)
	bdb, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	closeFile, err := os.OpenFile(lockFile, os.O_EXCL, 0)
	if err != nil {
		return nil, err
	}
	rb := &Bucket{}
	bid := data.Uint32ToByte(0)
	rb.bukcetID = bucketID(bid)
	rb.name = []byte(ROOT_BUCKET_NAME)
	err = bdb.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey(rb.bukcetID, []byte(ROOT_KEY_BID)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				txn.Set(bucketKey(rb.bukcetID, []byte(ROOT_KEY_BID)), data.Uint32ToByte(10))
				txn.Set(bucketKey(rb.bukcetID, []byte(PREFIX_BUCKET+ROOT_BUCKET_NAME)), []byte{0, 0, 0, 0})
				txn.Set(bucketKey(rb.bukcetID, append([]byte(PREFIX_BID), []byte{0, 0, 0, 0}...)), rb.name)
			} else {
				return err
			}
		}
		return nil
	})
	closeDone := make(chan struct{})
	retdb := &DB{
		badgerdb: bdb,
		rootbucket: rb,
		closeDone: closeDone,
		closeFile: closeFile,
	}
	// 回收bucket逻辑
	go func() {
		after := time.After(time.Second)
		for {
			select {
			case <-after:
				var recoverBIDs [][]byte
				err := retdb.View(func(txn *Tx) error {
					return txn.rootBucket.ForEachKeyOnlyForPrefix([]byte(PREFIX_RECOVERYID), func(key []byte) error {
						recoverBIDs = append(recoverBIDs, key)
						return nil
					})
				})
				if err != nil {
					continue
				}
				for _, key := range recoverBIDs {
					var deletkeys [][]byte
					err := retdb.View(func(txn *Tx) error {
						opts := badger.DefaultIteratorOptions
						opts.PrefetchValues = false
						iterator := txn.Txn.NewIterator(opts)
						defer iterator.Close()
						prefix := key[6:]
						for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
							deletkeys = append(deletkeys, iterator.Item().Key())
						}
						return nil
					})
					if err != nil {
						continue
					}
					for _, key := range deletkeys {
						select {
						case <-closeDone:
							return
						default:
							err := retdb.Update(func(txn *Tx) error {
								return txn.Txn.Delete(key)
							})
							if err != nil {
								continue
							}
						}
					}
					select {
					case <-closeDone:
						return
					default:
						retdb.Update(func(txn *Tx) error {
							err = txn.rootBucket.Set(append([]byte(PREFIX_FREEBID), key[6:]...), key[6:])
							if err != nil {
								return err
							}
							err = txn.rootBucket.Delete(append([]byte(PREFIX_RECOVERYID), key[6:]...))
							if err != nil {
								return err
							}
							return nil
						})
					}
				}
				after = time.After(time.Second)
			case <- closeDone:
				return
			}
		}
	}()
	return retdb, nil
}

func (db *DB) Close() error {
	close(db.closeDone)
	db.closeFile.Close()
	return db.badgerdb.Close()
}

func (db *DB) View(fn func(txn *Tx) error) error {
	db.RLock()
	defer db.RUnlock()
	txn := db.NewTx(false)
	defer txn.Discard()
	return fn(txn)
}

func (db *DB) Update(fn func(txn *Tx) error) error {
	db.Lock()
	defer db.Unlock()
	txn := db.NewTx(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (db *DB) NewTx(update bool) *Tx {
	txn := db.badgerdb.NewTransaction(update)
	rb := &Bucket{name: []byte(ROOT_BUCKET_NAME), bukcetID: [4]byte{0, 0, 0, 0}}
	tx := &Tx{txn, rb}
	rb.setTx(tx)
	return tx
}