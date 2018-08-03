package db

import "github.com/dgraph-io/badger"

type Bucket struct {
	name []byte
	bukcetID [4]byte
	tx *Tx
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	iterator, err := b.tx.Txn.Get(bucketKey(b.bukcetID, key))
	if err != nil {
		return nil, err
	}
	return iterator.Value()
}

func bucketKey(bucketID [4]byte, key []byte) []byte {
	bkey := make([]byte, 4+len(key))
	copy(bkey, bucketID[:])
	copy(bkey[4:], key)
	return bkey
}

func bucketID(id []byte) (rid [4]byte) {
	l := len(id)
	if l > 4 {
		l = 4
	}
	for i := 0; i < l; i++ {
		rid[i] = id[i]
	}
	return rid
}

func (b *Bucket) Set (key, value []byte) error {
	return b.tx.Txn.Set(key, value)
}

func (b *Bucket) Delete(key []byte) error {
	return b.tx.Txn.Delete(key)
}

func (b *Bucket) setTx(tx *Tx) {
	b.tx = tx
}

func (b *Bucket) ForEach(fn func(key, value []byte) error) error {
	opts := badger.DefaultIteratorOptions
	iterator := b.tx.Txn.NewIterator(opts)
	defer iterator.Close()
	prefix := b.bukcetID[:]
	for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
		myValue, err := iterator.Item().Value()
		if err != nil {
			return err
		}
		var p1, p2 []byte
		p1 = append(p1, iterator.Item().Key()[4:]...)
		p2 = append(p2, myValue...)
		err = fn(p1, p2)
		if err != nil {
			return err
		}
	}
	return nil
}