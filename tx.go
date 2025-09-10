package sokv

import (
	"errors"
	"fmt"
	"os"
)

const (
	pageRecordAlloc  = 1
	pageRecordFree   = 2
	pageRecordModify = 3
)

type pageRecord struct {
	typ  uint8
	pgId pageId
	off  uint32
	dat  []byte
}

type Tx[K any, V any] struct {
	isRead     bool
	isRollback bool
	tree       *BTreeDisk[K, V]
	seq        uint64
	recordLog  *os.File
	records    []pageRecord
}

func (tx *Tx[K, V]) begin() error {
	if tx.isRead {
		tx.tree.rw.RLock()
		return nil
	} else {
		tx.tree.rw.Lock()
		tx.records = make([]pageRecord, 0, 4)
		return nil
	}
}

func (tx *Tx[K, V]) Rollback() error {
	if tx.isRead {
		panic("current tx is read")
	}
	tx.records = nil
	tx.tree.rollbackDirtyPage()
	tx.isRollback = true
	defer tx.tree.rw.Unlock()
	return tx.recordLog.Truncate(0)
}

func (tx *Tx[K, V]) checkRollback() error {
	if !tx.isRead && tx.isRollback {
		return errors.New("current tx is rollback")
	} else {
		return nil
	}
}

func (tx *Tx[K, V]) commit() error {
	if tx.isRollback {
		return nil
	}
	if tx.isRead {
		tx.tree.rw.RUnlock()
		return nil
	} else {
		defer tx.tree.rw.Unlock()
	}
	return nil
}

func (tx *Tx[K, V]) Get(key K) (value V, found bool, err error) {
	if err = tx.checkRollback(); err != nil {
		return
	}
	var (
		keyBytes, valBytes []byte
	)
	keyBytes, err = tx.tree.keyCodec.Marshal(&key)
	if err != nil {
		return
	}
	valBytes, found, err = tx.tree.get(tx, keyBytes)
	if err != nil {
		return
	}
	err = tx.tree.valCodec.Unmarshal(valBytes, &value)
	return
}

func (tx *Tx[K, V]) Range(s K, fn func(k K, v V) bool) error {
	if err := tx.checkRollback(); err != nil {
		return err
	}
	keyBytes, err := tx.tree.keyCodec.Marshal(&s)
	if err != nil {
		return err
	}
	return tx.tree.treeRange(tx, keyBytes, func(key, val []byte) bool {
		var (
			gKey K
			gVal V
		)
		err := tx.tree.keyCodec.Unmarshal(key, &gKey)
		if err != nil {
			panic(err)
		}
		err = tx.tree.valCodec.Unmarshal(val, &gVal)
		if err != nil {
			panic(err)
		}
		return fn(gKey, gVal)
	})
}

func (tx *Tx[K, V]) MinKey() (key K, err error) {
	if err = tx.checkRollback(); err != nil {
		return
	}
	var keyBytes []byte
	keyBytes, err = tx.tree.minKey(tx)
	if err != nil {
		return
	}
	err = tx.tree.keyCodec.Unmarshal(keyBytes, &key)
	return
}

func (tx *Tx[K, V]) MaxKey() (key K, err error) {
	if err = tx.checkRollback(); err != nil {
		return
	}
	var keyBytes []byte
	keyBytes, err = tx.tree.maxKey(tx)
	if err != nil {
		return
	}
	err = tx.tree.keyCodec.Unmarshal(keyBytes, &key)
	return
}

func (tx *Tx[K, V]) Put(key K, value V) (isReplace bool, err error) {
	if err = tx.checkRollback(); err != nil {
		return
	}
	if tx.isRead {
		err = fmt.Errorf("current tx(%d) is read", tx.seq)
		return
	}
	keyBytes, err := tx.tree.keyCodec.Marshal(&key)
	if err != nil {
		return false, err
	}
	valBytes, err := tx.tree.valCodec.Marshal(&value)
	if err != nil {
		return false, err
	}
	return tx.tree.put(tx, keyBytes, valBytes)
}

func (tx *Tx[K, V]) Del(key K) (value V, found bool, err error) {
	if err = tx.checkRollback(); err != nil {
		return
	}
	if tx.isRead {
		err = fmt.Errorf("current tx(%d) is read", tx.seq)
		return
	}
	var (
		keyBytes []byte
		valBytes []byte
	)
	keyBytes, err = tx.tree.keyCodec.Marshal(&key)
	if err != nil {
		return
	}
	valBytes, found, err = tx.tree.del(tx, keyBytes)
	if err != nil {
		return
	}
	err = tx.tree.valCodec.Unmarshal(valBytes, &value)
	return
}
