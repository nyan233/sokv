package sokv

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
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

type Tx struct {
	isRead    bool
	tree      *BTreeDisk
	seq       uint64
	recordLog *os.File
	records   []pageRecord
}

func (tx *Tx) begin() error {
	if tx.isRead {
		tx.tree.rw.RLock()
		return nil
	} else {
		tx.tree.rw.Lock()
		tx.records = make([]pageRecord, 0, 4)
		return nil
	}
}

func (tx *Tx) commit() error {
	if tx.isRead {
		tx.tree.rw.RUnlock()
		return nil
	} else {
		defer tx.tree.rw.Unlock()
	}
	return nil
}

func (tx *Tx) Get(key []byte) (value []byte, found bool, err error) {
	return tx.tree.get(tx, key)
}

func (tx *Tx) Range(s []byte, fn func(k []byte, v []byte) bool) error {
	return tx.tree.treeRange(tx, s, fn)
}

func (tx *Tx) MinKey() ([]byte, error) {
	return tx.tree.minKey(tx)
}

func (tx *Tx) MaxKey() ([]byte, error) {
	return tx.tree.maxKey(tx)
}

func (tx *Tx) Put(key, value []byte) (isReplace bool, err error) {
	if tx.isRead {
		err = fmt.Errorf("current tx(%d) is read", tx.seq)
		return
	}
	return tx.tree.put(tx, key, value)
}

func (tx *Tx) Del(key []byte) (value []byte, found bool, err error) {
	if tx.isRead {
		err = fmt.Errorf("current tx(%d) is read", tx.seq)
		return
	}
	return tx.tree.del(tx, key)
}

type txMgr struct {
	rw        sync.RWMutex
	seq       atomic.Uint64
	recordLog *os.File
}

func (tm *txMgr) allocReadTransaction(tree *BTreeDisk) *Tx {
	return &Tx{
		isRead: true,
		seq:    tm.seq.Load(),
	}
}

func (tm *txMgr) allocWriteTransaction(tree *BTreeDisk) *Tx {
	return &Tx{
		isRead:    false,
		seq:       tm.seq.Add(1),
		recordLog: tm.recordLog,
	}
}
