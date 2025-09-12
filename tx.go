package sokv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

const (
	pageRecordStorage uint8  = 1
	pageRecordFree    uint8  = 2
	recordStart       uint64 = 0xaabbccddeeff
	recordEnd         uint64 = 0xffeeddccbbaa
)

type pageRecordHeader struct {
	length   uint32
	checksum uint32
}

type pageRecord struct {
	typ   uint8
	pgId  pageId
	txSeq uint64
	off   uint32
	dat   []byte
}

func (p *pageRecord) minSize() uint32 {
	return 1 + uint32(pgIdMemSize) + 4
}

func (p *pageRecord) size() uint32 {
	return 1 + uint32(pgIdMemSize) + 4 + uint32(len(p.dat))
}

type txHeader struct {
	seq                     uint64
	isRead                  bool
	isRollback              bool
	isCommit                bool
	records                 []pageRecord
	storagePageChangeRecord map[uint64][]pageRecord
	freePageChangeRecord    map[uint64][]pageRecord
}

func (h *txHeader) addPageModify(r pageRecord) error {
	if r.typ == 0 {
		panic("invalid page type")
	}
	r.txSeq = h.seq
	var (
		change = h.storagePageChangeRecord
		pgId   = r.pgId.ToUint64()
	)
	if r.typ == pageRecordFree {
		change = h.freePageChangeRecord
	}
	history, ok := change[pgId]
	if !ok {
		rlist := make([]pageRecord, 0, 4)
		rlist = append(rlist, r)
		change[pgId] = rlist
	} else {
		history = append(history, r)
		change[pgId] = history
	}
	h.records = append(h.records, r)
	return nil
}

type Tx[K any, V any] struct {
	header    *txHeader
	tree      *BTreeDisk[K, V]
	recordLog *os.File
}

func (tx *Tx[K, V]) isReadOnly() bool {
	return tx.header.isRead
}

func (tx *Tx[K, V]) begin() error {
	if tx.header.isRead {
		tx.tree.rw.RLock()
		return nil
	} else {
		tx.tree.rw.Lock()
		tx.header.records = make([]pageRecord, 0, 4)
		tx.header.storagePageChangeRecord = make(map[uint64][]pageRecord, 8)
		tx.header.freePageChangeRecord = make(map[uint64][]pageRecord, 8)
		return nil
	}
}

func (tx *Tx[K, V]) Rollback() error {
	if tx.header.isRead {
		panic("current tx is read")
	}
	if tx.header.isRollback {
		panic(fmt.Errorf("current tx already rollback : %d", tx.header.seq))
	}
	tx.header.records = nil
	tx.tree.rollbackDirtyPage()
	tx.header.isRollback = true
	defer tx.tree.rw.Unlock()
	return tx.recordLog.Truncate(0)
}

func (tx *Tx[K, V]) checkAbleUse() error {
	if !tx.header.isRead && tx.header.isRollback {
		return errors.New("current tx is rollback")
	} else if tx.header.isCommit {
		return errors.New("current tx is commit")
	} else {
		return nil
	}
}

func (tx *Tx[K, V]) commit() error {
	if err := tx.checkAbleUse(); err != nil {
		return err
	}
	tx.header.isCommit = true
	if tx.header.isRead {
		tx.tree.rw.RUnlock()
		return nil
	} else {
		defer tx.tree.rw.Unlock()
		return tx.doCommit()
	}
}

func (tx *Tx[K, V]) doCommit() error {
	// 先写record log, 确保其落盘
	writeCount, err := tx.recordLog.Write(binary.BigEndian.AppendUint64(nil, recordStart))
	if err != nil {
		return err
	}
	if writeCount != 8 {
		return fmt.Errorf("write count %d not equal 8", writeCount)
	}
	var buf bytes.Buffer
	buf.Grow(int(tx.tree.s.getPageSize()))
	for _, record := range tx.header.records {
		buf.Reset()
		// 先给header留位置
		buf.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})
		buf.WriteByte(record.typ)
		buf.Write(record.pgId[:])
		err = binary.Write(&buf, binary.BigEndian, record.txSeq)
		if err != nil {
			return err
		}
		err = binary.Write(&buf, binary.BigEndian, record.off)
		if err != nil {
			return err
		}
		buf.Write(record.dat)
		writeData := buf.Bytes()
		header := pageRecordHeader{
			length:   uint32(len(writeData)) - 8,
			checksum: crc32.ChecksumIEEE(writeData[8:]),
		}
		binary.BigEndian.PutUint32(writeData[0:4], header.length)
		binary.BigEndian.PutUint32(writeData[4:8], header.checksum)
		writeCount, err = tx.recordLog.Write(writeData)
		if err != nil {
			return err
		}
		if writeCount != len(writeData) {
			return fmt.Errorf("write count %d not equal %d", writeCount, len(writeData))
		}
	}
	writeCount, err = tx.recordLog.Write(binary.BigEndian.AppendUint64(nil, recordEnd))
	if err != nil {
		return err
	}
	if writeCount != 8 {
		return fmt.Errorf("write count %d not equal 8", writeCount)
	}
	err = tx.recordLog.Sync()
	if err != nil {
		return err
	}
	return tx.tree.doWritePageData(tx.header)
}

func (tx *Tx[K, V]) addPageModify(r pageRecord) error {
	return tx.header.addPageModify(r)
}

func (tx *Tx[K, V]) Get(key K) (value V, found bool, err error) {
	if err = tx.checkAbleUse(); err != nil {
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
	if !found {
		return
	}
	err = tx.tree.valCodec.Unmarshal(valBytes, &value)
	return
}

func (tx *Tx[K, V]) Range(s K, fn func(k K, v V) bool) error {
	if err := tx.checkAbleUse(); err != nil {
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
	if err = tx.checkAbleUse(); err != nil {
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
	if err = tx.checkAbleUse(); err != nil {
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
	if err = tx.checkAbleUse(); err != nil {
		return
	}
	if tx.header.isRead {
		err = fmt.Errorf("current tx(%d) is read", tx.header.seq)
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
	if err = tx.checkAbleUse(); err != nil {
		return
	}
	if tx.header.isRead {
		err = fmt.Errorf("current tx(%d) is read", tx.header.seq)
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
