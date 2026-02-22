package sokv

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nyan233/sokv/internal/sys"
	"os"
	"slices"
)

type freelist2Pos struct {
	f         *freelist2
	buf       []byte
	globalIdx uint32
	innerIdx  uint32
	pgId      uint32
}

func (p *freelist2Pos) get() uint32 {
	return binary.BigEndian.Uint32(p.buf[p.innerIdx*uint32(pgIdMemSize) : (p.innerIdx+1)*uint32(pgIdMemSize)])
}

func (p *freelist2Pos) set(txh *txHeader, v uint32) error {
	return p.f.modifyPage(txh, p.pgId, p.buf, func(b []byte) ([]byte, error) {
		s := p.innerIdx * uint32(pgIdMemSize)
		binary.BigEndian.PutUint32(b[s:], v)
		err := txh.addPageModify(pageRecord{
			typ:  pageRecordFree,
			pgId: p.pgId,
			off:  p.innerIdx * uint32(pgIdMemSize),
			dat:  b[s : s+4],
		})
		if err != nil {
			return b, err
		} else {
			return b, nil
		}
	})
}

func (p *freelist2Pos) setWithIdx(txh *txHeader, innerIdx uint64, v uint32) error {
	return p.f.modifyPage(txh, p.pgId, p.buf, func(b []byte) ([]byte, error) {
		s := innerIdx * uint64(pgIdMemSize)
		binary.BigEndian.PutUint32(b[s:], v)
		err := txh.addPageModify(pageRecord{
			typ:  pageRecordFree,
			pgId: p.pgId,
			off:  uint32(innerIdx) * uint32(pgIdMemSize),
			dat:  b[s : s+4],
		})
		if err != nil {
			return b, err
		} else {
			return b, nil
		}
	})
}

type freelist2 struct {
	file   *os.File
	opt    *pageStorageOption
	cache  pageCacheI
	cipher Cipher
}

func newFreelist2(opt *pageStorageOption) *freelist2 {
	return &freelist2{
		opt:    opt,
		cache:  newLFUCache(opt.FreelistMaxCacheSize),
		cipher: opt.PageCipher,
	}
}

func (f *freelist2) init() (err error) {
	f.file, err = sys.OpenFile(f.opt.FreelistPath)
	if err != nil {
		return
	}
	var stat os.FileInfo
	stat, err = f.file.Stat()
	if err != nil {
		return
	}
	fileSize := stat.Size()
	if fileSize == 0 {
		return f.initFile()
	} else {
		return nil
	}
}

func (f *freelist2) close() (err error) {
	err = f.file.Close()
	if err != nil {
		return
	}
	f.file = nil
	f.cache = nil
	return
}

func (f *freelist2) initFile() (err error) {
	err = f.file.Truncate(int64(recordSize))
	if err != nil {
		f.opt.Logger.Error("freelist2 truncate fail", "err", err)
		return
	}
	pgIdList := make([]uint32, 0, defaultPageCount)
	for i := 3; i < defaultPageCount; i++ {
		pgIdList = append(pgIdList, uint32(i))
	}
	err = f.build(nil, pgIdList)
	if err != nil {
		f.opt.Logger.Error("freelist2 build fail", "err", err)
	}
	return
}

func (f *freelist2) readPage(txh *txHeader, pageId uint32) (buf []byte, err error) {
	if !txh.valid() {
		buf, err = f.readRawPage(pageId)
		return
	}
	if !txh.isWriteTx() {
		panic("not write tx")
	}
	cp, found := f.cache.getPage(txh, pageId)
	if found {
		return cp.data, nil
	}
	buf, err = f.readRawPage(pageId)
	if err != nil {
		return
	}
	f.cache.putPage(txh, pageId, cachePage{
		txSeq: txh.seq,
		data:  buf,
		pgId:  pageId,
	})
	return
}

func (f *freelist2) readRawPage(pageId uint32) ([]byte, error) {
	buf := make([]byte, recordSize)
	readCount, err := f.file.ReadAt(buf, int64(pageId)*int64(recordSize))
	if err != nil {
		return nil, err
	}
	if readCount != len(buf) {
		err = errors.New("read count not equal size")
		return nil, err
	} else {
		return buf, nil
	}
}

func (f *freelist2) writeRawPage(pageId uint32, buf []byte) error {
	writeCount, err := f.file.WriteAt(buf, int64(pageId)*int64(recordSize))
	if err != nil {
		return err
	}
	if writeCount != len(buf) {
		return errors.New("write count not equal size")
	}
	return nil
}

func (f *freelist2) position(txh *txHeader, globalIdx uint32) (*freelist2Pos, error) {
	var (
		pos = new(freelist2Pos)
		err error
	)
	pos.f = f
	pos.globalIdx = globalIdx
	// 第一个页的第一个值是用来存储length的, 并不是真实的值
	pageIdCount := uint32(recordSize) / uint32(pgIdMemSize)
	pos.pgId = globalIdx / pageIdCount
	pos.innerIdx = globalIdx % pageIdCount
	pos.buf, err = f.readPage(txh, pos.pgId)
	return pos, err
}

func (f *freelist2) growFile() (err error) {
	var stat os.FileInfo
	stat, err = f.file.Stat()
	if err != nil {
		return
	}
	fileSize := stat.Size()
	// 大于1MB后每次只增长1MB
	if fileSize > 1024*1024 {
		fileSize += 1024 * 1024
	} else {
		fileSize *= 2
	}
	return f.file.Truncate(fileSize)
}

func (f *freelist2) isFull(length uint32) (bool, error) {
	pageIdCount := recordSize / uint32(pgIdMemSize)
	requirePage := length / pageIdCount
	stat, err := f.file.Stat()
	if err != nil {
		return false, err
	}
	if int64(requirePage*recordSize) > stat.Size() {
		return true, nil
	} else {
		return false, nil
	}
}

func (f *freelist2) modifyPage(txh *txHeader, pageId uint32, buf []byte, fn func(b []byte) ([]byte, error)) (err error) {
	if !txh.valid() {
		buf, err = fn(buf)
		if err != nil {
			return
		}
		return f.writeRawPage(pageId, buf)
	}
	if !txh.isWriteTx() {
		panic("not write tx")
	}
	f.cache.opShadowPage(txh, cachePage{
		txSeq: txh.seq,
		data:  buf,
		pgId:  pageId,
	}, func(p cachePage) (cachePage, bool) {
		p.data, err = fn(p.data)
		if err != nil {
			return cachePage{}, false
		} else {
			return p, true
		}
	})
	return
}

func (f *freelist2) build(txh *txHeader, pgIdList []uint32) (err error) {
	isSorted := slices.IsSorted(pgIdList)
	if !isSorted {
		return fmt.Errorf("pgIdList not sorted")
	}
	// 分组写入
	var (
		idx         int
		pageIdCount = recordSize / uint32(pgIdMemSize)
		buf         []byte
	)
	for len(pgIdList) > 0 {
		var stat os.FileInfo
		stat, err = f.file.Stat()
		if err != nil {
			return
		}
		if stat.Size() <= int64(idx)*int64(recordSize) {
			err = f.growFile()
			if err != nil {
				return
			}
		}
		buf, err = f.readPage(txh, uint32(idx))
		if err != nil {
			return
		}
		maxWrite := pageIdCount
		if idx == 0 {
			maxWrite--
		}
		if int(maxWrite) > len(pgIdList) {
			maxWrite = uint32(len(pgIdList))
		}
		err = f.modifyPage(txh, uint32(idx), buf, func(buf2 []byte) ([]byte, error) {
			if idx == 0 {
				//writeLen := createPageIdFromUint64(uint64(len(pgIdList)))
				//copy(buf2[:pgIdMemSize], writeLen[:])
				binary.BigEndian.PutUint32(buf2, uint32(len(pgIdList)))
				for i := 0; i < int(maxWrite); i++ {
					//copy(buf2[uintptr(i+1)*pgIdMemSize:], pgIdList[i][:])
					binary.BigEndian.PutUint32(buf2[uintptr(i+1)*pgIdMemSize:], pgIdList[i])
				}
			} else {
				for i := 0; i < int(maxWrite); i++ {
					//copy(buf2[uintptr(i)*pgIdMemSize:], pgIdList[i][:])
					binary.BigEndian.PutUint32(buf2[uintptr(i)*pgIdMemSize:], pgIdList[i])
				}
			}
			// 非初始化时写入页修改记录
			if txh.valid() {
				err = txh.addPageModify(pageRecord{
					typ:  pageRecordFree,
					pgId: uint32(idx),
					off:  0,
					dat:  buf2,
				})
				if err != nil {
					return buf2, err
				}
			}
			return buf2, nil
		})
		if err != nil {
			return
		}
		pgIdList = pgIdList[maxWrite:]
		idx++
	}
	return nil
}

func (f *freelist2) len(txh *txHeader) (uint32, error) {
	pos, err := f.position(txh, 0)
	if err != nil {
		return 0, err
	}
	return pos.get(), nil
}

func (f *freelist2) swap(txh *txHeader, i, j uint32) error {
	ipos, err := f.position(txh, i)
	if err != nil {
		return err
	}
	jpos, err := f.position(txh, j)
	if err != nil {
		return err
	}
	iVal := ipos.get()
	jVal := jpos.get()
	err = ipos.set(txh, jVal)
	if err != nil {
		return err
	}
	err = jpos.set(txh, iVal)
	if err != nil {
		return err
	}
	return nil
}

func (f *freelist2) less(txh *txHeader, i, j uint32) (bool, error) {
	ipos, err := f.position(txh, i)
	if err != nil {
		return false, err
	}
	jpos, err := f.position(txh, j)
	if err != nil {
		return false, err
	}
	return cmp.Less(ipos.get(), jpos.get()), nil
}

func (f *freelist2) push(txh *txHeader, v uint32) error {
	maxIdx, err := f.doPush(txh, v)
	if err != nil {
		return err
	}
	return f.up(txh, maxIdx)
}

func (f *freelist2) pop(txh *txHeader) (p uint32, err error) {
	var (
		lengthPos *freelist2Pos
		length    uint32
	)
	lengthPos, err = f.position(txh, 0)
	if err != nil {
		return 0, err
	}
	length = lengthPos.get()
	if length == 0 {
		return 0, errNoAvailablePage
	}
	p, err = f.doPop(txh, length)
	if err != nil {
		return 0, err
	}
	err = lengthPos.set(txh, length-1)
	if err != nil {
		return 0, err
	}
	err = f.down(txh, length-1)
	return
}

func (f *freelist2) doPush(txh *txHeader, v uint32) (maxIdx uint32, err error) {
	var (
		isFull bool
		length uint32
	)
	lengthPos, err := f.position(txh, 0)
	if err != nil {
		return 0, err
	}
	length = lengthPos.get()
	if length > 0 {
		isFull, err = f.isFull(length + 1)
		if err != nil {
			return 0, err
		}
	}
	if isFull {
		err = f.growFile()
		if err != nil {
			return 0, err
		}
	}
	putPos, err := f.position(txh, length+1)
	if err != nil {
		return 0, err
	}
	err = lengthPos.set(txh, length+1)
	if err != nil {
		return 0, err
	}
	maxIdx = length + 1
	err = putPos.set(txh, v)
	if err != nil {
		return 0, err
	}
	return
}

func (f *freelist2) doPop(txh *txHeader, length uint32) (p uint32, err error) {
	var (
		lastPos, firstPos *freelist2Pos
	)
	lastPos, err = f.position(txh, length)
	if err != nil {
		return 0, err
	}
	firstPos, err = f.position(txh, 1)
	if err != nil {
		return 0, err
	}
	p = firstPos.get()
	err = firstPos.set(txh, lastPos.get())
	return
}

func (f *freelist2) up(txh *txHeader, endIdx uint32) error {
	currentIdx := endIdx
	for {
		parentIdx := currentIdx / 2
		if parentIdx < 1 {
			break
		}
		isLess, err := f.less(txh, currentIdx, parentIdx)
		if err != nil {
			return err
		}
		if isLess {
			err = f.swap(txh, currentIdx, parentIdx)
			if err != nil {
				return err
			}
		} else {
			break
		}
		currentIdx = parentIdx
	}
	return nil
}

func (f *freelist2) down(txh *txHeader, endIdx uint32) error {
	var idx uint32 = 1
	for {
		leftSubIdx := idx * 2
		rightSubIdx := idx*2 + 1
		if leftSubIdx > endIdx {
			break
		}
		cmpIdx := leftSubIdx
		if rightSubIdx <= endIdx {
			isLess, err := f.less(txh, rightSubIdx, leftSubIdx)
			if err != nil {
				return err
			}
			if isLess {
				cmpIdx = rightSubIdx
			}
		}
		isLess, err := f.less(txh, cmpIdx, idx)
		if err != nil {
			return err
		}
		if isLess {
			err = f.swap(txh, cmpIdx, idx)
			if err != nil {
				return err
			}
		} else {
			break
		}
		idx = cmpIdx
	}
	return nil
}

func (f *freelist2) flushShadowPage2Disk(txh *txHeader) error {
	changeList := txh.getChangeList(pageRecordFree)
	if len(changeList) == 0 {
		return nil
	}
	for _, pgId := range changeList {
		p, ok := f.cache.getShadowPageChange(txh, pgId)
		if !ok {
			return fmt.Errorf("tx have page change, but no shadow page : %d", pgId)
		}
		err := f.writeRawPage(pgId, p.data)
		if err != nil {
			return err
		}
	}
	return nil
}
