package sokv

import (
	"cmp"
	"errors"
	"fmt"
	"github.com/nyan233/sokv/internal/sys"
	"os"
	"unsafe"
)

var (
	defaultFreelistHeapByteSize = unsafe.Sizeof(pageId{}) * 4096
)

type pageIdPos struct {
	pg        freelistPage
	pgId      uint64
	globalIdx uint64
	innerIdx  uint64
}

func (p *pageIdPos) get() pageId {
	return p.pg.pgIdList[p.innerIdx]
}

func (p *pageIdPos) setPageIdFrom(v uint64) {
	p.pg.pgIdList[p.innerIdx].FromUint64(v)
}

func (p *pageIdPos) setPageId(v pageId) {
	p.pg.pgIdList[p.innerIdx] = v
}

type freelistPage struct {
	rawBuf   []byte
	pgIdList []pageId
}

func (p *freelistPage) parse(buf []byte) {
	p.rawBuf = buf
	p.pgIdList = make([]pageId, 0, 256)
	idx := 0
	for {
		if idx+int(pgIdMemSize) > len(buf) {
			break
		}
		p.pgIdList = append(p.pgIdList, pageId(buf[idx:idx+int(pgIdMemSize)]))
		idx += int(pgIdMemSize)
	}
}

func (p *freelistPage) writePgIdListToRawBuf() {
	if len(p.pgIdList)*int(pgIdMemSize) > len(p.rawBuf) {
		panic(fmt.Errorf("pageIdList byte size overflow of pageSize(%d)", len(p.rawBuf)))
	}
	for i := 0; i < len(p.pgIdList); i++ {
		copy(p.rawBuf[i:i*int(pgIdMemSize)], p.pgIdList[i][:])
	}
}

type freelist struct {
	file        *os.File
	path        string
	sysPageSize uint32
	cache       *pageCache
}

func newFreelist(path string, sysPageSize uint32) *freelist {
	return &freelist{
		path:        path,
		sysPageSize: sysPageSize,
	}
}

func (f *freelist) init() (err error) {
	f.file, err = sys.OpenFile(f.path)
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

func (f *freelist) close() (err error) {
	err = f.file.Close()
	if err != nil {
		return
	}
	f.file = nil
	return
}

func (f *freelist) initFile() (err error) {
	return f.file.Truncate(int64(defaultFreelistHeapByteSize))
}

func (f *freelist) growFile() (err error) {
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

func (f *freelist) readPage(txSeq, pageId uint64) (p freelistPage, err error) {
	cp, found := f.cache.readPage(pageId)
	if found {
		p.parse(cp.data)
		return
	}
	var (
		buf       = make([]byte, f.sysPageSize)
		readCount int
	)
	readCount, err = f.file.ReadAt(buf, int64(pageId)*int64(f.sysPageSize))
	if err != nil {
		return
	}
	if readCount != len(buf) {
		err = errors.New("read count not equal size")
		return
	}
	p.parse(buf)
	f.cache.setReadValue(cachePage{
		txSeq: txSeq,
		data:  p.rawBuf,
		pgId:  createPageIdFromUint64(pageId),
	})
	return
}

func (f *freelist) writePage(txSeq, pageId uint64, page freelistPage) error {
	page.writePgIdListToRawBuf()
	// 初始化时直接写
	if txSeq == 0 {
		writeCount, err := f.file.WriteAt(page.rawBuf, int64(pageId)*int64(f.sysPageSize))
		if err != nil {
			return err
		}
		if writeCount != len(page.rawBuf) {
			return errors.New("write count not equal size")
		}
		return nil
	} else {
		f.cache.setDirtyPage(cachePage{
			txSeq: txSeq,
			data:  page.rawBuf,
			pgId:  createPageIdFromUint64(pageId),
		})
		return nil
	}
}

func (f *freelist) readPageWithPgIdIdx(txSeq, idx uint64) (pos pageIdPos, err error) {
	pos.globalIdx = idx
	// 因为第一个页的第一个值是用来存储length的, 并不是真实的值, 所以需要略过
	idx++
	pageIdCount := f.sysPageSize / uint32(pgIdMemSize)
	pos.pgId = idx / uint64(pageIdCount)
	pos.innerIdx = idx % uint64(pageIdCount)
	pos.pg, err = f.readPage(txSeq, pos.pgId)
	if err != nil {
		return
	}
	return
}

func (f *freelist) isFull(length uint64) (bool, error) {
	pageIdCount := uint64(f.sysPageSize / uint32(pgIdMemSize))
	requirePage := length / pageIdCount
	stat, err := f.file.Stat()
	if err != nil {
		return false, err
	}
	if int64(requirePage*uint64(f.sysPageSize)) > stat.Size() {
		return true, nil
	} else {
		return false, nil
	}
}

func (f *freelist) popOne(txSeq uint64) (p pageId, found bool, err error) {
	return f.popPageId(txSeq)
}

func (f *freelist) pop(txSeq uint64, n int) ([]pageId, error) {
	res := make([]pageId, 0, n)
	for i := 0; i < n; i++ {
		p, found, err := f.popPageId(txSeq)
		if err != nil {
			return nil, err
		}
		if found {
			res = append(res, p)
		} else {
			break
		}
	}
	return res, nil
}

func (f *freelist) popPageId(txSeq uint64) (p pageId, found bool, err error) {
	var (
		firstPage freelistPage
		pos       pageIdPos
	)
	firstPage, err = f.readPage(txSeq, 0)
	if err != nil {
		return
	}
	currentLength := firstPage.pgIdList[0].ToUint64()
	if currentLength == 0 {
		return
	} else if currentLength == 1 {
		p = firstPage.pgIdList[1]
		found = true
		firstPage.pgIdList[0].FromUint64(0)
		err = f.writePage(txSeq, 0, firstPage)
		return
	}
	maxIdx := firstPage.pgIdList[0].ToUint64() - 1
	p = firstPage.pgIdList[1]
	found = true
	// 定位maxIdx所在的page
	pos, err = f.readPageWithPgIdIdx(txSeq, maxIdx)
	if err != nil {
		return
	}
	// f.data[0] = f.data[maxIdx]
	firstPage.pgIdList[1] = pos.get()
	// f.length.FromUint64(maxIdx)
	firstPage.pgIdList[0].FromUint64(maxIdx)
	err = f.writePage(txSeq, 0, firstPage)
	if err != nil {
		return
	}
	maxIdx--
	var (
		idx        uint64
		lPos, rPos pageIdPos
	)
	for {
		if idx == maxIdx {
			break
		}
		// v := f.data[idx]
		pos, err = f.readPageWithPgIdIdx(txSeq, idx)
		if err != nil {
			return
		}
		v := pos.get()
		leftSubIdx := idx*2 + 1
		rightSubIdx := idx*2 + 2
		if leftSubIdx <= maxIdx && rightSubIdx <= maxIdx {
			// leftVal := f.data[leftSubIdx]
			// rightVal := f.data[rightSubIdx]
			lPos, err = f.readPageWithPgIdIdx(txSeq, leftSubIdx)
			if err != nil {
				return
			}
			rPos, err = f.readPageWithPgIdIdx(txSeq, rightSubIdx)
			if err != nil {
				return
			}
			leftVal := lPos.get()
			rightVal := rPos.get()
			switch cmp.Compare(leftVal.ToUint64(), rightVal.ToUint64()) {
			case 0:
				// ????, 不应该出现等于的情况
				panic(fmt.Sprintf("freelist found dup pageId : %d", leftVal.ToUint64()))
			case -1:
				if cmp.Less(leftVal.ToUint64(), v.ToUint64()) {
					// f.data[leftSubIdx] = v
					// f.data[idx] = leftVal
					lPos.setPageId(v)
					pos.setPageId(leftVal)
				}
				err = f.writePage(txSeq, pos.pgId, pos.pg)
				if err != nil {
					return
				}
				err = f.writePage(txSeq, lPos.pgId, lPos.pg)
				if err != nil {
					return
				}
				idx = leftSubIdx
			case 1:
				if cmp.Less(rightVal.ToUint64(), v.ToUint64()) {
					//f.data[rightSubIdx] = v
					//f.data[idx] = rightVal
					rPos.setPageId(v)
					pos.setPageId(rightVal)
				}
				err = f.writePage(txSeq, pos.pgId, pos.pg)
				if err != nil {
					return
				}
				err = f.writePage(txSeq, rPos.pgId, rPos.pg)
				if err != nil {
					return
				}
				idx = rightSubIdx
			}
		} else if leftSubIdx <= maxIdx {
			// 只存在左节点
			// leftVal := f.data[leftSubIdx]
			lPos, err = f.readPageWithPgIdIdx(txSeq, leftSubIdx)
			if err != nil {
				return
			}
			leftVal := lPos.get()
			if cmp.Less(leftVal.ToUint64(), v.ToUint64()) {
				// f.data[leftSubIdx] = v
				// f.data[idx] = leftVal
				lPos.setPageId(v)
				pos.setPageId(leftVal)
			}
			err = f.writePage(txSeq, pos.pgId, pos.pg)
			if err != nil {
				return
			}
			err = f.writePage(txSeq, lPos.pgId, lPos.pg)
			if err != nil {
				return
			}
			idx = leftSubIdx
		} else {
			// 不存在左节点和右节点
			break
		}
	}
	return
}

func (f *freelist) pushOne(txSeq uint64, id pageId) error {
	return f.pushPageId(txSeq, id)
}

func (f *freelist) push(txSeq uint64, pageIdL []pageId) (err error) {
	for _, pageId := range pageIdL {
		err = f.pushPageId(txSeq, pageId)
		if err != nil {
			return
		}
	}
	return
}

func (f *freelist) pushPageId(txSeq uint64, pageId pageId) (err error) {
	var (
		firstPage freelistPage
		isFull    bool
	)
	firstPage, err = f.readPage(txSeq, 0)
	if err != nil {
		return
	}
	currentLength := firstPage.pgIdList[0].ToUint64()
	firstPage.pgIdList[0].FromUint64(currentLength + 1)
	isFull, err = f.isFull(currentLength)
	if err != nil {
		return
	}
	if isFull {
		err = f.growFile()
		if err != nil {
			return
		}
	}
	firstPage.pgIdList[1] = pageId
	err = f.writePage(txSeq, 0, firstPage)
	if err != nil {
		return
	}
	var (
		currentPos pageIdPos
		parentPos  pageIdPos
	)
	for {
		if currentLength == 0 {
			break
		}
		parentIndex := currentLength / 2
		parentPos, err = f.readPageWithPgIdIdx(txSeq, parentIndex)
		if err != nil {
			return
		}
		currentPos, err = f.readPageWithPgIdIdx(txSeq, currentLength)
		if err != nil {
			return
		}
		parentV := parentPos.get()
		currentV := currentPos.get()
		// 当前节点小于父节点, 交换位置
		if cmp.Less(currentV.ToUint64(), parentV.ToUint64()) {
			//f.data[parentIndex] = currentV
			//f.data[currentLength] = parentV
			parentPos.setPageId(currentV)
			currentPos.setPageId(parentV)
		}
		err = f.writePage(txSeq, parentPos.pgId, parentPos.pg)
		if err != nil {
			return
		}
		err = f.writePage(txSeq, currentPos.pgId, currentPos.pg)
		if err != nil {
			return
		}
		currentLength = parentIndex
	}
	return
}
