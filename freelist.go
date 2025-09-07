package sokv

import (
	"cmp"
	"fmt"
	"github.com/nyan233/sokv/internal/sys"
	"os"
	"unsafe"
)

var (
	defaultFreelistHeapByteSize = unsafe.Sizeof(pageId{}) * 4096
)

type freelist struct {
	mapFile *os.File
	path    string
	length  *pageId
	data    []pageId
}

func newFreelist(path string) *freelist {
	return &freelist{
		path: path,
	}
}

func (f *freelist) init() (err error) {
	f.mapFile, err = os.OpenFile(f.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return
	}
	var stat os.FileInfo
	stat, err = f.mapFile.Stat()
	if err != nil {
		return
	}
	fileSize := stat.Size()
	if fileSize == 0 {
		return f.initFile()
	} else {
		return f.mmap(uint64(fileSize), false)
	}
}

func (f *freelist) close() (err error) {
	const pageIdSize = unsafe.Sizeof(pageId{})
	err = sys.MUnmap(f.mapFile, unsafe.Slice((*byte)(unsafe.Pointer(&f.data[0])), (len(f.data)+1)*int(pageIdSize)))
	if err != nil {
		return
	}
	f.length = nil
	f.data = nil
	err = f.mapFile.Close()
	if err != nil {
		return
	}
	f.mapFile = nil
	return
}

func (f *freelist) initFile() (err error) {
	err = f.mapFile.Truncate(int64(defaultFreelistHeapByteSize))
	if err != nil {
		return
	}
	return f.mmap(uint64(defaultFreelistHeapByteSize), false)
}

func (f *freelist) mmap(size uint64, re bool) (err error) {
	const pageIdSize = unsafe.Sizeof(pageId{})
	var (
		b []byte
	)
	if re {
		b, err = sys.Remap(f.mapFile, size, unsafe.Slice((*byte)(unsafe.Pointer(f.length)), (len(f.data)+1)*int(pageIdSize)))
	} else {
		b, err = sys.MMap(f.mapFile, size)
	}
	if err != nil {
		return
	}
	// 首个elem用于计算堆的结尾
	f.length = (*pageId)(unsafe.Pointer(&b[0]))
	f.data = unsafe.Slice((*pageId)(unsafe.Pointer(&b[pageIdSize])), uintptr(len(b))/unsafe.Sizeof(pageId{})-1)
	return
}

func (f *freelist) growFile() (err error) {
	var stat os.FileInfo
	stat, err = f.mapFile.Stat()
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
	err = f.mapFile.Truncate(fileSize)
	if err != nil {
		return
	}
	return f.mmap(uint64(fileSize), true)
}

func (f *freelist) popOne() (p pageId, found bool, err error) {
	p, found = f.popPageId()
	return
}

func (f *freelist) pop(n int) ([]pageId, error) {
	res := make([]pageId, 0, n)
	for i := 0; i < n; i++ {
		p, found := f.popPageId()
		if found {
			res = append(res, p)
		} else {
			break
		}
	}
	return res, nil
}

func (f *freelist) popPageId() (p pageId, found bool) {
	currentLength := f.length.ToUint64()
	if currentLength == 0 {
		return
	} else if currentLength == 1 {
		p = f.data[0]
		found = true
		f.length.FromUint64(0)
		return
	}
	maxIdx := f.length.ToUint64() - 1
	p = f.data[0]
	found = true
	f.data[0] = f.data[maxIdx]
	f.length.FromUint64(maxIdx)
	maxIdx--
	var idx uint64
	for {
		if idx == maxIdx {
			break
		}
		v := f.data[idx]
		leftSubIdx := idx*2 + 1
		rightSubIdx := idx*2 + 2
		if leftSubIdx <= maxIdx && rightSubIdx <= maxIdx {
			leftVal := f.data[leftSubIdx]
			rightVal := f.data[rightSubIdx]
			switch cmp.Compare(leftVal.ToUint64(), rightVal.ToUint64()) {
			case 0:
				// ????, 不应该出现等于的情况
				panic(fmt.Sprintf("freelist found dup pageId : %d", leftVal.ToUint64()))
			case -1:
				if cmp.Less(leftVal.ToUint64(), v.ToUint64()) {
					f.data[leftSubIdx] = v
					f.data[idx] = leftVal
				}
				idx = leftSubIdx
			case 1:
				if cmp.Less(rightVal.ToUint64(), v.ToUint64()) {
					f.data[rightSubIdx] = v
					f.data[idx] = rightVal
				}
				idx = rightSubIdx
			}
		} else if leftSubIdx <= maxIdx {
			// 只存在左节点
			leftVal := f.data[leftSubIdx]
			if cmp.Less(leftVal.ToUint64(), v.ToUint64()) {
				f.data[leftSubIdx] = v
				f.data[idx] = leftVal
			}
			idx = leftSubIdx
		} else {
			// 不存在左节点和右节点
			break
		}
	}
	return
}

func (f *freelist) pushOne(id pageId) error {
	return f.pushPageId(id)
}

func (f *freelist) push(pageIdL []pageId) (err error) {
	for _, pageId := range pageIdL {
		err = f.pushPageId(pageId)
		if err != nil {
			return
		}
	}
	return
}

func (f *freelist) pushPageId(pageId pageId) (err error) {
	currentLength := f.length.ToUint64()
	f.length.FromUint64(currentLength + 1)
	if currentLength >= uint64(len(f.data)) {
		err = f.growFile()
		if err != nil {
			return
		}
	}
	f.data[currentLength] = pageId
	for {
		if currentLength == 0 {
			break
		}
		parentIndex := currentLength / 2
		parentV := f.data[parentIndex]
		currentV := f.data[currentLength]
		// 当前节点小于父节点, 交换位置
		if cmp.Less(currentV.ToUint64(), parentV.ToUint64()) {
			f.data[parentIndex] = currentV
			f.data[currentLength] = parentV
		}
		currentLength = parentIndex
	}
	return
}

func (f *freelist) peek() pageId {
	return f.data[0]
}

func (f *freelist) size() int {
	return int(f.length.ToUint64())
}
