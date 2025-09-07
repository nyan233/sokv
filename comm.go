package sokv

import (
	"strconv"
	"unsafe"
)

const (
	defaultPageCount = 256 * 1024 * 16
)

const (
	PageHeaderMetadata uint8 = iota + 1
	PageHeaderFreeList
	PageHeaderDat
)

type pageStorageBaseI interface {
	Init() error
	Close() error
	// AllocPage pageId msb ~= 2^48
	AllocPage(n int) ([]uint64, error)
	FreePage([]uint64) error
	ReadPage(uint64) (*pageDesc, error)
	WritePage(*pageDesc) error
	RangePageByPageId(uint64, func(desc *pageDesc) error) error
	RangePageByPageDesc(*pageDesc, func(desc *pageDesc) error) error
	// WriteAll contain Overflow page
	WriteAll([]*pageDesc) error
}

type pageHeader struct {
	Header uint8
	// PgId msb ~= 2^48
	PgId  pageId
	sum   uint32
	Flags uint64
	// PgId msb ~= 2^48
	Overflow pageId
}

type pageDesc struct {
	TxSeq uint64
	// NOTE : 无论你有什么理由都不能操作原始缓冲区, 应该由pageCache来操作
	rawBuf []byte
	Header pageHeader
	Data   []byte
}

func (p *pageDesc) minSize() uint32 {
	return uint32(unsafe.Sizeof(pageHeader{}))
}

// SetFlags NOTE: 注意, 如果这个页头是由mmap分配, 那么这个改动将刷新到磁盘, 将刷新到磁盘, 将刷新到磁盘, 重要的事情说三遍
func (p *pageDesc) SetFlags(flags uint64) {
	p.Header.Flags = flags
}

type pageId [6]byte

func (p *pageId) ToUint64() uint64 {
	return uint64(p[0])<<40 | uint64(p[1])<<32 | uint64(p[2])<<24 | uint64(p[3])<<16 | uint64(p[4])<<8 | uint64(p[5])
}

func (p *pageId) FromUint64(v uint64) {
	p[0] = byte(v >> 40)
	p[1] = byte(v >> 32)
	p[2] = byte(v >> 24)
	p[3] = byte(v >> 16)
	p[4] = byte(v >> 8)
	p[5] = byte(v)
}

func (p *pageId) String() string {
	return strconv.FormatUint(p.ToUint64(), 10)
}

func createPageIdFromUint64(v uint64) (p pageId) {
	p.FromUint64(v)
	return
}
