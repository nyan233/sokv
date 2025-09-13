package sokv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nyan233/sokv/internal/sys"
	"hash/crc32"
	"os"
	"strconv"
	"unsafe"
)

const (
	defaultPageCount = 256
	pgIdMemSize      = unsafe.Sizeof(pageId{})
)

const (
	pageHeaderMetadata uint8 = iota + 1
	pageHeaderFreeList
	pageHeaderDat
)

var (
	medataHeader = [4]byte{'c', 'a', 'f', 'e'}
)

type metaHeader struct {
	header       [4]byte
	sum          uint32
	pageSize     uint32
	rootNodePgId pageId
	datLen       uint16
}

type metadata struct {
	rawBuf []byte
	header metaHeader
	data   []byte
}

func (m *metadata) minSize() uint32 {
	// return uint32(unsafe.Sizeof(metaHeader{}))
	return 4 + 4 + 4 + uint32(pgIdMemSize) + 2
}

func (m *metadata) checksum() uint32 {
	return crc32.ChecksumIEEE(m.rawBuf[8:])
}

func (m *metadata) parse(v []byte) {
	if len(v) < int(m.minSize()) {
		panic("value too short")
	}
	m.rawBuf = v
	m.header.header = [4]byte(v[0:4])
	m.header.sum = binary.BigEndian.Uint32(v[4:8])
	m.header.pageSize = binary.BigEndian.Uint32(v[8:12])
	m.header.rootNodePgId = pageId(v[12:18])
	m.header.datLen = binary.BigEndian.Uint16(v[18:20])
	m.data = v[20:]
}

func (m *metadata) writeHeader2RawBuf() {
	copy(m.rawBuf[0:4], m.header.header[:])
	binary.BigEndian.PutUint32(m.rawBuf[8:], m.header.pageSize)
	copy(m.rawBuf[12:18], m.header.rootNodePgId[:])
	binary.BigEndian.PutUint16(m.rawBuf[18:20], m.header.datLen)
	// update checksum
	m.header.sum = crc32.ChecksumIEEE(m.rawBuf[8:])
	binary.BigEndian.PutUint32(m.rawBuf[4:], m.header.sum)
}

type pageHeader struct {
	Header uint8
	sum    uint32
	// PgId msb ~= 2^48
	PgId  pageId
	Flags uint64
	// PgId msb ~= 2^48
	Overflow pageId
}

type pageDesc struct {
	TxSeq uint64
	// NOTE : 无论你有什么理由都不能手动操作原始缓冲区
	rawBuf []byte
	Header pageHeader
	Data   []byte
}

func (p *pageDesc) minSize() uint32 {
	// return uint32(unsafe.Sizeof(pageHeader{}))
	return uint32(1 + 4 + pgIdMemSize + 8 + pgIdMemSize)
}

// SetFlags NOTE: 注意, 如果这个页头是由mmap分配, 那么这个改动将刷新到磁盘, 将刷新到磁盘, 将刷新到磁盘, 重要的事情说三遍
func (p *pageDesc) SetFlags(flags uint64) {
	p.Header.Flags = flags
}

func (p *pageDesc) parse(v []byte) {
	if len(v) < int(p.minSize()) {
		panic("v length too short")
	}
	p.rawBuf = v
	p.Header.Header = v[0]
	p.Header.sum = binary.BigEndian.Uint32(v[1:5])
	p.Header.PgId = pageId(v[5:11])
	p.Header.Flags = binary.BigEndian.Uint64(v[11:19])
	p.Header.Overflow = pageId(v[19:25])
	p.Data = v[25:]
}

func (p *pageDesc) checksum() uint32 {
	return crc32.ChecksumIEEE(p.rawBuf[5:])
}

func (p *pageDesc) writeHeader2RawBuf() {
	p.rawBuf[0] = p.Header.Header
	copy(p.rawBuf[5:11], p.Header.PgId[:])
	binary.BigEndian.PutUint64(p.rawBuf[11:19], p.Header.Flags)
	copy(p.rawBuf[19:25], p.Header.Overflow[:])

	// update checksum
	p.Header.sum = crc32.ChecksumIEEE(p.rawBuf[5:])
	binary.BigEndian.PutUint32(p.rawBuf[1:5], p.Header.sum)
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

type pageStorageOption struct {
	DataPath             string
	FreelistPath         string
	PageSize             uint32
	MaxCacheSize         int
	FreelistMaxCacheSize int
}

type pageStorage struct {
	file     *os.File
	opt      *pageStorageOption
	freelist *freelist
	cache    *pageCache
}

func newPageStorage(opt *pageStorageOption) *pageStorage {
	// page缓存不能关闭
	if opt.MaxCacheSize == 0 {
		panic("MaxCacheSize equal zero")
	}
	if opt.FreelistMaxCacheSize == 0 {
		panic("FreelistMaxCacheSize equal zero")
	}
	return &pageStorage{
		opt:   opt,
		cache: newPageCache(opt.MaxCacheSize),
	}
}

func (m *pageStorage) getPageSize() uint32 {
	return m.opt.PageSize
}

func (m *pageStorage) init() (err error) {
	m.file, err = sys.OpenFile(m.opt.DataPath)
	if err != nil {
		return
	}
	m.opt.PageSize = uint32(sys.GetSysPageSize())
	var fileSize uint64
	stat, err := m.file.Stat()
	if err != nil {
		return
	}
	fileSize = uint64(stat.Size())
	if fileSize == 0 {
		err = m.initFile()
		return
	}
	var md metadata
	md, err = m.getMetadata(false)
	if err != nil {
		return
	}
	m.opt.PageSize = md.header.pageSize
	m.freelist = newFreelist(m.opt)
	return m.freelist.init()
}

func (m *pageStorage) initFile() (err error) {
	defaultSize := uint64(m.opt.PageSize) * defaultPageCount
	err = m.file.Truncate(int64(defaultSize))
	if err != nil {
		return err
	}
	// init metadata
	var md metadata
	md, err = m.getMetadata(true)
	if err != nil {
		return
	}
	md.header.header = medataHeader
	md.header.pageSize = m.opt.PageSize
	// init freelist
	m.freelist = newFreelist(m.opt)
	err = m.freelist.init()
	if err != nil {
		return
	}
	md.header.rootNodePgId.FromUint64(2)
	md.writeHeader2RawBuf()
	err = m.writeRawPage(createPageIdFromUint64(0), md.rawBuf)
	if err != nil {
		return
	}
	pgIdList := make([]pageId, 0, defaultPageCount-3)
	for i := 3; i < defaultPageCount; i++ {
		pgIdList = append(pgIdList, createPageIdFromUint64(uint64(i)))
	}
	return m.freelist.initPageIdList(&txHeader{seq: 0}, pgIdList)
}

func (m *pageStorage) getMetadata(isInit bool) (metadata, error) {
	var (
		md metadata
	)
	cp, found := m.cache.readPage(0)
	if found {
		md.parse(cp.data)
		return md, nil
	}
	rawPage, err := m.readRawPage(createPageIdFromUint64(0))
	if err != nil {
		return md, err
	}
	md.parse(rawPage)
	var byteLen int
	if isInit {
		byteLen = int(m.opt.PageSize)
	} else {
		byteLen = int(md.header.pageSize)
		if md.checksum() != md.header.sum {
			err = errors.New("metadata page corrupted")
			return md, err
		}
	}
	if byteLen != len(rawPage) {
		m.opt.PageSize = md.header.pageSize
		rawPage, err = m.readRawPage(createPageIdFromUint64(0))
		if err != nil {
			return md, err
		}
		md.parse(rawPage)
	}
	m.cache.setReadValue(cachePage{
		txSeq: 0,
		data:  md.rawBuf,
		pgId:  createPageIdFromUint64(0),
	})
	return md, nil
}

func (m *pageStorage) grow() (err error) {
	// 大于1GB之后每次增长1GB, 小于1GB则*2
	stat, err := m.file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	newFileSize := fileSize * 2
	if fileSize > 1024*1024*1024 {
		newFileSize = fileSize + 1024*1024*1024
	}
	err = m.file.Truncate(newFileSize)
	if err != nil {
		return err
	}
	freePageStart := fileSize / int64(m.opt.PageSize)
	freePageEnd := newFileSize / int64(m.opt.PageSize)
	for i := freePageStart; i < freePageEnd; i++ {
		err = m.freelist.pushOne(&txHeader{seq: 0}, createPageIdFromUint64(uint64(i)))
		if err != nil {
			return err
		}
	}
	return
}

func (m *pageStorage) close() (err error) {
	err = m.file.Close()
	if err != nil {
		return
	}
	m.file = nil
	return
}

func (m *pageStorage) readRootPage() (pgId pageId, err error) {
	var md metadata
	md, err = m.getMetadata(false)
	if err != nil {
		return
	} else {
		return md.header.rootNodePgId, nil
	}
}

func (m *pageStorage) setRootPage(txh *txHeader, pgId pageId) error {
	return m.setRootPageWithPageId(txh, pgId)
}

func (m *pageStorage) setRootPageWithPageId(txh *txHeader, rootPgId pageId) error {
	if txh.seq == 0 {
		return fmt.Errorf("invalid tx seq: %d", txh.seq)
	}
	md, err := m.getMetadata(false)
	if err != nil {
		return err
	}
	pgId := createPageIdFromUint64(0)
	md.header.rootNodePgId = rootPgId
	md.writeHeader2RawBuf()
	m.cache.setDirtyPage(cachePage{
		txSeq: txh.seq,
		data:  md.rawBuf,
		pgId:  pgId,
	})
	return nil
}

// NOTE: 不允许在事务中执行, 会直接写原始页, 一般只用于初始化时设置配置
func (m *pageStorage) setLocalData(b []byte) error {
	md, err := m.getMetadata(false)
	if err != nil {
		return err
	}
	maxSize := int(m.opt.PageSize - md.minSize())
	if len(b) > maxSize {
		return fmt.Errorf("data too large : len(b) == %d > maxSize(%d)", len(b), maxSize)
	}
	md.header.datLen = uint16(len(b))
	copy(md.data, b)
	md.writeHeader2RawBuf()
	pgId := createPageIdFromUint64(0)
	m.cache.setDirtyPage(cachePage{
		txSeq: 0,
		data:  md.rawBuf,
		pgId:  pgId,
	})
	return m.writeRawPage(pgId, md.rawBuf)
}

func (m *pageStorage) loadLocalData() ([]byte, error) {
	md, err := m.getMetadata(false)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, md.header.datLen)
	copy(buf, md.data[:md.header.datLen])
	return buf, nil
}

func (m *pageStorage) writePage(txh *txHeader, pd pageDesc) error {
	if txh.seq == 0 {
		return fmt.Errorf("invalid tx seq: %d", pd.TxSeq)
	}
	pd.writeHeader2RawBuf()
	cpDat := make([]byte, pd.minSize())
	copy(cpDat, pd.rawBuf)
	// 记录一下头的变化
	err := txh.addPageModify(pageRecord{
		typ:  pageRecordStorage,
		pgId: pd.Header.PgId,
		off:  0,
		dat:  cpDat,
	})
	if err != nil {
		return err
	}
	m.cache.setDirtyPage(cachePage{
		txSeq: txh.seq,
		data:  pd.rawBuf,
		pgId:  pd.Header.PgId,
	})
	return nil
}

func (m *pageStorage) allocPage(txh *txHeader, n int) (res []pageId, err error) {
	res = make([]pageId, 0, n)
	res, err = m.freelist.pop(txh, n)
	if err != nil {
		return
	}
	if len(res) < n {
		err = m.grow()
		if err != nil {
			return nil, err
		}
		var pageIdList2 []pageId
		pageIdList2, err = m.freelist.pop(txh, n-len(res))
		if err != nil {
			return
		}
		res = append(res, pageIdList2...)
	}
	return res, nil
}

func (m *pageStorage) freePage(txh *txHeader, pgIdList []pageId) error {
	for _, pgId := range pgIdList {
		if pgId.ToUint64() < 2 {
			return fmt.Errorf("free page type not is dat")
		}
	}
	return m.freelist.push(txh, pgIdList)
}

func (m *pageStorage) readRawPage(pgId pageId) ([]byte, error) {
	buf := make([]byte, m.opt.PageSize)
	readCount, err := m.file.ReadAt(buf, int64(pgId.ToUint64()*uint64(m.opt.PageSize)))
	if err != nil {
		return nil, err
	}
	if readCount != int(m.opt.PageSize) {
		return nil, fmt.Errorf("read %d bytes instead of expected %d", readCount, m.opt.PageSize)
	}
	return buf, nil
}

func (m *pageStorage) readPage(pgId pageId) (pd pageDesc, err error) {
	if pgId.ToUint64() < 2 {
		err = fmt.Errorf("read page type not is dat : %d", pgId.ToUint64())
		return
	}
	cp, found := m.cache.readPage(pgId.ToUint64())
	if found {
		pd.parse(cp.data)
	} else {
		var buf []byte
		buf, err = m.readRawPage(pgId)
		if err != nil {
			return
		}
		pd.parse(buf)
	}
	if !bytesIsZero(pd.rawBuf) {
		if pd.checksum() != pd.Header.sum {
			err = errors.New("page data corrupted")
			return
		}
	}
	if !found {
		m.cache.setReadValue(cachePage{
			txSeq: 0,
			data:  pd.rawBuf,
			pgId:  pgId,
		})
	}
	return
}

func (m *pageStorage) writeRawPage(pgId pageId, buf []byte) error {
	writeCount, err := m.file.WriteAt(buf, int64(pgId.ToUint64()*uint64(m.opt.PageSize)))
	if err != nil {
		return err
	}
	if writeCount != len(buf) {
		return fmt.Errorf("write %d bytes instead of expected %d", writeCount, len(buf))
	} else {
		return nil
	}
}

func (m *pageStorage) deleteAllDirtyPage() {
	m.cache.delAllDirtyPage()
	m.freelist.cache.delAllDirtyPage()
}
