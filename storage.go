package sokv

import (
	"fmt"
	"github.com/nyan233/sokv/internal/sys"
	"hash/crc32"
	"os"
	"unsafe"
)

var (
	medataHeader = [4]byte{'c', 'a', 'f', 'e'}
)

type metaHeader struct {
	header       [4]byte
	sum          uint32
	sysPageSize  uint32
	rootNodePgId pageId
	datLen       uint16
}

type mmapPsMetadata struct {
	header *metaHeader
	data   []byte
}

func (m *mmapPsMetadata) minSize() uint32 {
	return uint32(unsafe.Sizeof(metaHeader{}))
}

type mmapPsFreelist struct {
	header   uint8
	pgId     pageId
	count    uint16
	overflow pageId
	freelist []pageId
}

func (p *mmapPsFreelist) minSize() uint32 {
	return uint32(unsafe.Sizeof(mmapPsFreelist{})) - 24
}

type mmapPageStorage struct {
	mapFile     *os.File
	path        string
	dat         []byte
	sysPageSize uint32
	freelist    *freelist
}

func newMMapPageStorage(path string) *mmapPageStorage {
	return &mmapPageStorage{
		path: path,
	}
}

func (m *mmapPageStorage) getPageSize() uint32 {
	return m.sysPageSize
}

func (m *mmapPageStorage) init() (err error) {
	m.mapFile, err = os.OpenFile(m.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return
	}
	m.sysPageSize = uint32(sys.GetSysPageSize())
	var fileSize uint64
	stat, err := m.mapFile.Stat()
	if err != nil {
		return
	}
	fileSize = uint64(stat.Size())
	if fileSize == 0 {
		err = m.initFile()
		return
	}
	m.dat, err = sys.MMap(m.mapFile, fileSize)
	metadata := m.getMetadata(false)
	m.sysPageSize = metadata.header.sysPageSize
	m.freelist = newFreelist(m.path + ".freelist")
	return m.freelist.init()
}

func (m *mmapPageStorage) initFile() (err error) {
	defaultSize := uint64(m.sysPageSize) * defaultPageCount
	err = m.mapFile.Truncate(int64(defaultSize))
	if err != nil {
		return err
	}
	m.dat, err = sys.MMap(m.mapFile, defaultSize)
	if err != nil {
		return
	}
	// init metadata
	metadata := m.getMetadata(true)
	metadata.header.header = medataHeader
	metadata.header.sysPageSize = m.sysPageSize
	// init freelist
	m.freelist = newFreelist(m.path + ".freelist")
	err = m.freelist.init()
	if err != nil {
		return
	}
	metadata.header.rootNodePgId.FromUint64(2)
	metadata.header.sum = crc32.ChecksumIEEE(unsafe.Slice((*byte)(unsafe.Pointer(&m.dat[0])), metadata.header.sysPageSize))
	for i := 3; i < defaultPageCount; i++ {
		err = m.freelist.pushOne(createPageIdFromUint64(uint64(i)))
		if err != nil {
			return
		}
	}
	var rootPage *pageDesc
	rootPage, err = m.readPage(createPageIdFromUint64(2))
	if err != nil {
		return
	}
	rootPage.Header.Header = PageHeaderDat
	rootPage.Header.PgId = createPageIdFromUint64(2)
	return
}

func (m *mmapPageStorage) getMetadata(isInit bool) *mmapPsMetadata {
	ptr := unsafe.Pointer(&m.dat[0])
	metadata := &mmapPsMetadata{
		header: (*metaHeader)(ptr),
	}
	var byteLen int
	if isInit {
		byteLen = int(m.sysPageSize)
	} else {
		byteLen = int(metadata.header.sysPageSize)
	}
	metadata.data = unsafe.Slice((*byte)(&m.dat[metadata.minSize()]), byteLen)
	return metadata
}

func (m *mmapPageStorage) grow() (err error) {
	// 大于1GB之后每次增长1GB, 小于1GB则*2
	stat, err := m.mapFile.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	newFileSize := fileSize * 2
	if fileSize > 1024*1024*1024 {
		newFileSize = fileSize + 1024*1024*1024
	}
	err = m.mapFile.Truncate(newFileSize)
	if err != nil {
		return err
	}
	m.dat, err = sys.Remap(m.mapFile, uint64(newFileSize), m.dat)
	if err != nil {
		return err
	}
	freePageStart := fileSize / int64(m.sysPageSize)
	freePageEnd := newFileSize / int64(m.sysPageSize)
	for i := freePageStart; i < freePageEnd; i++ {
		err = m.freelist.pushOne(createPageIdFromUint64(uint64(i)))
		if err != nil {
			return err
		}
	}
	return
}

func (m *mmapPageStorage) close() (err error) {
	err = m.mapFile.Close()
	if err != nil {
		return
	}
	m.mapFile = nil
	m.dat = nil
	return
}

func (m *mmapPageStorage) readRootPage() (*pageDesc, error) {
	metadata := m.getMetadata(false)
	return m.readPage(metadata.header.rootNodePgId)
}

func (m *mmapPageStorage) setRootPage(desc *pageDesc) error {
	metadata := m.getMetadata(false)
	metadata.header.rootNodePgId = desc.Header.PgId
	// sum只计算Header和sum字段后面的值
	metadata.header.sum = crc32.ChecksumIEEE(unsafe.Slice((*byte)(unsafe.Pointer(&m.dat[8])), metadata.header.sysPageSize-8))
	return nil
}

func (m *mmapPageStorage) setRootPageWithPageId(pgId pageId) error {
	metadata := m.getMetadata(false)
	metadata.header.rootNodePgId = pgId
	// sum只计算Header和sum字段后面的值
	metadata.header.sum = crc32.ChecksumIEEE(unsafe.Slice((*byte)(unsafe.Pointer(&m.dat[8])), metadata.header.sysPageSize-8))
	return nil
}

func (m *mmapPageStorage) setLocalData(b []byte) error {
	metadata := m.getMetadata(false)
	maxSize := int(m.sysPageSize - metadata.minSize())
	if len(b) > maxSize {
		return fmt.Errorf("data too large : len(b) == %d > maxSize(%d)", len(b), maxSize)
	}
	metadata.header.datLen = uint16(len(b))
	copy(metadata.data, b)
	// sum只计算Header和sum字段后面的值
	metadata.header.sum = crc32.ChecksumIEEE(unsafe.Slice((*byte)(unsafe.Pointer(&m.dat[8])), metadata.header.sysPageSize-8))
	return nil
}

func (m *mmapPageStorage) loadLocalData() ([]byte, error) {
	metadata := m.getMetadata(false)
	buf := make([]byte, metadata.header.datLen)
	copy(buf, metadata.data[:metadata.header.datLen])
	return buf, nil
}

func (m *mmapPageStorage) allocPage(n int) (res []pageId, err error) {
	res = make([]pageId, 0, n)
	res, err = m.freelist.pop(n)
	if err != nil {
		return
	}
	if len(res) < n {
		err = m.grow()
		if err != nil {
			return nil, err
		}
		var pageIdList2 []pageId
		pageIdList2, err = m.freelist.pop(n - len(res))
		if err != nil {
			return
		}
		res = append(res, pageIdList2...)
	}
	return res, m.setDatPageHeader(res)
}

func (m *mmapPageStorage) setDatPageHeader(res []pageId) error {
	for _, v := range res {
		header := (*pageHeader)(unsafe.Pointer(&m.dat[v.ToUint64()*uint64(m.sysPageSize)]))
		header.Header = PageHeaderDat
		header.PgId = v
	}
	return nil
}

func (m *mmapPageStorage) freePage(pgIdList []pageId) error {
	for _, pgId := range pgIdList {
		if pgId.ToUint64() < 2 {
			return fmt.Errorf("free page type not is dat")
		}
	}
	return m.freelist.push(pgIdList)
}

func (m *mmapPageStorage) readPage(pgId pageId) (pd *pageDesc, err error) {
	if pgId.ToUint64() < 2 {
		return nil, fmt.Errorf("read page type not is dat : %d", pgId.ToUint64())
	}
	pd = new(pageDesc)
	pd.Header = (*pageHeader)(unsafe.Pointer(&m.dat[pgId.ToUint64()*uint64(m.sysPageSize)]))
	pd.Data = unsafe.Slice((*byte)(unsafe.Pointer(&m.dat[pgId.ToUint64()*uint64(m.sysPageSize)+uint64(pd.minSize())])), m.sysPageSize-pd.minSize())
	return
}

func (m *mmapPageStorage) cleanPage(pd *pageDesc) error {
	pgId := pd.Header.PgId
	pgRawBytes := unsafe.Slice((*byte)(unsafe.Pointer(&m.dat[pgId.ToUint64()*uint64(m.sysPageSize)])), m.sysPageSize)
	clear(pgRawBytes)
	return nil
}

func (m *mmapPageStorage) writePage(desc *pageDesc) error {
	//TODO implement me
	panic("implement me")
}

func (m *mmapPageStorage) rangePageByPageId(u uint64, f func(desc *pageDesc) error) error {
	//TODO implement me
	panic("implement me")
}

func (m *mmapPageStorage) RangePageByPageDesc(desc *pageDesc, f func(desc *pageDesc) error) error {
	//TODO implement me
	panic("implement me")
}

func (m *mmapPageStorage) writeAll(pds []*pageDesc) error {
	//TODO implement me
	panic("implement me")
}
