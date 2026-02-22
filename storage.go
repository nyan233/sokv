package sokv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nyan233/sokv/internal/sys"
	"hash/crc32"
	"log/slog"
	"os"
	"unsafe"
)

const (
	defaultPageCount = 256
	pgIdMemSize      = unsafe.Sizeof(uint32(0))
	// 8KB
	recordSize = 8 * 1024
)

const (
	pageHeaderMetadata uint8 = iota + 1
	pageHeaderFreeList
	pageHeaderDat
)

var (
	medataHeader = [4]byte{'s', 'o', 'k', 'v'}
)

func init() {
	sysPageSize := sys.GetSysPageSize()
	if recordSize%sysPageSize != 0 {
		panic(fmt.Sprintf("recordSize(%d) must be a multiple of sysPageSize(%d)", recordSize, sysPageSize))
	}
}

type metaHeader struct {
	header       [4]byte
	sum          uint32
	rootNodePgId uint32
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
	// m.header.header = [4]byte(v[0:4])
	m.header.header = popBytes(&v, 4, func(a0 []byte) [4]byte {
		return [4]byte(v)
	})
	// m.header.sum = binary.BigEndian.Uint32(v[4:8])
	m.header.sum = popBytes(&v, 4, binary.BigEndian.Uint32)
	m.header.rootNodePgId = popBytes(&v, 4, binary.BigEndian.Uint32)
	m.header.datLen = popBytes(&v, 2, binary.BigEndian.Uint16)
	m.data = v
}

func (m *metadata) writeHeader2RawBuf() {
	copy(m.rawBuf[0:4], m.header.header[:])
	binary.BigEndian.PutUint32(m.rawBuf[8:12], m.header.rootNodePgId)
	binary.BigEndian.PutUint16(m.rawBuf[12:14], m.header.datLen)
	// update checksum
	m.header.sum = crc32.ChecksumIEEE(m.rawBuf[8:])
	binary.BigEndian.PutUint32(m.rawBuf[4:], m.header.sum)
}

type pageHeader struct {
	Header   uint8
	sum      uint32
	PgId     uint32
	Flags    uint64
	Overflow uint32
}

type pageDesc struct {
	TxSeq uint64
	// NOTE : 无论你有什么理由都不能手动操作原始缓冲区
	rawBuf []byte
	Header pageHeader
	Data   []byte
}

func (p *pageDesc) id() uint32 {
	return p.Header.PgId
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
	// v[0]
	p.Header.Header = popBytes(&v, 1, func(a0 []byte) uint8 {
		return a0[0]
	})
	// p.Header.sum = binary.BigEndian.Uint32(v[1:5])
	p.Header.sum = popBytes(&v, 4, binary.BigEndian.Uint32)
	p.Header.PgId = popBytes(&v, 4, binary.BigEndian.Uint32)
	p.Header.Flags = popBytes(&v, 8, binary.BigEndian.Uint64)
	p.Header.Overflow = popBytes(&v, 16, binary.BigEndian.Uint32)
	p.Data = v
}

func (p *pageDesc) checksum() uint32 {
	return crc32.ChecksumIEEE(p.rawBuf[5:])
}

func (p *pageDesc) writeHeader2RawBuf() {
	p.rawBuf[0] = p.Header.Header
	binary.BigEndian.PutUint32(p.rawBuf[5:9], p.Header.PgId)
	binary.BigEndian.PutUint64(p.rawBuf[9:17], p.Header.Flags)
	binary.BigEndian.PutUint32(p.rawBuf[17:21], p.Header.Overflow)

	// update checksum
	p.Header.sum = crc32.ChecksumIEEE(p.rawBuf[5:])
	binary.BigEndian.PutUint32(p.rawBuf[1:5], p.Header.sum)
}

type pageStorageOption struct {
	DataPath             string
	FreelistPath         string
	MaxCacheSize         int
	FreelistMaxCacheSize int
	PageCipher           Cipher
	Logger               *slog.Logger
}

type pageStorage struct {
	file   *os.File
	opt    *pageStorageOption
	cache  pageCacheI
	cipher Cipher
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
		opt:    opt,
		cache:  newLFUCache(opt.MaxCacheSize),
		cipher: opt.PageCipher,
	}
}

func (m *pageStorage) init() (err error) {
	m.file, err = sys.OpenFile(m.opt.DataPath)
	if err != nil {
		return
	}
	var fileSize uint64
	stat, err := m.file.Stat()
	if err != nil {
		return
	}
	fileSize = uint64(stat.Size())
	if fileSize == 0 {
		err = m.initFile()
		return
	} else {
		return nil
	}
}

func (m *pageStorage) initFile() (err error) {
	defaultSize := uint64(recordSize) * defaultPageCount
	err = m.file.Truncate(int64(defaultSize))
	if err != nil {
		return err
	}
	// init metadata
	var md metadata
	md, err = m.getMetadata(nil, true)
	if err != nil {
		return
	}
	md.header.header = medataHeader
	md.header.rootNodePgId = 2
	md.writeHeader2RawBuf()
	err = m.writeRawPage(0, md.rawBuf)
	if err != nil {
		return
	}
	if m.cipher != nil {
		zeroBuf := make([]byte, recordSize)
		err = m.writeRawPage(1, zeroBuf)
		if err != nil {
			return
		}
		err = m.writeRawPage(2, zeroBuf)
		if err != nil {
			return
		}
	}
	return nil
}

func (m *pageStorage) getMetadata(txh *txHeader, isInit bool) (metadata, error) {
	var md metadata
	cp, found := m.cache.getPage(txh, 0)
	if found {
		md.parse(cp.data)
		return md, nil
	}
	rawPage, err := m.readRawPage(0)
	if err != nil {
		return md, err
	}
	md.parse(rawPage)
	if !isInit {
		if md.checksum() != md.header.sum {
			err = errors.New("metadata page corrupted")
			return md, err
		}
	}
	m.cache.putPage(txh, 0, cachePage{
		txSeq: 0,
		data:  md.rawBuf,
		pgId:  0,
	})
	return md, nil
}

func (m *pageStorage) grow(txh *txHeader) (pageIdList []uint32, err error) {
	// 大于1GB之后每次增长1GB, 小于1GB则*2
	stat, err := m.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	newFileSize := fileSize * 2
	if fileSize > 1024*1024*1024 {
		newFileSize = fileSize + 1024*1024*1024
	}
	err = m.file.Truncate(newFileSize)
	if err != nil {
		return nil, err
	}
	freePageStart := fileSize / int64(recordSize)
	freePageEnd := newFileSize / int64(recordSize)
	pageCount := uint64((newFileSize - fileSize) / int64(recordSize))
	pageIdList = make([]uint32, 0, pageCount)
	for i := freePageStart; i < freePageEnd; i++ {
		pageIdList = append(pageIdList, uint32(i))
	}
	return
}

func (m *pageStorage) close() (err error) {
	err = m.file.Close()
	if err != nil {
		return
	}
	m.file = nil
	m.cache = nil
	return
}

func (m *pageStorage) readRootPage(txh *txHeader) (pgId uint32, err error) {
	var md metadata
	md, err = m.getMetadata(txh, false)
	if err != nil {
		return
	} else {
		return md.header.rootNodePgId, nil
	}
}

func (m *pageStorage) setRootPage(txh *txHeader, pgId uint32) error {
	return m.setRootPageWithPageId(txh, pgId)
}

func (m *pageStorage) setRootPageWithPageId(txh *txHeader, rootPgId uint32) error {
	if txh.isWriteTx() {
		return fmt.Errorf("current tx type not is write")
	}
	md, err := m.getMetadata(txh, false)
	if err != nil {
		return err
	}
	m.cache.opShadowPage(txh, cachePage{
		txSeq: txh.seq,
		data:  md.rawBuf,
		pgId:  0,
	}, func(p cachePage) (cachePage, bool) {
		md.header.rootNodePgId = rootPgId
		md.writeHeader2RawBuf()
		err = txh.addPageModify(pageRecord{
			typ: pageRecordStorage,
			// metadata pageId
			pgId: 0,
			off:  0,
			dat:  md.rawBuf[:md.minSize()],
		})
		return p, err == nil
	})
	return nil
}

// NOTE: 不允许在事务中执行, 会直接写原始页, 一般只用于初始化时设置配置
func (m *pageStorage) setLocalData(b []byte) error {
	md, err := m.getMetadata(nil, false)
	if err != nil {
		return err
	}
	maxSize := int(recordSize - md.minSize())
	if len(b) > maxSize {
		return fmt.Errorf("data too large : len(b) == %d > maxSize(%d)", len(b), maxSize)
	}
	md.header.datLen = uint16(len(b))
	copy(md.data, b)
	md.writeHeader2RawBuf()
	m.cache.putPage(nil, 0, cachePage{
		txSeq: 0,
		data:  md.rawBuf,
		pgId:  0,
	})
	return m.writeRawPage(0, md.rawBuf)
}

func (m *pageStorage) loadLocalData() ([]byte, error) {
	md, err := m.getMetadata(nil, false)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, md.header.datLen)
	copy(buf, md.data[:md.header.datLen])
	return buf, nil
}

func (m *pageStorage) writePage(txh *txHeader, pd pageDesc) error {
	if !txh.isWriteTx() {
		return fmt.Errorf("current tx type not is write")
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
	m.cache.addShadowPages(txh, []cachePage{
		{
			txSeq: txh.seq,
			data:  pd.rawBuf,
			pgId:  pd.Header.PgId,
		},
	})
	return nil
}

func (m *pageStorage) flushPageHeader(txh *txHeader, pd *pageDesc) error {
	if !txh.isWriteTx() {
		return fmt.Errorf("current tx type not is write")
	}
	pd.writeHeader2RawBuf()
	cpDat := make([]byte, pd.minSize())
	copy(cpDat, pd.rawBuf)
	// 记录一下头的变化
	return txh.addPageModify(pageRecord{
		typ:  pageRecordStorage,
		pgId: pd.Header.PgId,
		off:  0,
		dat:  cpDat,
	})
}

func (m *pageStorage) flushShadowPage2Disk(txh *txHeader) error {
	changeList := txh.getChangeList(pageRecordStorage)
	if len(changeList) == 0 {
		return nil
	}
	for _, pgId := range changeList {
		p, ok := m.cache.getShadowPageChange(txh, pgId)
		if !ok {
			return fmt.Errorf("tx have page change, but no shadow page : %d", pgId)
		}
		err := m.writeRawPage(pgId, p.data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *pageStorage) modifyPage(txh *txHeader, pd pageDesc, fn func(pd pageDesc) (pageDesc, error)) (err error) {
	if !txh.isWriteTx() {
		return fmt.Errorf("current tx type not is write")
	}
	m.cache.opShadowPage(txh, cachePage{
		txSeq: txh.seq,
		data:  pd.rawBuf,
		pgId:  pd.id(),
	}, func(p cachePage) (cachePage, bool) {
		var modifyPd pageDesc
		modifyPd, err = fn(pd)
		if err != nil {
			return p, false
		}
		modifyPd.writeHeader2RawBuf()
		cpDat := make([]byte, modifyPd.minSize())
		copy(cpDat, modifyPd.rawBuf)
		// 记录一下头的变化
		err = txh.addPageModify(pageRecord{
			typ:  pageRecordStorage,
			pgId: modifyPd.Header.PgId,
			off:  0,
			dat:  cpDat,
		})
		if err != nil {
			return p, false
		}
		p.data = modifyPd.rawBuf
		return p, true
	})
	return
}

func (m *pageStorage) readRawPage(pgId uint32) ([]byte, error) {
	buf := make([]byte, recordSize)
	readCount, err := m.file.ReadAt(buf, int64(pgId*recordSize))
	if err != nil {
		return nil, err
	}
	if readCount != recordSize {
		return nil, fmt.Errorf("read %d bytes instead of expected %d", readCount, readCount)
	}
	if m.cipher != nil && !bytesIsZero(buf) {
		err = m.cipher.Decrypt(buf)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func (m *pageStorage) readPage(txh *txHeader, pgId uint32) (pd pageDesc, err error) {
	if pgId < 2 {
		err = fmt.Errorf("read page type not is dat : %d", pgId)
		return
	}
	cp, found := m.cache.getPage(txh, pgId)
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
		m.cache.putPage(txh, pgId, cachePage{
			txSeq: 0,
			data:  pd.rawBuf,
			pgId:  pgId,
		})
	}
	return
}

func (m *pageStorage) writeRawPage(pgId uint32, buf []byte) (err error) {
	if m.cipher != nil {
		buf, err = m.cipher.Encrypt(buf)
		if err != nil {
			return
		}
		defer m.cipher.free(buf)
	}
	writeCount, err := m.file.WriteAt(buf, int64(pgId*recordSize))
	if err != nil {
		return err
	}
	if writeCount != len(buf) {
		return fmt.Errorf("write %d bytes instead of expected %d", writeCount, len(buf))
	} else {
		return nil
	}
}
