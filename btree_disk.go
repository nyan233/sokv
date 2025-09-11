package sokv

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/nyan233/sokv/internal/sys"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	treeMaxM = 256
)

type stackElement struct {
	node *nodeDiskDesc
	tag  uint64
}

type stack struct {
	list []stackElement
}

func (s *stack) push(e stackElement) {
	s.list = append(s.list, e)
}

func (s *stack) pop() stackElement {
	if len(s.list) == 0 {
		return stackElement{
			node: nil,
		}
	}
	v := s.list[len(s.list)-1]
	s.list = s.list[:len(s.list)-1]
	return v
}

func (s *stack) peek() stackElement {
	if len(s.list) == 0 {
		return stackElement{}
	} else {
		return s.list[len(s.list)-1]
	}
}

type keywordDiskDesc struct {
	keyLen uint32
	valLen uint32
}

type keywordDisk struct {
	key []byte
	val []byte
}

type nodeDiskDesc struct {
	pd       pageDesc
	pageLink []pageDesc
	subNodes []pageId
	// 布局的描述, 不在磁盘上存储
	keywordsPageView []keywordDiskDesc
	// 在内存中的keywords, 修改后需要将其刷到磁盘中
	memKeywords []keywordDisk
}

func (node *nodeDiskDesc) sumKeywordDiskOff(idx int) (off uint32) {
	for i := 0; i < idx; i++ {
		off += 8
		off += node.keywordsPageView[idx].keyLen + node.keywordsPageView[idx].valLen
	}
	return
}

func (node *nodeDiskDesc) isLeaf() bool {
	return node.subNodes[0].ToUint64() == 0
}

func (node *nodeDiskDesc) effectiveSubNodes() []pageId {
	res := make([]pageId, 0, len(node.subNodes))
	for _, sub := range node.subNodes {
		if sub.ToUint64() > 0 {
			res = append(res, sub)
		}
	}
	return res
}

func (node *nodeDiskDesc) subNodeSize() (s uint32) {
	for _, sub := range node.subNodes {
		if sub.ToUint64() > 0 {
			s++
		} else {
			break
		}
	}
	return
}

func (node *nodeDiskDesc) delLastKeyword() keywordDisk {
	v := node.memKeywords[len(node.memKeywords)-1]
	node.memKeywords = node.memKeywords[:len(node.memKeywords)-1]
	return v
}

func (node *nodeDiskDesc) delFirstKeyword() keywordDisk {
	v := node.memKeywords[0]
	node.memKeywords = node.memKeywords[1:]
	return v
}

type diskLocalData struct {
	M int `json:"m"`
}

type BTreeDisk[K any, V any] struct {
	rw            sync.RWMutex
	keyCodec      Codec[K]
	valCodec      Codec[V]
	size          atomic.Uint64
	s             *pageStorage
	c             Config
	recordLogFile *os.File
	txSeq         atomic.Uint64
	logger        slog.Logger
}

type Config struct {
	RootDir                  string
	Name                     string
	TreeM                    int
	MaxPageCacheSize         int
	MaxFreeListPageCacheSize int
}

func NewBTreeDisk[K any, V any](c Config) *BTreeDisk[K, V] {
	return &BTreeDisk[K, V]{
		c: c,
	}
}

func (bt *BTreeDisk[K, V]) SetKeyCodec(keyCodec Codec[K]) {
	bt.keyCodec = keyCodec
}

func (bt *BTreeDisk[K, V]) SetValCodec(valCodec Codec[V]) {
	bt.valCodec = valCodec
}

func (bt *BTreeDisk[K, V]) getFilePath(suffix string) string {
	return filepath.Join(bt.c.RootDir, bt.c.Name+suffix)
}

func (bt *BTreeDisk[K, V]) Init() error {
	if bt.c.TreeM > treeMaxM {
		return fmt.Errorf("bt.m > treeMaxM(%d)", treeMaxM)
	}
	bt.s = newPageStorage(bt.getFilePath(".dat"), bt.getFilePath(".freelist"), uint32(sys.GetSysPageSize()))
	err := bt.s.init()
	if err != nil {
		return err
	}
	pgSize := bt.s.getPageSize()
	slotSize := bt.c.TreeM * int(unsafe.Sizeof(pageId{})+8)
	// 节点分槽占用的空间比一个页还要大
	if slotSize > int(pgSize) {
		return fmt.Errorf("slotSize(%d) > pgSize(%d)", slotSize, pgSize)
	}
	localDataBytes, err := bt.s.loadLocalData()
	if err != nil {
		return err
	}
	localData := &diskLocalData{
		M: bt.c.TreeM,
	}
	if len(localDataBytes) > 0 {
		err = json.Unmarshal(localDataBytes, localData)
		if err != nil {
			return err
		}
		bt.c.TreeM = localData.M
	} else {
		localDataBytes, err = json.Marshal(localData)
		if err != nil {
			return err
		}
		err = bt.s.setLocalData(localDataBytes)
		if err != nil {
			return err
		}
	}
	bt.recordLogFile, err = os.OpenFile(bt.getFilePath(".record"), os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	stat, err := bt.recordLogFile.Stat()
	if err != nil {
		return err
	}
	if stat.Size() > 0 {
		bt.logger.Info("record found data, try crashRecovery", "fileSize", stat.Size())
		recordLogFile, err := os.OpenFile(bt.getFilePath(".record"), os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		return bt.crashRecovery(recordLogFile)
	}
	return nil
}

func (bt *BTreeDisk[K, V]) crashRecovery(recordLog *os.File) error {
	var (
		txh = &txHeader{
			records:                 make([]pageRecord, 0, 16),
			storagePageChangeRecord: make(map[uint64][]pageRecord, 16),
			freePageChangeRecord:    make(map[uint64][]pageRecord, 16),
		}
	)
	buf, err := io.ReadAll(recordLog)
	if err != nil {
		return err
	}
	if binary.BigEndian.Uint64(buf[0:8]) != recordStart {
		bt.logger.Warn("crashRecovery record data corrupted", "start", fmt.Sprintf("%v", buf[0:8]))
		return recordLog.Truncate(0)
	}
	if binary.BigEndian.Uint64(buf[len(buf)-8:]) != recordEnd {
		bt.logger.Warn("crashRecovery record data corrupted", "end", fmt.Sprintf("%v", buf[len(buf)-8:]))
	}
	buf = buf[:len(buf)-8]
	buf = buf[:8]
	for len(buf) > 0 {
		var (
			record pageRecord
			header pageRecordHeader
		)
		err = binary.Read(bytes.NewReader(buf), binary.BigEndian, &header)
		if err != nil {
			bt.logger.Warn("crashRecovery record data corrupted", "err", err)
			return recordLog.Truncate(0)
		}
		buf = buf[8:]
		if int(header.length) > len(buf) {
			bt.logger.Warn("crashRecovery page record data corrupted", "length", int(header.length))
			return recordLog.Truncate(0)
		}
		pageRecordChecksum := crc32.ChecksumIEEE(buf[:header.length])
		if header.checksum != pageRecordChecksum {
			bt.logger.Warn("crashRecovery record data corrupted, checksum mismatch", "checksum", header.checksum, "pageRecordChecksum", pageRecordChecksum)
			return recordLog.Truncate(0)
		}
		record.typ = buf[0]
		buf = buf[1:]
		record.pgId = pageId(buf[:6])
		buf = buf[6:]
		record.txSeq = binary.BigEndian.Uint64(buf[0:8])
		buf = buf[8:]
		record.off = binary.BigEndian.Uint32(buf[:4])
		buf = buf[4:]
		record.dat = buf[:header.length-record.minSize()]
		if txh.seq == 0 {
			txh.seq = record.txSeq
		}
		err = txh.addPageModify(record)
		if err != nil {
			panic(err)
		}
	}
	return bt.doWritePageData(txh)
}

func (bt *BTreeDisk[K, V]) doWritePageData(txh *txHeader) error {
	for pgId, changeList := range txh.storagePageChangeRecord {
		rawPage, err := bt.s.readRawPage(createPageIdFromUint64(pgId))
		if err != nil {
			return err
		}
		for _, change := range changeList {
			copy(rawPage[change.off:], change.dat)
		}
		err = bt.s.writeRawPage(createPageIdFromUint64(pgId), rawPage)
		if err != nil {
			return err
		}
	}
	for pgId, changeList := range txh.freePageChangeRecord {
		rawPage, err := bt.s.freelist.readRawPage(pgId)
		if err != nil {
			return err
		}
		for _, change := range changeList {
			copy(rawPage[change.off:], change.dat)
		}
		err = bt.s.freelist.writeRawPage(pgId, rawPage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bt *BTreeDisk[K, V]) OpenReadTx(logic func(tx *Tx[K, V]) error) (err error) {
	tx := &Tx[K, V]{
		header: &txHeader{
			seq:        bt.txSeq.Load(),
			isRead:     true,
			isRollback: false,
			isCommit:   false,
		},
		tree:      bt,
		recordLog: bt.recordLogFile,
	}
	err = tx.begin()
	if err != nil {
		return
	}
	err = logic(tx)
	if err != nil {
		return
	}
	return tx.commit()
}

func (bt *BTreeDisk[K, V]) OpenWriteTx(logic func(tx *Tx[K, V]) error) (err error) {
	tx := &Tx[K, V]{
		header: &txHeader{
			seq:        bt.txSeq.Add(1),
			isRead:     false,
			isRollback: false,
			isCommit:   false,
		},
		tree:      bt,
		recordLog: bt.recordLogFile,
	}
	err = tx.begin()
	if err != nil {
		return err
	}
	err = logic(tx)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			return err2
		}
		return err
	}
	return tx.commit()
}

// 丢弃所有脏页, 用于回滚未提交的事务
func (bt *BTreeDisk[K, V]) rollbackDirtyPage() {
	bt.s.deleteAllDirtyPage()
}

func (bt *BTreeDisk[K, V]) loadRootNode() (d *nodeDiskDesc, err error) {
	var pd pageDesc
	pd, err = bt.s.readRootPage()
	if err != nil {
		return
	}
	return bt.loadNode(pd)
}

func (bt *BTreeDisk[K, V]) loadNode(pd pageDesc) (d *nodeDiskDesc, err error) {
	if pd.Header.Header != PageHeaderDat {
		err = fmt.Errorf("node type must is dat : %+v", pd.Header)
		return
	}
	var (
		off1    = uintptr(bt.c.TreeM) * unsafe.Sizeof(pageId{})
		readOff = off1
		curPd   = pd
		buf     bytes.Buffer
	)
	d = new(nodeDiskDesc)
	d.pd = pd
	d.subNodes = make([]pageId, bt.c.TreeM)
	for i := 0; i < bt.c.TreeM; i++ {
		d.subNodes[i] = pageId(d.pd.Data[i*6 : (i+1)*6])
	}
	keywordLen := d.pd.Header.Flags & uint64(math.MaxUint16)
	// TODO : 后台页整理, 有些页可能原来有很多溢出页, 但之后因为调整/删除各种原因数据变少之后这些不用的溢出页需要释放
	for {
		buf.Write(curPd.Data[readOff:])
		d.pageLink = append(d.pageLink, curPd)
		readOff = 0
		if curPd.Header.Overflow.ToUint64() > 0 {
			curPd, err = bt.s.readPage(curPd.Header.Overflow)
			if err != nil {
				return
			}
		} else {
			break
		}
	}
	b := buf.Bytes()
	for i := 0; i < int(keywordLen); i++ {
		kw := *(*keywordDiskDesc)(unsafe.Pointer(&b[0]))
		b = b[8:]
		d.keywordsPageView = append(d.keywordsPageView, kw)
		d.memKeywords = append(d.memKeywords, keywordDisk{
			key: b[:kw.keyLen],
			val: b[kw.valLen : kw.keyLen+kw.valLen],
		})
		b = b[kw.keyLen+kw.valLen:]
	}
	//for i := 0; i < bt.m; i++ {
	//	if off1+8 >= uintptr(len(curPd.Data)) {
	//		if !(curPd.Header.Overflow.ToUint64() > 0) {
	//			break
	//		}
	//		curPd, err = bt.s.readPage(curPd.Header.Overflow)
	//		if err != nil {
	//			return
	//		}
	//		off1 = 0
	//	}
	//	var (
	//		buf []byte
	//		kw  = (*keywordDiskDesc)(unsafe.Pointer(&curPd.Data[off1]))
	//	)
	//	off1 += 8
	//	if kw.keyLen == 0 && kw.valLen == 0 {
	//		break
	//	}
	//	buf = make([]byte, 0, kw.keyLen+kw.valLen)
	//	readCount := uintptr(kw.keyLen + kw.valLen)
	//	for readCount == 0 {
	//		oneMaxRead := readCount
	//		if off1+oneMaxRead >= uintptr(len(curPd.Data)) {
	//			oneMaxRead = uintptr(len(curPd.Data)) - off1
	//		}
	//		bufOldLen := uintptr(len(buf))
	//		buf = buf[:bufOldLen+oneMaxRead]
	//		copy(buf[bufOldLen:], curPd.Data[off1:off1+oneMaxRead])
	//		readCount -= oneMaxRead
	//		off1 += oneMaxRead
	//		if off1 == uintptr(len(curPd.Data))-1 && readCount > 0 {
	//			// ???, key/value从长度上判断有溢出, 但没有溢出页
	//			if !(curPd.Header.Overflow.ToUint64() > 0) {
	//				err = fmt.Errorf("pageId(%d) keyword idx(%d) len overflow, but no overflow page", curPd.Header.PgId.ToUint64(), i)
	//				return
	//			}
	//			curPd, err = bt.s.readPage(curPd.Header.Overflow)
	//			if err != nil {
	//				return
	//			}
	//			off1 = 0
	//		}
	//	}
	//	d.keywordsPageView = append(d.keywordsPageView, *kw)
	//	d.memKeywords = append(d.memKeywords, keywordDisk{
	//		key: buf[:kw.keyLen],
	//		val: buf[kw.valLen:],
	//	})
	//}
	return
}

func (bt *BTreeDisk[K, V]) loadNodeWithPageId(pgId pageId) (d *nodeDiskDesc, err error) {
	var pd pageDesc
	pd, err = bt.s.readPage(pgId)
	if err != nil {
		return
	}
	return bt.loadNode(pd)
}

func (bt *BTreeDisk[K, V]) allocNode(tx *Tx[K, V]) (d *nodeDiskDesc, err error) {
	var (
		res []pageId
		pd  pageDesc
	)
	res, err = bt.s.allocPage(tx.header, 1)
	if err != nil {
		return
	}
	pd, err = bt.s.readPage(res[0])
	if err != nil {
		return
	}
	d = new(nodeDiskDesc)
	d.pd = pd
	d.pageLink = make([]pageDesc, 0, 4)
	d.subNodes = make([]pageId, bt.c.TreeM)
	d.memKeywords = make([]keywordDisk, 0, 4)
	d.keywordsPageView = make([]keywordDiskDesc, 0, 4)
	return
}

func (bt *BTreeDisk[K, V]) freeNode(tx *Tx[K, V], node *nodeDiskDesc) (err error) {
	ipg := node.pd
	allPage := make([]pageId, 0, 4)
	zeroPgId := createPageIdFromUint64(0)
	for i := 0; i < len(node.subNodes); i++ {
		node.subNodes[i] = zeroPgId
	}
	for {
		allPage = append(allPage, ipg.Header.PgId)
		overflowPgId := ipg.Header.Overflow
		// 回收时不清page
		//err = bt.s.cleanPage(ipg)
		//if err != nil {
		//	return
		//}
		if overflowPgId.ToUint64() == 0 {
			break
		}
		ipg, err = bt.s.readPage(ipg.Header.Overflow)
		if err != nil {
			return
		}
	}
	return bt.s.freePage(tx.header, allPage)
}

func (bt *BTreeDisk[K, V]) flushSubNodes(tx *Tx[K, V], node *nodeDiskDesc) (err error) {
	for i := 0; i < len(node.subNodes); i++ {
		copy(node.pd.Data[i:i*6], node.subNodes[i][:])
	}
	cpDat := make([]byte, len(node.subNodes)*6)
	copy(cpDat, node.pd.Data)
	err = tx.addPageModify(pageRecord{
		typ:  pageRecordStorage,
		pgId: node.pd.Header.PgId,
		off:  node.pd.minSize(),
		dat:  cpDat,
	})
	if err != nil {
		return
	}
	return bt.s.writePage(tx.header, node.pd)
}

func (bt *BTreeDisk[K, V]) flushNodeKeywords(tx *Tx[K, V], node *nodeDiskDesc, idx int, onlyFlushLen bool) error {
	// 仅刷新长度信息
	if onlyFlushLen {
		keywordLen := uint32(len(node.memKeywords))
		node.pd.SetFlags(uint64(keywordLen))
		return bt.s.writePage(tx.header, node.pd)
	} else {
		// 判断写入模式
		// 3种情况, 内存中的值比磁盘中的少/跟磁盘的数量一样/比磁盘中的多, 一一处理
		if len(node.memKeywords) > len(node.keywordsPageView) {
			// 偏移必须在磁盘视图之内
			if !(idx <= len(node.keywordsPageView)) {
				return fmt.Errorf("memKeywords flush index overflow : %d", len(node.memKeywords)-1)
			}
			diskPageViewLen := len(node.keywordsPageView)
			memKeywordLen := len(node.memKeywords)
			for i := 0; i < memKeywordLen-diskPageViewLen; i++ {
				node.keywordsPageView = append(node.keywordsPageView, keywordDiskDesc{})
			}
			var buf bytes.Buffer
			for i := idx; i < len(node.memKeywords); i++ {
				key := node.memKeywords[i].key
				val := node.memKeywords[i].val
				kw := keywordDiskDesc{
					keyLen: uint32(len(key)),
					valLen: uint32(len(val)),
				}
				node.keywordsPageView[i] = kw
				b := *(*[8]byte)(unsafe.Pointer(&kw))
				buf.Write(b[:])
				buf.Write(key)
				buf.Write(val)
			}
			// 写入的长度要手动刷新, keywords所在的页不一定是起始页
			node.pd.SetFlags(uint64(len(node.memKeywords)))
			err := bt.s.writePage(tx.header, node.pd)
			if err != nil {
				return err
			}
			err = bt.writeKeywordDataToNode(tx, node, node.sumKeywordDiskOff(idx), buf.Bytes())
			if err != nil {
				return err
			}
		} else if len(node.memKeywords) < len(node.keywordsPageView) {
			node.keywordsPageView = node.keywordsPageView[:len(node.memKeywords)]
			var buf bytes.Buffer
			for i := idx; i < len(node.memKeywords); i++ {
				key := node.memKeywords[i].key
				val := node.memKeywords[i].val
				kw := keywordDiskDesc{
					keyLen: uint32(len(key)),
					valLen: uint32(len(val)),
				}
				node.keywordsPageView[i] = kw
				b := *(*[8]byte)(unsafe.Pointer(&kw))
				buf.Write(b[:])
				buf.Write(key)
				buf.Write(val)
			}
			// 写入的长度要手动刷新, keywords所在的页不一定是起始页
			node.pd.SetFlags(uint64(len(node.memKeywords)))
			err := bt.s.writePage(tx.header, node.pd)
			if err != nil {
				return err
			}
			err = bt.writeKeywordDataToNode(tx, node, node.sumKeywordDiskOff(idx), buf.Bytes())
			if err != nil {
				return err
			}
		} else {
			var buf bytes.Buffer
			for i := idx; i < len(node.memKeywords); i++ {
				key := node.memKeywords[i].key
				val := node.memKeywords[i].val
				kw := keywordDiskDesc{
					keyLen: uint32(len(key)),
					valLen: uint32(len(val)),
				}
				node.keywordsPageView[i] = kw
				b := *(*[8]byte)(unsafe.Pointer(&kw))
				buf.Write(b[:])
				buf.Write(key)
				buf.Write(val)
			}
			err := bt.writeKeywordDataToNode(tx, node, node.sumKeywordDiskOff(idx), buf.Bytes())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (bt *BTreeDisk[K, V]) writeKeywordDataToNode(tx *Tx[K, V], node *nodeDiskDesc, offset uint32, buf []byte) (err error) {
	// 略过subNodes的位置
	offset += uint32(uintptr(bt.c.TreeM) * unsafe.Sizeof(pageId{}))
	// 定位所在的page
	var (
		targetIdx  int
		targetPage *pageDesc
	)
	for i := 0; i < len(node.pageLink); i++ {
		page := node.pageLink[i]
		if offset < uint32(len(page.Data)) {
			targetPage = &node.pageLink[i]
			targetIdx = i
			break
		} else {
			offset -= uint32(len(page.Data))
		}
	}
	if targetPage == nil {
		return fmt.Errorf("wirte offset(%d) overflow", offset)
	}
	for len(buf) > 0 {
		maxWrite := uint32(len(targetPage.Data)) - offset
		if maxWrite > uint32(len(buf)) {
			maxWrite = uint32(len(buf))
		}
		copy(targetPage.Data[offset:offset+maxWrite], buf[:maxWrite])
		cpDat := make([]byte, maxWrite)
		copy(cpDat, buf[:maxWrite])
		err = tx.addPageModify(pageRecord{
			typ:  pageRecordStorage,
			pgId: targetPage.Header.PgId,
			off:  offset + targetPage.minSize(),
			dat:  cpDat,
		})
		if err != nil {
			return err
		}
		err = bt.s.writePage(tx.header, *targetPage)
		if err != nil {
			return err
		}
		offset = 0
		buf = buf[maxWrite:]
		// 写满一页了, 但还有数据
		if len(buf) > 0 {
			// 没有可用的页了, 分配新页
			if targetIdx+1 >= len(node.pageLink) {
				res, err := bt.s.allocPage(tx.header, 1)
				if err != nil {
					return err
				}
				newPage, err := bt.s.readPage(res[0])
				if err != nil {
					return err
				}
				node.pageLink = append(node.pageLink, newPage)
				// 设置溢出页
				targetPage.Header.Overflow = newPage.Header.PgId
				// writePage会设置header的修改记录, 这里就不手动设置了
				err = bt.s.writePage(tx.header, *targetPage)
				if err != nil {
					return
				}
				targetPage = &node.pageLink[len(node.pageLink)-1]
			} else {
				targetPage = &node.pageLink[targetIdx+1]
			}
		}
	}
	return nil
}

func (bt *BTreeDisk[K, V]) put(tx *Tx[K, V], key, val []byte) (bool, error) {
	var (
		root *nodeDiskDesc
		err  error
	)
	root, err = bt.loadRootNode()
	if err != nil {
		return false, err
	}
	if len(root.memKeywords) == 0 {
		root.memKeywords = append(root.memKeywords, keywordDisk{
			key: key,
			val: val,
		})
		return false, bt.flushNodeKeywords(tx, root, 0, false)
	}
	isReplace, isFull, err := bt.doPut(tx, root, key, val)
	if err != nil {
		return false, err
	}
	if isFull {
		mediumElem, left, right, err := bt.splitNode(tx, root)
		if err != nil {
			return false, err
		}
		newRoot, err := bt.allocNode(tx)
		if err != nil {
			return false, err
		}
		newRoot.memKeywords = append(newRoot.memKeywords, mediumElem)
		newRoot.subNodes[0] = left.pd.Header.PgId
		newRoot.subNodes[1] = right.pd.Header.PgId
		err = bt.flushSubNodes(tx, newRoot)
		if err != nil {
			return false, err
		}
		err = bt.flushNodeKeywords(tx, newRoot, 0, false)
		if err != nil {
			return false, err
		}
		err = bt.freeNode(tx, root)
		if err != nil {
			return false, err
		}
		// 重新设置根节点
		err = bt.s.setRootPage(tx.header, newRoot.pd)
		if err != nil {
			return false, err
		}
	}
	if !isReplace {
		bt.size.Add(1)
	}
	return isReplace, nil
}

func (bt *BTreeDisk[K, V]) doPut(tx *Tx[K, V], root *nodeDiskDesc, key, val []byte) (bool, bool, error) {
	index, found := slices.BinarySearchFunc(root.memKeywords, key, func(a keywordDisk, b []byte) int {
		return bytes.Compare(a.key, b)
	})
	if found {
		root.memKeywords[index].val = val
		err := bt.flushNodeKeywords(tx, root, index, false)
		if err != nil {
			return false, false, err
		}
		return true, false, nil
	}
	if root.isLeaf() {
		root.memKeywords = slices.Insert(root.memKeywords, index, keywordDisk{
			key: key,
			val: val,
		})
		err := bt.flushNodeKeywords(tx, root, index, false)
		if err != nil {
			return false, false, err
		}
		return false, bt.nodeEQMax(root), nil
	} else {
		subNodePd, err := bt.s.readPage(root.subNodes[index])
		if err != nil {
			return false, false, err
		}
		subNode, err := bt.loadNode(subNodePd)
		if err != nil {
			return false, false, err
		}
		isReplace, isFull, err := bt.doPut(tx, subNode, key, val)
		if err != nil {
			return false, false, err
		}
		// do split
		if isFull {
			mediumElem, left, right, err := bt.splitNode(tx, subNode)
			if err != nil {
				return false, false, err
			}
			root.memKeywords = slices.Insert(root.memKeywords, index, mediumElem)
			root.subNodes[index] = left.pd.Header.PgId
			root.subNodes = slices.Insert(root.subNodes, index+1, right.pd.Header.PgId)
			err = bt.flushSubNodes(tx, root)
			if err != nil {
				return false, false, err
			}
			err = bt.flushNodeKeywords(tx, root, index, false)
			if err != nil {
				return false, false, err
			}
			err = bt.freeNode(tx, subNode)
			if err != nil {
				return false, false, fmt.Errorf("freeNode err: %v", err)
			}
		}
		return isReplace, bt.nodeEQMax(root), nil
	}
}

func (bt *BTreeDisk[K, V]) splitNode(tx *Tx[K, V], root *nodeDiskDesc) (medium keywordDisk, s1, s2 *nodeDiskDesc, err error) {
	//medium = root.keywords[len(root.keywords)/2]
	//s1 = &btNode[K, V]{
	//	keywords: root.keywords[:len(root.keywords)/2],
	//}
	//s2 = &btNode[K, V]{
	//	keywords: root.keywords[len(root.keywords)/2+1:],
	//}
	//if len(root.subNodes) > 0 {
	//	s1.subNodes = root.subNodes[:len(root.subNodes)/2]
	//	s2.subNodes = root.subNodes[len(root.subNodes)/2:]
	//}
	mediumIdx := len(root.memKeywords) / 2
	medium = root.memKeywords[mediumIdx]
	s1, err = bt.allocNode(tx)
	if err != nil {
		return
	}
	s2, err = bt.allocNode(tx)
	if err != nil {
		return
	}
	s1.memKeywords = root.memKeywords[:len(root.memKeywords)/2]
	s2.memKeywords = root.memKeywords[len(root.memKeywords)/2+1:]
	if root.isLeaf() {
		rootSubNodes := root.effectiveSubNodes()
		copy(s1.subNodes, rootSubNodes[:len(rootSubNodes)/2])
		copy(s2.subNodes, rootSubNodes[len(rootSubNodes)/2:])
	}
	err = bt.flushNodeKeywords(tx, s1, 0, false)
	if err != nil {
		return
	}
	err = bt.flushNodeKeywords(tx, s2, 0, false)
	if err != nil {
		return
	}
	return
}

func (bt *BTreeDisk[K, V]) get(tx *Tx[K, V], key []byte) (val []byte, found bool, err error) {
	var (
		targetNode *nodeDiskDesc
		root       *nodeDiskDesc
		idx        int
	)
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	targetNode, idx, err = bt.findNode(root, nil, key)
	if err != nil {
		return
	}
	if idx == -1 {
		return
	}
	found = true
	val = targetNode.memKeywords[idx].val
	return
}

func (bt *BTreeDisk[K, V]) findNode(root *nodeDiskDesc, s *stack, key []byte) (*nodeDiskDesc, int, error) {
	index, found := slices.BinarySearchFunc(root.memKeywords, key, func(a keywordDisk, b []byte) int {
		return bytes.Compare(a.key, b)
	})
	if s != nil {
		s.push(stackElement{
			node: root,
			tag:  uint64(index),
		})
	}
	if found {
		return root, index, nil
	} else {
		if root.isLeaf() {
			return nil, -1, nil
		} else {
			pd, err := bt.s.readPage(root.subNodes[index])
			if err != nil {
				return nil, -1, err
			}
			subNode, err := bt.loadNode(pd)
			if err != nil {
				return nil, -1, err
			}
			return bt.findNode(subNode, s, key)
		}
	}
}

func (bt *BTreeDisk[K, V]) maxKey(tx *Tx[K, V]) (key []byte, err error) {
	var root *nodeDiskDesc
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	return bt.maxKeyWithNode(root)
}

func (bt *BTreeDisk[K, V]) maxKeyWithNode(root *nodeDiskDesc) (key []byte, err error) {
	for {
		key = root.memKeywords[len(root.memKeywords)-1].key
		if root.isLeaf() {
			break
		}
		// 子节点总是比关键字的数量多1, 有子节点的情况下
		var pd pageDesc
		pd, err = bt.s.readPage(root.subNodes[len(root.memKeywords)])
		if err != nil {
			return
		}
		root, err = bt.loadNode(pd)
		if err != nil {
			return
		}
	}
	return
}

func (bt *BTreeDisk[K, V]) minKey(tx *Tx[K, V]) (key []byte, err error) {
	var root *nodeDiskDesc
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	return bt.minKeyWithNode(root)
}

func (bt *BTreeDisk[K, V]) minKeyWithNode(root *nodeDiskDesc) (key []byte, err error) {
	for {
		key = root.memKeywords[0].key
		if root.isLeaf() {
			break
		}
		// 子节点总是比关键字的数量多1, 有子节点的情况下
		var pd pageDesc
		pd, err = bt.s.readPage(root.subNodes[0])
		if err != nil {
			return
		}
		root, err = bt.loadNode(pd)
		if err != nil {
			return
		}
	}
	return
}

func (bt *BTreeDisk[K, V]) treeRange(tx *Tx[K, V], start []byte, fn func(key, val []byte) bool) (err error) {
	var root *nodeDiskDesc
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	s := new(stack)
	return bt.doRange(root, s, start, fn)
}

func (bt *BTreeDisk[K, V]) doRange(root *nodeDiskDesc, s *stack, start []byte, fn func(key, val []byte) bool) error {
	index, found := slices.BinarySearchFunc(root.memKeywords, start, func(a keywordDisk, b []byte) int {
		return bytes.Compare(a.key, b)
	})
	if !found {
		if root.isLeaf() {
			return nil
		}
		s.push(stackElement{node: root, tag: uint64(index)})
		subNode, err := bt.loadNodeWithPageId(root.subNodes[index])
		if err != nil {
			return err
		}
		return bt.doRange(subNode, s, start, fn)
	} else {
		isContinue, err := bt.rangeOfCentral(root, index, true, fn)
		if err != nil {
			return err
		}
		if !isContinue {
			return nil
		}
		for {
			parent := s.pop()
			if parent.node == nil {
				break
			}
			isContinue, err = bt.rangeOfCentral(parent.node, int(parent.tag), true, fn)
			if err != nil {
				return err
			}
			if !isContinue {
				break
			}
		}
		return nil
	}
}

func (bt *BTreeDisk[K, V]) rangeOfCentral(node *nodeDiskDesc, eIndex int, isFirst bool, fn func(key, val []byte) bool) (bool, error) {
	if isFirst {
		for i := eIndex; i < len(node.memKeywords); i++ {
			keyword := node.memKeywords[i]
			if !fn(keyword.key, keyword.val) {
				return false, nil
			}
			if node.isLeaf() {
				continue
			}
			subNode, err := bt.loadNodeWithPageId(node.subNodes[i+1])
			if err != nil {
				return false, err
			}
			isContinue, err := bt.rangeOfCentral(subNode, 0, false, fn)
			if err != nil {
				return false, err
			}
			if !isContinue {
				return false, nil
			}
		}
	} else {
		for i := eIndex; i < len(node.memKeywords); i++ {
			keyword := node.memKeywords[i]
			if node.isLeaf() {
				if !fn(keyword.key, keyword.val) {
					return false, nil
				}
			} else {
				subNode, err := bt.loadNodeWithPageId(node.subNodes[i])
				if err != nil {
					return false, err
				}
				isContinue, err := bt.rangeOfCentral(subNode, 0, false, fn)
				if err != nil {
					return false, err
				}
				if !isContinue {
					return false, nil
				}
				if !fn(keyword.key, keyword.val) {
					return false, nil
				}
				subNode, err = bt.loadNodeWithPageId(node.subNodes[i+1])
				if err != nil {
					return false, err
				}
				isContinue, err = bt.rangeOfCentral(subNode, 0, false, fn)
				if err != nil {
					return false, err
				}
				if !isContinue {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

func (bt *BTreeDisk[K, V]) del(tx *Tx[K, V], key []byte) (val []byte, found bool, err error) {
	var (
		root  *nodeDiskDesc
		node  *nodeDiskDesc
		node2 *nodeDiskDesc
		idx   int
	)
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	s := new(stack)
	node, idx, err = bt.findNode(root, s, key)
	if err != nil {
		return
	}
	if idx == -1 {
		return
	}
	val = node.memKeywords[idx].val
	found = true
	bt.size.Add(math.MaxUint64)
	// 在叶节点发生删除, 直接删除关键字后检查是否需要做下溢或者连接
	if node.isLeaf() {
		node.memKeywords = slices.Delete(node.memKeywords, idx, idx+1)
		err = bt.flushNodeKeywords(tx, node, idx-1, false)
		if err != nil {
			return
		}
		s.pop()
		err = bt.del2(tx, node, s)
		return
	}
	node2, err = bt.loadNodeWithPageId(node.subNodes[idx])
	if err != nil {
		return
	}
	for !node2.isLeaf() {
		s.push(stackElement{
			node: node2,
			tag:  uint64(len(node.subNodes) - 1),
		})
		node2, err = bt.loadNodeWithPageId(node2.subNodes[len(node.subNodes)-1])
		if err != nil {
			return
		}
	}
	node.memKeywords[idx] = node2.memKeywords[len(node2.memKeywords)-1]
	node2.memKeywords = node2.memKeywords[:len(node2.memKeywords)-1]
	err = bt.flushNodeKeywords(tx, node, idx, false)
	if err != nil {
		return
	}
	err = bt.flushNodeKeywords(tx, node2, 0, false)
	if err != nil {
		return
	}
	err = bt.del2(tx, node2, s)
	return
}

// 处理下溢和连接
func (bt *BTreeDisk[K, V]) del2(tx *Tx[K, V], leafNode *nodeDiskDesc, s *stack) error {
	if !(len(leafNode.memKeywords) < bt.c.TreeM/2) {
		return nil
	}
	node := leafNode
	for {
		parent := s.pop()
		if parent.node == nil {
			break
		}
		parentNode, parentIdx := parent.node, parent.tag
		// 查看兄弟节点是否有多余的关键字, 可以借一个过来, 可能是左兄弟或者右兄弟
		if parentIdx+1 >= uint64(parentNode.subNodeSize()) {
			// 在最右侧的节点, 只能借左兄弟了
			leftNode, err := bt.loadNodeWithPageId(parentNode.subNodes[parentIdx-1])
			if err != nil {
				return err
			}
			if bt.nodeGEQMin(leftNode) {
				node.memKeywords = slices.Insert(node.memKeywords, 0, parentNode.memKeywords[parentIdx])
				parentNode.memKeywords[parentIdx] = leftNode.delLastKeyword()
				err = bt.flushNodeKeywords(tx, node, 0, false)
				if err != nil {
					return err
				}
				err = bt.flushNodeKeywords(tx, parentNode, int(parentIdx), false)
				if err != nil {
					return err
				}
				err = bt.flushNodeKeywords(tx, leftNode, len(leftNode.memKeywords)-1, true)
				if err != nil {
					return err
				}
				break
			} else {
				goto elseLogic
			}
		} else if parentIdx+1 < uint64(parentNode.subNodeSize()) {
			// 可以借右兄弟的节点
			rightNode, err := bt.loadNodeWithPageId(parentNode.subNodes[parentIdx+1])
			if err != nil {
				return err
			}
			if bt.nodeGEQMin(rightNode) {
				node.memKeywords = append(node.memKeywords, parentNode.memKeywords[parentIdx])
				parentNode.memKeywords[parentIdx] = rightNode.delFirstKeyword()
				err = bt.flushNodeKeywords(tx, node, len(node.memKeywords)-1, false)
				if err != nil {
					return err
				}
				err = bt.flushNodeKeywords(tx, parentNode, int(parentIdx), false)
				if err != nil {
					return err
				}
				err = bt.flushNodeKeywords(tx, rightNode, len(rightNode.memKeywords)-1, true)
				if err != nil {
					return err
				}
				break
			} else {
				goto elseLogic
			}
		}
	elseLogic:
		// parentNode.keywords[parentIdx] = parentNode.subNodes[parentIdx+1].keywords[len(parentNode.subNodes[parentIdx+1].keywords)-1]
		// 中间节点下推
		oldKeywordLen := len(node.memKeywords)
		node.memKeywords = append(node.memKeywords, parentNode.memKeywords[parentIdx])
		// 合并右兄弟节点
		rightNode, err := bt.loadNodeWithPageId(parentNode.subNodes[parentIdx+1])
		if err != nil {
			return err
		}
		node.memKeywords = append(node.memKeywords, rightNode.memKeywords...)
		rightSubNodes := rightNode.effectiveSubNodes()
		copy(node.subNodes[node.subNodeSize():], rightSubNodes)
		// 合并完成之后删除父节点中的元素和多余的子节点
		parentNode.memKeywords = slices.Delete(parentNode.memKeywords, int(parentIdx), int(parentIdx)+1)
		parentNode.subNodes = slices.Delete(parentNode.subNodes, int(parentIdx+1), int(parentIdx+2))
		parentNode.subNodes = append(parentNode.subNodes, createPageIdFromUint64(0))
		// 将改动刷盘并释放兄弟节点
		err = bt.flushSubNodes(tx, node)
		if err != nil {
			return err
		}
		err = bt.flushNodeKeywords(tx, node, oldKeywordLen-1, false)
		if err != nil {
			return err
		}
		err = bt.flushSubNodes(tx, parentNode)
		if err != nil {
			return err
		}
		err = bt.flushNodeKeywords(tx, parentNode, int(parentIdx), false)
		if err != nil {
			return err
		}
		err = bt.freeNode(tx, rightNode)
		if err != nil {
			return err
		}
		if bt.nodeGEQMin(parentNode) {
			break
		}
		// 合并操作导致根节点没有元素了, 那就将子节点作为根节点
		if len(parentNode.memKeywords) == 0 && s.peek().node == nil {
			err = bt.s.setRootPageWithPageId(tx.header, parentNode.subNodes[0])
			if err != nil {
				return err
			}
			break
		}
		node = parentNode
	}
	return nil
}

func (bt *BTreeDisk[K, V]) nodeGEQMin(node *nodeDiskDesc) bool {
	return len(node.memKeywords) >= bt.c.TreeM/2-1
}

func (bt *BTreeDisk[K, V]) nodeEQMax(node *nodeDiskDesc) bool {
	return len(node.memKeywords) == bt.c.TreeM-1
}
