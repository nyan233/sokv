package sokv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
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
	ipg      *pageDesc
	pageLink []*pageDesc
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

type BTreeDisk struct {
	rw    sync.RWMutex
	txMgr *txMgr
	size  atomic.Uint64
	s     *mmapPageStorage
	m     int
}

func NewBTreeDisk(path string, m int) *BTreeDisk {
	return &BTreeDisk{
		s: newMMapPageStorage(path),
		m: m,
	}
}

func (bt *BTreeDisk) Init() error {
	if bt.m > treeMaxM {
		return fmt.Errorf("bt.m > treeMaxM(%d)", treeMaxM)
	}
	err := bt.s.init()
	if err != nil {
		return err
	}
	pgSize := bt.s.getPageSize()
	slotSize := bt.m * int(unsafe.Sizeof(pageId{})+8)
	// 节点分槽占用的空间比一个页还要大
	if slotSize > int(pgSize) {
		return fmt.Errorf("slotSize(%d) > pgSize(%d)", slotSize, pgSize)
	}
	localDataBytes, err := bt.s.loadLocalData()
	if err != nil {
		return err
	}
	localData := &diskLocalData{
		M: bt.m,
	}
	if len(localDataBytes) > 0 {
		err = json.Unmarshal(localDataBytes, localData)
		if err != nil {
			return err
		}
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
	return nil
}

func (bt *BTreeDisk) OpenReadTx(logic func(tx *Tx) error) (err error) {
	tx := bt.txMgr.allocReadTransaction(bt)
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

func (bt *BTreeDisk) OpenWriteTx(logic func(tx *Tx) error) (err error) {
	tx := bt.txMgr.allocWriteTransaction(bt)
	err = tx.begin()
	if err != nil {
		return err
	}
	err = logic(tx)
	if err != nil {
		return
	}
	return tx.commit()
}

func (bt *BTreeDisk) loadRootNode() (d *nodeDiskDesc, err error) {
	var pd *pageDesc
	pd, err = bt.s.readRootPage()
	if err != nil {
		return
	}
	return bt.loadNode(pd)
}

func (bt *BTreeDisk) loadNode(pd *pageDesc) (d *nodeDiskDesc, err error) {
	if pd.Header.Header != PageHeaderDat {
		err = fmt.Errorf("node type must is dat : %+v", pd.Header)
		return
	}
	var (
		off1    = uintptr(bt.m) * unsafe.Sizeof(pageId{})
		readOff = off1
		curPd   = pd
		buf     bytes.Buffer
	)
	d = new(nodeDiskDesc)
	d.ipg = pd
	p := uintptr(unsafe.Pointer(&pd.Data[0]))
	d.subNodes = unsafe.Slice((*pageId)(unsafe.Pointer(p)), bt.m)
	keywordLen := d.ipg.Header.Flags & uint64(math.MaxUint16)
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

func (bt *BTreeDisk) loadNodeWithPageId(pgId pageId) (d *nodeDiskDesc, err error) {
	var pd *pageDesc
	pd, err = bt.s.readPage(pgId)
	if err != nil {
		return
	}
	return bt.loadNode(pd)
}

func (bt *BTreeDisk) allocNode() (d *nodeDiskDesc, err error) {
	var (
		res []pageId
		pd  *pageDesc
	)
	res, err = bt.s.allocPage(1)
	if err != nil {
		return
	}
	pd, err = bt.s.readPage(res[0])
	if err != nil {
		return
	}
	return bt.loadNode(pd)
}

func (bt *BTreeDisk) freeNode(node *nodeDiskDesc) (err error) {
	ipg := node.ipg
	allPage := make([]pageId, 0, 4)
	zeroPgId := createPageIdFromUint64(0)
	for i := 0; i < len(node.subNodes); i++ {
		node.subNodes[i] = zeroPgId
	}
	for {
		allPage = append(allPage, ipg.Header.PgId)
		overflowPgId := ipg.Header.Overflow
		err = bt.s.cleanPage(ipg)
		if err != nil {
			return
		}
		if overflowPgId.ToUint64() == 0 {
			break
		}
		ipg, err = bt.s.readPage(ipg.Header.Overflow)
		if err != nil {
			return
		}
	}
	return bt.s.freePage(allPage)
}

func (bt *BTreeDisk) flushNodeKeywords(node *nodeDiskDesc, idx int, onlyFlushLen bool) error {
	// 仅刷新长度信息
	if onlyFlushLen {
		keywordLen := uint32(len(node.memKeywords))
		node.ipg.SetFlags(uint64(keywordLen))
	} else {
		//// 判断写入模式
		//if idx >= len(node.keywordsPageView) && len(node.memKeywords) > len(node.keywordsPageView) {
		//	// 仅追加
		//	var buf bytes.Buffer
		//	for i := len(node.keywordsPageView); i < len(node.memKeywords); i++ {
		//		key := node.memKeywords[i].key
		//		val := node.memKeywords[i].val
		//		b := *(*[8]byte)(unsafe.Pointer(&keywordDiskDesc{
		//			keyLen: uint32(len(key)),
		//			valLen: uint32(len(val)),
		//		}))
		//		buf.Write(b[:])
		//		buf.Write(key)
		//		buf.Write(val)
		//	}
		//	err := bt.writeKeywordDataToNode(node, node.sumKeywordDiskOff(len(node.keywordsPageView)-1), buf.Bytes())
		//	if err != nil {
		//		return err
		//	}
		//	node.ipg.SetFlags(uint64(len(node.memKeywords)))
		//} else if len(node.memKeywords) > len(node.keywordsPageView) {
		//	// 部分追加, 部分覆盖
		//	var buf bytes.Buffer
		//	for i := idx; i < len(node.memKeywords); i++ {
		//		key := node.memKeywords[i].key
		//		val := node.memKeywords[i].val
		//		b := *(*[8]byte)(unsafe.Pointer(&keywordDiskDesc{
		//			keyLen: uint32(len(key)),
		//			valLen: uint32(len(val)),
		//		}))
		//		buf.Write(b[:])
		//		buf.Write(key)
		//		buf.Write(val)
		//	}
		//	err := bt.writeKeywordDataToNode(node, node.sumKeywordDiskOff(idx), buf.Bytes())
		//	if err != nil {
		//		return err
		//	}
		//	node.ipg.SetFlags(uint64(len(node.memKeywords)))
		//} else {
		//	// 仅覆盖
		//	var buf bytes.Buffer
		//	for i := idx; i < len(node.memKeywords); i++ {
		//		key := node.memKeywords[i].key
		//		val := node.memKeywords[i].val
		//		b := *(*[8]byte)(unsafe.Pointer(&keywordDiskDesc{
		//			keyLen: uint32(len(key)),
		//			valLen: uint32(len(val)),
		//		}))
		//		buf.Write(b[:])
		//		buf.Write(key)
		//		buf.Write(val)
		//	}
		//	err := bt.writeKeywordDataToNode(node, node.sumKeywordDiskOff(idx), buf.Bytes())
		//	if err != nil {
		//		return err
		//	}
		//}
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
			err := bt.writeKeywordDataToNode(node, node.sumKeywordDiskOff(idx), buf.Bytes())
			if err != nil {
				return err
			}
			node.ipg.SetFlags(uint64(len(node.memKeywords)))
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
			err := bt.writeKeywordDataToNode(node, node.sumKeywordDiskOff(idx), buf.Bytes())
			if err != nil {
				return err
			}
			node.ipg.SetFlags(uint64(len(node.memKeywords)))
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
			err := bt.writeKeywordDataToNode(node, node.sumKeywordDiskOff(idx), buf.Bytes())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (bt *BTreeDisk) writeKeywordDataToNode(node *nodeDiskDesc, offset uint32, buf []byte) (err error) {
	// 略过subNodes的位置
	offset += uint32(uintptr(bt.m) * unsafe.Sizeof(pageId{}))
	// 定位所在的page
	var (
		targetIdx  int
		targetPage *pageDesc
	)
	for i := 0; i < len(node.pageLink); i++ {
		page := node.pageLink[i]
		if offset < uint32(len(page.Data)) {
			targetPage = page
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
		offset = 0
		buf = buf[maxWrite:]
		// 写满一页了, 但还有数据
		if len(buf) > 0 {
			// 没有可用的页了, 分配新页
			if targetIdx+1 >= len(node.pageLink) {
				res, err := bt.s.allocPage(1)
				if err != nil {
					return err
				}
				newPage, err := bt.s.readPage(res[0])
				if err != nil {
					return err
				}
				node.pageLink = append(node.pageLink, newPage)
				targetPage = newPage
			} else {
				targetPage = node.pageLink[targetIdx+1]
			}
		}
	}
	return nil
}

func (bt *BTreeDisk) put(tx *Tx, key, val []byte) (bool, error) {
	bt.rw.Lock()
	defer bt.rw.Unlock()
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
		return false, bt.flushNodeKeywords(root, 0, false)
	}
	isReplace, isFull, err := bt.doPut(root, key, val)
	if err != nil {
		return false, err
	}
	if isFull {
		mediumElem, left, right, err := bt.splitNode(root)
		if err != nil {
			return false, err
		}
		newRoot, err := bt.allocNode()
		if err != nil {
			return false, err
		}
		newRoot.memKeywords = append([]keywordDisk{}, mediumElem)
		newRoot.subNodes[0] = left.ipg.Header.PgId
		newRoot.subNodes[1] = right.ipg.Header.PgId
		err = bt.flushNodeKeywords(newRoot, 0, false)
		if err != nil {
			return false, err
		}
		err = bt.freeNode(root)
		if err != nil {
			return false, err
		}
		// 重新设置根节点
		err = bt.s.setRootPage(newRoot.ipg)
		if err != nil {
			return false, err
		}
	}
	if !isReplace {
		bt.size.Add(1)
	}
	return isReplace, nil
}

func (bt *BTreeDisk) doPut(root *nodeDiskDesc, key, val []byte) (bool, bool, error) {
	index, found := slices.BinarySearchFunc(root.memKeywords, key, func(a keywordDisk, b []byte) int {
		return bytes.Compare(a.key, b)
	})
	if found {
		root.memKeywords[index].val = val
		err := bt.flushNodeKeywords(root, index, false)
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
		err := bt.flushNodeKeywords(root, index, false)
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
		isReplace, isFull, err := bt.doPut(subNode, key, val)
		if err != nil {
			return false, false, err
		}
		// do split
		if isFull {
			mediumElem, left, right, err := bt.splitNode(subNode)
			if err != nil {
				return false, false, err
			}
			root.memKeywords = slices.Insert(root.memKeywords, index, mediumElem)
			root.subNodes[index] = left.ipg.Header.PgId
			root.subNodes = slices.Insert(root.subNodes, index+1, right.ipg.Header.PgId)
			err = bt.freeNode(subNode)
			if err != nil {
				return false, false, fmt.Errorf("freeNode err: %v", err)
			}
		}
		return isReplace, bt.nodeEQMax(root), nil
	}
}

func (bt *BTreeDisk) splitNode(root *nodeDiskDesc) (medium keywordDisk, s1, s2 *nodeDiskDesc, err error) {
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
	s1, err = bt.allocNode()
	if err != nil {
		return
	}
	s2, err = bt.allocNode()
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
	err = bt.flushNodeKeywords(s1, 0, false)
	if err != nil {
		return
	}
	err = bt.flushNodeKeywords(s2, 0, false)
	if err != nil {
		return
	}
	return
}

func (bt *BTreeDisk) get(tx *Tx, key []byte) (val []byte, found bool, err error) {
	bt.rw.RLock()
	defer bt.rw.RUnlock()
	var (
		targerNode *nodeDiskDesc
		root       *nodeDiskDesc
		idx        int
	)
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	targerNode, idx, err = bt.findNode(root, nil, key)
	if err != nil {
		return
	}
	if idx == -1 {
		return
	}
	found = true
	val = targerNode.memKeywords[idx].val
	return
}

func (bt *BTreeDisk) findNode(root *nodeDiskDesc, s *stack, key []byte) (*nodeDiskDesc, int, error) {
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

func (bt *BTreeDisk) maxKey(tx *Tx) (key []byte, err error) {
	bt.rw.RLock()
	defer bt.rw.RUnlock()
	var root *nodeDiskDesc
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	return bt.maxKeyWithNode(root)
}

func (bt *BTreeDisk) maxKeyWithNode(root *nodeDiskDesc) (key []byte, err error) {
	for {
		key = root.memKeywords[len(root.memKeywords)-1].key
		if root.isLeaf() {
			break
		}
		// 子节点总是比关键字的数量多1, 有子节点的情况下
		var pd *pageDesc
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

func (bt *BTreeDisk) minKey(tx *Tx) (key []byte, err error) {
	bt.rw.RLock()
	defer bt.rw.RUnlock()
	var root *nodeDiskDesc
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	return bt.minKeyWithNode(root)
}

func (bt *BTreeDisk) minKeyWithNode(root *nodeDiskDesc) (key []byte, err error) {
	for {
		key = root.memKeywords[0].key
		if root.isLeaf() {
			break
		}
		// 子节点总是比关键字的数量多1, 有子节点的情况下
		var pd *pageDesc
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

func (bt *BTreeDisk) treeRange(tx *Tx, start []byte, fn func(key, val []byte) bool) (err error) {
	bt.rw.RLock()
	defer bt.rw.RUnlock()
	var root *nodeDiskDesc
	root, err = bt.loadRootNode()
	if err != nil {
		return
	}
	s := new(stack)
	return bt.doRange(root, s, start, fn)
}

func (bt *BTreeDisk) doRange(root *nodeDiskDesc, s *stack, start []byte, fn func(key, val []byte) bool) error {
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

func (bt *BTreeDisk) rangeOfCentral(node *nodeDiskDesc, eIndex int, isFirst bool, fn func(key, val []byte) bool) (bool, error) {
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

func (bt *BTreeDisk) del(tx *Tx, key []byte) (val []byte, found bool, err error) {
	bt.rw.Lock()
	defer bt.rw.Unlock()
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
		err = bt.flushNodeKeywords(node, idx-1, false)
		if err != nil {
			return
		}
		s.pop()
		err = bt.del2(node, s)
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
	err = bt.flushNodeKeywords(node, idx, false)
	if err != nil {
		return
	}
	err = bt.flushNodeKeywords(node2, 0, false)
	if err != nil {
		return
	}
	err = bt.del2(node2, s)
	return
}

// 处理下溢和连接
func (bt *BTreeDisk) del2(leafNode *nodeDiskDesc, s *stack) error {
	if !(len(leafNode.memKeywords) < bt.m/2) {
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
				err = bt.flushNodeKeywords(node, 0, false)
				if err != nil {
					return err
				}
				err = bt.flushNodeKeywords(parentNode, int(parentIdx), false)
				if err != nil {
					return err
				}
				err = bt.flushNodeKeywords(leftNode, len(leftNode.memKeywords)-1, true)
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
				err = bt.flushNodeKeywords(node, len(node.memKeywords)-1, false)
				if err != nil {
					return err
				}
				err = bt.flushNodeKeywords(parentNode, int(parentIdx), false)
				if err != nil {
					return err
				}
				err = bt.flushNodeKeywords(rightNode, len(rightNode.memKeywords)-1, true)
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
		parentNode.subNodes = slices.Delete(parentNode.subNodes, int(parentIdx+1), int(parentIdx+1+1))
		if bt.nodeGEQMin(parentNode) {
			break
		}
		// 将改动刷盘并释放兄弟节点
		err = bt.flushNodeKeywords(node, oldKeywordLen-1, false)
		if err != nil {
			return err
		}
		err = bt.flushNodeKeywords(parentNode, int(parentIdx), false)
		if err != nil {
			return err
		}
		err = bt.freeNode(rightNode)
		if err != nil {
			return err
		}
		// 合并操作导致根节点没有元素了, 那就将子节点作为根节点
		if len(parentNode.memKeywords) == 0 && s.peek().node == nil {
			err = bt.s.setRootPageWithPageId(parentNode.subNodes[0])
			if err != nil {
				return err
			}
			break
		}
		node = parentNode
	}
	return nil
}

func (bt *BTreeDisk) nodeGEQMin(node *nodeDiskDesc) bool {
	return len(node.memKeywords) >= bt.m/2-1
}

func (bt *BTreeDisk) nodeEQMax(node *nodeDiskDesc) bool {
	return len(node.memKeywords) == bt.m-1
}
