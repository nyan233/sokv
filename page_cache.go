package sokv

import (
	"sync"
	"sync/atomic"
)

type cachePage struct {
	txSeq uint64
	data  []byte
	pgId  pageId
}

type batchPutItem struct {
	pgId uint64
	val  cachePage
}

type pageCacheI interface {
	getPage(txh *txHeader, pgId uint64) (p cachePage, found bool)
	putPage(txh *txHeader, pgId uint64, p cachePage) bool
	batchPutPage(txh *txHeader, l []batchPutItem) int
	createShadowPage(txh *txHeader)
	// writeBack == true则将改动写回到缓存中
	deleteShadowPage(txh *txHeader, writeBack bool)
	addShadowPages(txh *txHeader, pgs []cachePage)
	existShadowPage(txh *txHeader, pgId uint64) bool
	getShadowPageChange(txh *txHeader, pgId uint64) (cachePage, bool)
	opShadowPage(txh *txHeader, src cachePage, opFn func(p cachePage) (cachePage, bool))
	cap() int64
	len() int64
	clean()
}

type lfuNode struct {
	pgId uint64
	val  cachePage
	freq int
	link *freqLink
	prev *lfuNode
	next *lfuNode
}

type freqLink struct {
	freq         int
	cacheValLink *lfuNode
	prev         *freqLink
	next         *freqLink
}

type shadowPageItem struct {
	pgId uint64
	old  []byte
	new  []byte
}

// 关联事务的影子页, 写事务中持有的是修改后的页, 读事务持有原页
type txShadowPage struct {
	seq     uint64
	nodeMap map[uint64]*shadowPageItem
	pool    sync.Pool
}

func (s *txShadowPage) item2CacheVal(pgId uint64, data []byte) cachePage {
	return cachePage{
		txSeq: s.seq,
		data:  data,
		pgId:  createPageIdFromUint64(pgId),
	}
}

// lfuCache see : https://arxiv.org/pdf/2110.11602
type lfuCache struct {
	shadowPage *txShadowPage
	nodeMap    map[uint64]*lfuNode
	freqRoot   *freqLink
	capacity   int64
	size       atomic.Int64
	mu         sync.Mutex
}

func newLFUCache(capacity int) pageCacheI {
	if capacity%64 != 0 {
		panic("capacity must be a multiple of 64")
	}
	return &lfuCache{
		nodeMap: make(map[uint64]*lfuNode, 256),
		freqRoot: &freqLink{
			freq: 0,
			prev: nil,
			next: nil,
		},
		capacity: int64(capacity),
	}
}

func (l *lfuCache) getPage(txh *txHeader, pgId uint64) (p cachePage, found bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.shadowPage != nil {
		item, ok := l.shadowPage.nodeMap[pgId]
		if !ok {
			goto cacheGet
		}
		if txh.isWriteTx() {
			return l.shadowPage.item2CacheVal(pgId, item.new), true
		} else if !txh.isWriteTx() {
			return l.shadowPage.item2CacheVal(pgId, item.old), true
		}
	}
cacheGet:
	cacheVal, ok := l.nodeMap[pgId]
	if !ok {
		found = false
		return
	}
	l.upUseCount(cacheVal)
	return cacheVal.val, true
}

func (l *lfuCache) removeNode(v *lfuNode) {
	prev := v.prev
	next := v.next
	if prev != nil {
		prev.next = next
	}
	if next != nil {
		next.prev = prev
	}
}

func (l *lfuCache) upUseCount(v *lfuNode) {
	v.freq++
	l.removeNode(v)
	if v.link.next != nil && v.link.next.freq == v.freq {
		v.link = v.link.next
	} else {
		v.link = l.newFreqLink(v.link, v.link.next)
		v.link.freq = v.freq
	}
	if v.link.cacheValLink == nil {
		v.link.cacheValLink = v
	} else {
		v.next = v.link.cacheValLink
		v.link.cacheValLink.prev = v
		v.link.cacheValLink = v
	}
}

func (l *lfuCache) newFreqLink(prev, next *freqLink) *freqLink {
	newVal := &freqLink{
		freq: 0,
		prev: prev,
		next: next,
	}
	if prev != nil {
		prev.next = newVal
	}
	if next != nil {
		next.prev = newVal
	}
	return newVal
}

func (l *lfuCache) putPage(txh *txHeader, pgId uint64, p cachePage) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.doPut(pgId, p)
}

func (l *lfuCache) doPut(pgId uint64, p cachePage) bool {
	cacheVal, ok := l.nodeMap[pgId]
	// only update
	if ok {
		cacheVal.val = p
		return false
	}
	newCacheVal := &lfuNode{
		pgId: pgId,
		val:  p,
		freq: 0,
		link: nil,
		prev: nil,
		next: nil,
	}
	// full, remove other val
	if l.size.Load() >= l.capacity {
		// no interval
		if newCacheVal.freq < l.freqRoot.freq {
			return false
		}
	}
	l.nodeMap[pgId] = newCacheVal
	if l.freqRoot.freq != 0 {
		oldRoot := l.freqRoot
		l.freqRoot = &freqLink{
			freq: 0,
			prev: nil,
			next: oldRoot,
		}
		oldRoot.prev = l.freqRoot
	}
	newCacheVal.next = l.freqRoot.cacheValLink
	newCacheVal.link = l.freqRoot
	if l.freqRoot.cacheValLink != nil {
		l.freqRoot.cacheValLink.prev = newCacheVal
	}
	l.freqRoot.cacheValLink = newCacheVal
	return true
}

func (l *lfuCache) batchPutPage(txh *txHeader, itemList []batchPutItem) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for idx, item := range itemList {
		if !l.doPut(item.pgId, item.val) {
			return idx + 1
		}
	}
	return len(itemList)
}

func (l *lfuCache) cap() int64 {
	return l.capacity
}

func (l *lfuCache) len() int64 {
	return l.size.Load()
}

func (l *lfuCache) clean() {
	//TODO implement me
	panic("implement me")
}

func (l *lfuCache) createShadowPage(txh *txHeader) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.shadowPage = &txShadowPage{
		seq:     txh.seq,
		nodeMap: make(map[uint64]*shadowPageItem, 8),
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, recordSize)
			},
		},
	}
}

func (l *lfuCache) deleteShadowPage(txh *txHeader, writeBack bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, v := range l.shadowPage.nodeMap {
		l.shadowPage.pool.Put(v.old)
		if writeBack {
			_, ok := l.nodeMap[v.pgId]
			if ok {
				l.doPut(v.pgId, cachePage{
					txSeq: txh.seq,
					data:  v.new,
					pgId:  createPageIdFromUint64(v.pgId),
				})
			}
		}
	}
	l.shadowPage = nil
}

func (l *lfuCache) addShadowPages(txh *txHeader, pgs []cachePage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.shadowPage.seq != txh.seq {
		panic("shadowPage seq conflict")
	}
	for _, pg := range pgs {
		l.addShadowPage(txh, pg)
	}
}

func (l *lfuCache) addShadowPage(txh *txHeader, pg cachePage) {
	pgId := pg.pgId.ToUint64()
	item, ok := l.shadowPage.nodeMap[pgId]
	if !ok {
		oldData := l.shadowPage.pool.Get().([]byte)
		oldData = oldData[:0]
		oldData = append(oldData, pg.data...)
		item = &shadowPageItem{
			pgId: pgId,
			old:  oldData,
			new:  pg.data,
		}
		l.shadowPage.nodeMap[pgId] = item
	} else {
		item.new = pg.data
	}
}

func (l *lfuCache) existShadowPage(txh *txHeader, pgId uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.shadowPage.nodeMap[pgId]
	return ok
}

func (l *lfuCache) opShadowPage(txh *txHeader, src cachePage, opFn func(p cachePage) (cachePage, bool)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.shadowPage.nodeMap[src.pgId.ToUint64()]
	if !ok {
		l.addShadowPage(txh, src)
	}
	dst, ok := opFn(src)
	if !ok {
		return
	}
	l.addShadowPage(txh, dst)
}

func (l *lfuCache) getShadowPageChange(txh *txHeader, pgId uint64) (cachePage, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	item, ok := l.shadowPage.nodeMap[pgId]
	if !ok {
		return cachePage{}, false
	} else {
		return l.shadowPage.item2CacheVal(item.pgId, item.new), true
	}
}
