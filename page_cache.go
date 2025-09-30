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
	getPage(pgId uint64) (p cachePage, found bool)
	getAndLockPage(pgId uint64) (p cachePage, found bool)
	putPage(pgId uint64, p cachePage) bool
	putAndLockPage(pgId uint64, p cachePage) bool
	batchPutPage(l []batchPutItem, unlock bool) int
	cap() int64
	len() int64
	clean()
}

type lfuNode struct {
	pgId   uint64
	val    cachePage
	freq   int
	locked bool
	link   *freqLink
	prev   *lfuNode
	next   *lfuNode
}

type freqLink struct {
	freq         int
	cacheValLink *lfuNode
	prev         *freqLink
	next         *freqLink
}

// 关联事务的影子页, 写事务中持有的是修改后的页, 读事务持有原页
type txShadowPage struct {
	seq uint64
	// NOTE : 这里的和lfuCache obj中的nodeMap不太一样
	// 这里的lfuNode关联的可能是不在cache里的值, 用于做两阶段提交
	// 事务提交完成之后这里不在cache里的值就会被删除, 这里的node数量不会受到capacity限制, 可以无限增长.
	nodeMap map[uint64]*lfuNode
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

func (l *lfuCache) getPage(pgId uint64) (p cachePage, found bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cacheVal, ok := l.nodeMap[pgId]
	if !ok {
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
	if v.link.next.freq == v.freq {
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

func (l *lfuCache) getAndLockPage(pgId uint64) (p cachePage, found bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cacheVal, ok := l.nodeMap[pgId]
	if !ok {
		return
	}
	cacheVal.locked = true
	l.upUseCount(cacheVal)
	return cacheVal.val, true
}

func (l *lfuCache) putPage(pgId uint64, p cachePage) bool {
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
		pgId:   pgId,
		val:    p,
		freq:   0,
		locked: false,
		link:   nil,
		prev:   nil,
		next:   nil,
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

func (l *lfuCache) putAndLockPage(pgId uint64, p cachePage) bool {
	return l.doPut(pgId, p)
}

func (l *lfuCache) batchPutPage(itemList []batchPutItem, unlock bool) int {
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
