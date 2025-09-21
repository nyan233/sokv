package sokv

import (
	"sync"
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
	putPage(pgId uint64, p cachePage)
	putAndLockPage(pgId uint64, p cachePage)
	batchPutPage(l []batchPutItem, unlock bool)
	cap() int
	len() int
	clean()
}

// 节点结构，用于双向链表
type lfuNode struct {
	key        uint64    // 页面 ID
	value      cachePage // 页面数据
	freq       int       // 访问频率
	locked     bool      // 是否锁定（不可移除）
	prev, next *lfuNode  // 双向链表指针
}

// 频率链表结构，同一频率的节点组成一个双向链表
type freqList struct {
	freq       int      // 频率值
	head, tail *lfuNode // 链表头尾
}

// lfuCache 实现 pageCacheI 接口
type lfuCache struct {
	cache    map[uint64]*lfuNode // 键到节点的映射
	freqMap  map[int]*freqList   // 频率到链表的映射
	minFreq  int                 // 当前最小频率（仅考虑未锁定页面）
	capacity int                 // 缓存容量
	size     int                 // 当前缓存大小（包括锁定和未锁定页面）
	mu       sync.RWMutex        // 读写锁
}

// newLFUCache 创建一个新的 LFU 缓存
func newLFUCache(capacity int) pageCacheI {
	if capacity%64 != 0 {
		panic("capacity must be a multiple of 64")
	}
	return &lfuCache{
		cache:    make(map[uint64]*lfuNode),
		freqMap:  make(map[int]*freqList),
		capacity: capacity,
		size:     0,
		minFreq:  0,
	}
}

// getPage 获取页面并更新频率
func (c *lfuCache) getPage(pgId uint64) (p cachePage, found bool) {
	c.mu.RLock()
	node, exists := c.cache[pgId]
	if !exists {
		c.mu.RUnlock()
		return cachePage{}, false
	}
	c.mu.RUnlock()

	c.mu.Lock()
	// 更新频率（即使锁定也更新频率）
	c.updateFreq(node)
	c.mu.Unlock()

	return node.value, true
}

// getOrLockPage 获取页面并锁定
func (c *lfuCache) getAndLockPage(pgId uint64) (p cachePage, found bool) {
	c.mu.RLock()
	node, exists := c.cache[pgId]
	if !exists {
		c.mu.RUnlock()
		return cachePage{}, false
	}
	c.mu.RUnlock()

	c.mu.Lock()
	// 锁定页面
	node.locked = true
	// 更新频率
	c.updateFreq(node)
	c.mu.Unlock()

	return node.value, true
}

// putPage 插入或更新页面（未锁定）
func (c *lfuCache) putPage(pgId uint64, p cachePage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capacity == 0 {
		return
	}

	// 如果页面已存在，更新值和频率
	if node, exists := c.cache[pgId]; exists {
		node.value = p
		node.locked = false // 确保未锁定
		c.updateFreq(node)
		return
	}

	// 如果缓存已满，移除最低频率的未锁定页面
	if c.size >= c.capacity {
		if !c.removeMinFreq() {
			return // 没有可移除的页面，拒绝插入
		}
	}

	// 创建新节点（未锁定）
	node := &lfuNode{
		key:    pgId,
		value:  p,
		freq:   1,
		locked: false,
	}

	// 添加到缓存
	c.cache[pgId] = node
	c.addToFreqList(node)
	c.size++

	// 更新最小频率
	if c.minFreq == 0 || c.minFreq > 1 {
		c.minFreq = 1
	}
}

// putOrLockPage 插入或更新页面并锁定
func (c *lfuCache) putAndLockPage(pgId uint64, p cachePage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capacity == 0 {
		return
	}

	// 如果页面已存在，更新值和频率
	if node, exists := c.cache[pgId]; exists {
		node.value = p
		node.locked = true // 锁定页面
		c.updateFreq(node)
		return
	}

	// 如果缓存已满，移除最低频率的未锁定页面
	if c.size >= c.capacity {
		if !c.removeMinFreq() {
			return // 没有可移除的页面，拒绝插入
		}
	}

	// 创建新节点（锁定）
	node := &lfuNode{
		key:    pgId,
		value:  p,
		freq:   1,
		locked: true,
	}

	// 添加到缓存
	c.cache[pgId] = node
	c.addToFreqList(node)
	c.size++

	// 更新最小频率
	if c.minFreq == 0 || c.minFreq > 1 {
		c.minFreq = 1
	}
}

// batchPutPage 批量插入页面
func (c *lfuCache) batchPutPage(l []batchPutItem, unlock bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, item := range l {
		if unlock {
			c.putPage(item.pgId, item.val) // 插入未锁定页面
		} else {
			c.putAndLockPage(item.pgId, item.val) // 插入锁定页面
		}
	}
}

// cap 返回缓存容量
func (c *lfuCache) cap() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.capacity
}

// len 返回当前缓存中的页面数
func (c *lfuCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// clean 清空缓存
func (c *lfuCache) clean() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[uint64]*lfuNode)
	c.freqMap = make(map[int]*freqList)
	c.size = 0
	c.minFreq = 0
}

// updateFreq 更新节点的访问频率
func (c *lfuCache) updateFreq(node *lfuNode) {
	// 从旧频率链表中移除
	c.removeFromFreqList(node)

	// 增加频率
	node.freq++
	if node.freq == 1 {
		c.minFreq = 1
	} else if node.freq-1 == c.minFreq && c.freqMap[node.freq-1].head == nil {
		// 寻找新的最小频率（仅考虑未锁定页面）
		c.updateMinFreq()
	}

	// 添加到新频率链表
	c.addToFreqList(node)
}

// addToFreqList 将节点添加到频率链表
func (c *lfuCache) addToFreqList(node *lfuNode) {
	list, exists := c.freqMap[node.freq]
	if !exists {
		list = &freqList{freq: node.freq}
		c.freqMap[node.freq] = list
	}

	node.next = list.head
	if list.head != nil {
		list.head.prev = node
	}
	list.head = node
	if list.tail == nil {
		list.tail = node
	}
}

// removeFromFreqList 从频率链表中移除节点
func (c *lfuCache) removeFromFreqList(node *lfuNode) {
	list := c.freqMap[node.freq]

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		list.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		list.tail = node.prev
	}

	if list.head == nil {
		delete(c.freqMap, node.freq)
	}

	node.prev = nil
	node.next = nil
}

// removeMinFreq 移除最低频率的未锁定页面
func (c *lfuCache) removeMinFreq() bool {
	list := c.freqMap[c.minFreq]
	if list == nil || list.tail == nil {
		return false
	}

	// 从尾部开始查找未锁定的节点
	node := list.tail
	for node != nil && node.locked {
		node = node.prev
	}
	if node == nil {
		return false // 没有未锁定的页面
	}

	// 移除节点
	c.removeFromFreqList(node)
	delete(c.cache, node.key)
	c.size--

	if list.head == nil {
		delete(c.freqMap, c.minFreq)
		// 更新最小频率
		c.updateMinFreq()
	}
	return true
}

// updateMinFreq 更新最小频率（仅考虑未锁定页面）
func (c *lfuCache) updateMinFreq() {
	minVal := int(^uint(0) >> 1) // MaxInt
	for freq, list := range c.freqMap {
		// 检查链表中是否有未锁定节点
		for node := list.head; node != nil; node = node.next {
			if !node.locked {
				if freq < minVal {
					minVal = freq
				}
				break
			}
		}
	}
	c.minFreq = minVal
}
