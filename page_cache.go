package sokv

import (
	"fmt"
	cmap "github.com/zbh255/gocode/container/map"
)

type cachePage struct {
	txSeq uint64
	data  []byte
	pgId  pageId
}

type pageCache struct {
	c         map[uint64]cachePage
	useCount  *cmap.BTreeMap[uint64, *[]uint64]
	dirtyPage *cmap.BTreeMap[uint64, cachePage]
}

func newPageCache(maxPage int) *pageCache {
	if maxPage%64 != 0 {
		panic("maxPage must be a multiple of 64")
	}
	return &pageCache{
		c:         make(map[uint64]cachePage, maxPage),
		useCount:  cmap.NewBtreeMap[uint64, *[]uint64](64),
		dirtyPage: cmap.NewBtreeMap[uint64, cachePage](64),
	}
}

func (cache *pageCache) readPage(pgId uint64) (cachePage, bool) {
	pd, ok := cache.dirtyPage.LoadOk(pgId)
	if ok {
		return pd, ok
	}
	pd, ok = cache.c[pgId]
	return pd, ok
}

func (cache *pageCache) setReadValue(pd cachePage) {
	pgId := pd.pgId.ToUint64()
	_, ok := cache.c[pgId]
	if ok {
		panic(fmt.Errorf("dup set cache value : %d", pgId))
	}
	cache.c[pd.pgId.ToUint64()] = pd
}

func (cache *pageCache) setDirtyPage(pd cachePage) bool {
	pgId := pd.pgId.ToUint64()
	delete(cache.c, pgId)
	return cache.dirtyPage.StoreOk(pgId, pd)
}

func (cache *pageCache) rangeAndClearDirtyPage(fn func(pgId uint64, pd cachePage) bool) {
	cache.dirtyPage.Range(cache.dirtyPage.MinKey(), fn)
	cache.dirtyPage = cmap.NewBtreeMap[uint64, cachePage](64)
}

func (cache *pageCache) delAllDirtyPage() {
	cache.rangeAndClearDirtyPage(func(pgId uint64, pd cachePage) bool {
		_, ok := cache.c[pgId]
		if ok {
			delete(cache.c, pgId)
		}
		return true
	})
}
