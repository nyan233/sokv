package sokv

import (
	cmap "github.com/zbh255/gocode/container/map"
)

type pageCache struct {
	c         map[uint64]pageDesc
	useCount  *cmap.BTreeMap[uint64, *[]uint64]
	dirtyPage *cmap.BTreeMap[uint64, pageDesc]
	s         *mmapPageStorage
}

func newPageCache(maxPage int) *pageCache {
	if maxPage%64 != 0 {
		panic("maxPage must be a multiple of 64")
	}
	return &pageCache{
		c:        make(map[uint64]pageDesc, maxPage),
		useCount: cmap.NewBtreeMap[uint64, *[]uint64](64),
	}
}

func (cache *pageCache) read(pgId uint64) (pageDesc, bool, error) {
	dirtyPage, ok := cache.dirtyPage.LoadOk(pgId)
	if ok {
		return dirtyPage, ok, nil
	}
	cachePage, ok := cache.c[pgId]
	if ok {
		return cachePage, ok, nil
	}
	cache.s.readPage(createPageIdFromUint64(pgId))
}

// 每个写必须关联一个事务
func (cache *pageCache) write(desc pageDesc) {
	if cache.dirtyPage == nil {
		cache.dirtyPage = cmap.NewBtreeMap[uint64, pageDesc](32)
	}
	cache.dirtyPage.StoreOk(desc.Header.PgId.ToUint64(), desc)
}

func (cache *pageCache) flushAllDirtyPage() error {
	cache.dirtyPage.Range(0, func(key uint64, val pageDesc) bool {
		return true
	})
	return nil
}
