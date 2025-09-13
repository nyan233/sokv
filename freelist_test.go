package sokv

import (
	"github.com/nyan233/sokv/internal/sys"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

func TestFreelist(t *testing.T) {
	initTest(t)
	f := newFreelist(&pageStorageOption{
		FreelistPath:         path.Join("testdata", "testbt.freelist"),
		PageSize:             uint32(sys.GetSysPageSize()),
		MaxCacheSize:         1024,
		FreelistMaxCacheSize: 1024,
	})
	require.NoError(t, f.init())
	var pgIdList []pageId
	for i := 3; i <= 255; i++ {
		pgIdList = append(pgIdList, createPageIdFromUint64(uint64(i)))
	}
	txh := &txHeader{
		seq:                     1,
		isRead:                  false,
		isRollback:              false,
		isCommit:                false,
		records:                 make([]pageRecord, 0),
		storagePageChangeRecord: make(map[uint64][]pageRecord),
		freePageChangeRecord:    make(map[uint64][]pageRecord),
	}
	require.NoError(t, f.initPageIdList(txh, pgIdList))
	var nextPgId uint64 = 3
	for {
		if nextPgId > 255 {
			break
		}
		p, found, err := f.popOne(txh)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, p.ToUint64(), nextPgId)
		nextPgId++
	}
	_, found, err := f.popOne(txh)
	require.NoError(t, err)
	require.False(t, found)
	for i := 256; i <= 512; i++ {
		err = f.pushOne(txh, createPageIdFromUint64(uint64(i)))
		require.NoError(t, err)
	}
	nextPgId = 256
	for {
		if nextPgId > 510 {
			break
		}
		p, found, err := f.popOne(txh)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, p.ToUint64(), nextPgId)
		nextPgId++
	}

}
