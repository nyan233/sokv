package sokv

import (
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

func TestFreelist(t *testing.T) {
	initTest(t)
	f := newFreelist2(&pageStorageOption{
		FreelistPath:         path.Join("testdata", "testbt.freelist"),
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
	require.NoError(t, f.build(txh, pgIdList))
	var nextPgId uint64 = 3
	for {
		if nextPgId > 255 {
			break
		}
		p, err := f.pop(txh)
		require.NoError(t, err)
		require.Equal(t, p.ToUint64(), nextPgId)
		nextPgId++
	}
	_, err := f.pop(txh)
	require.Equal(t, err, errNoAvailablePage)
	for i := 256; i <= 512; i++ {
		err = f.push(txh, createPageIdFromUint64(uint64(i)))
		require.NoError(t, err)
	}
	nextPgId = 256
	for {
		if nextPgId > 510 {
			break
		}
		p, err := f.pop(txh)
		require.NoError(t, err)
		require.Equal(t, p.ToUint64(), nextPgId)
		nextPgId++
	}

}
