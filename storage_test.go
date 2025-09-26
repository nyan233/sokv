package sokv

import (
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

func TestStorage(t *testing.T) {
	initTest(t)
	s := newPageStorage(&pageStorageOption{
		DataPath:             path.Join("testdata", "test.storage.dat"),
		FreelistPath:         path.Join("testdata", "test.storage.dat.freelist"),
		MaxCacheSize:         1024 * 1024,
		FreelistMaxCacheSize: 1024 * 1024,
		PageCipher:           nil,
	})
	require.NoError(t, s.init())
	txh := &txHeader{
		seq:                     1,
		isRead:                  false,
		isRollback:              false,
		isCommit:                false,
		records:                 make([]pageRecord, 0),
		storagePageChangeRecord: make(map[uint64][]pageRecord),
		freePageChangeRecord:    make(map[uint64][]pageRecord),
	}
	const AllocCount = defaultPageCount * 256
	pageIds, err := s.allocPage(txh, AllocCount)
	require.NoError(t, err)
	require.Equal(t, int(AllocCount), len(pageIds))
	for i := 0; i < defaultPageCount*16; i++ {
		require.Equal(t, pageIds[i].ToUint64(), uint64(i+3))
	}
}
