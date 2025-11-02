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
	readTxHeader := newTxHeader()
	readTxHeader.seq = 0
	readTxHeader.isRead = true
	readTxHeader.isRollback = false
	readTxHeader.isCommit = false

	writeTxHeader := newTxHeader()
	writeTxHeader.seq = 1
	writeTxHeader.isRead = false
	writeTxHeader.isRollback = false
	writeTxHeader.isCommit = false
	s.cache.createShadowPage(writeTxHeader)

	p, err := s.readPage(writeTxHeader, createPageIdFromUint64(3))
	require.NoError(t, err)
	require.Equal(t, p.idOfU8(), uint64(0))
	p.Header.Header = pageHeaderDat
	p.Header.PgId = createPageIdFromUint64(3)
	err = s.modifyPage(writeTxHeader, p, func(p2 pageDesc) (pageDesc, error) {
		p2.Data[0] = 1
		p2.Data[1] = 2
		p2.Data[2] = 3
		p2.Data[3] = 4
		return p2, nil
	})
	require.NoError(t, err)
	readP, err := s.readPage(readTxHeader, createPageIdFromUint64(3))
	require.NoError(t, err)
	require.Equal(t, int(readP.Data[0]), 0)
	require.Equal(t, int(readP.Data[1]), 0)
	require.Equal(t, int(readP.Data[2]), 0)
	require.Equal(t, int(readP.Data[3]), 0)
	writeP, err := s.readPage(writeTxHeader, createPageIdFromUint64(3))
	require.NoError(t, err)
	require.Equal(t, int(writeP.Data[0]), 1)
	require.Equal(t, int(writeP.Data[1]), 2)
	require.Equal(t, int(writeP.Data[2]), 3)
	require.Equal(t, int(writeP.Data[3]), 4)
}
