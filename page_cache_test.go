package sokv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPageCache(t *testing.T) {
	createReadTx := func() *txHeader {
		return &txHeader{
			seq:        0,
			isRead:     true,
			isRollback: false,
			isCommit:   false,
		}
	}
	createWriteTx := func() *txHeader {
		return &txHeader{
			seq:        1,
			isRead:     false,
			isRollback: false,
			isCommit:   false,
		}
	}
	readTx := createReadTx()
	writeTx := createWriteTx()
	c := newLFUCache(1024)
	for i := 0; i < 1024; i++ {
		c.putPage(readTx, uint64(i), cachePage{
			txSeq: uint64(i),
			data:  make([]byte, i),
			pgId:  createPageIdFromUint64(uint64(i)),
		})
	}
	for i := 0; i < 1024; i++ {
		cacheVal, found := c.getPage(readTx, uint64(i))
		require.True(t, found)
		require.Equal(t, cacheVal.txSeq, uint64(i))
		require.Equal(t, cacheVal.pgId.ToUint64(), uint64(i))
		require.Equal(t, cacheVal.data, make([]byte, i))
	}
	c.createShadowPage(writeTx)
	f, ok := c.getPage(writeTx, 1023)
	require.True(t, ok)
	c.opShadowPage(writeTx, f, func(p cachePage) (cachePage, bool) {
		p.data[0] = 100
		p.data[1] = 102
		p.data[2] = 103
		p.data[3] = 104
		return p, true
	})
	readP, ok := c.getPage(readTx, 1023)
	require.True(t, ok)
	writeReadP, ok := c.getPage(writeTx, 1023)
	require.True(t, ok)
	require.Equal(t, readP.data[:4], []byte{0, 0, 0, 0})
	require.Equal(t, writeReadP.data[:4], []byte{100, 102, 103, 104})
}
