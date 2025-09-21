package sokv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPageCache(t *testing.T) {
	c := newLFUCache(1024)
	for i := 0; i < 1024; i++ {
		c.putPage(uint64(i), cachePage{
			txSeq: uint64(i),
			data:  make([]byte, i),
			pgId:  createPageIdFromUint64(uint64(i)),
		})
	}
	for i := 0; i < 1024; i++ {
		cacheVal, found := c.getPage(uint64(i))
		require.True(t, found)
		require.Equal(t, cacheVal.txSeq, uint64(i))
		require.Equal(t, cacheVal.pgId.ToUint64(), uint64(i))
		require.Equal(t, cacheVal.data, make([]byte, i))
	}
}
