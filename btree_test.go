package sokv

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func initTest(t *testing.T) {
	err := os.Mkdir("testdata", 0644)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}
}

func TestBTree(t *testing.T) {
	initTest(t)
	bt := NewBTreeDisk("testdata/test.db", 64)
	bt.OpenWriteTx(func(tx *Tx) error {
		return nil
	})
}

func TestBTreeSearch(t *testing.T) {
	initTest(t)
	bt := NewBTreeDisk("testdata/test.db", 64)
	require.NoError(t, bt.Init())
	minKey, err := bt.minKey()
	require.NoError(t, err)
	maxKey, err := bt.maxKey()
	require.NoError(t, err)
	t.Logf("minKey : %d", binary.BigEndian.Uint64(minKey))
	t.Logf("maxKey : %d", binary.BigEndian.Uint64(maxKey))
	get, b, err := bt.get(binary.BigEndian.AppendUint64(nil, uint64(1024)))
	assert.NoError(t, err)
	assert.Equal(t, b, true)
	t.Log(get)
}
