package sokv

import (
	"github.com/stretchr/testify/require"
	"github.com/zbh255/gocode/random"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func initTest(t *testing.T) {
	err := os.RemoveAll("testdata")
	require.NoError(t, err)
	err = os.Mkdir("testdata", 0644)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}
}

func TestBTree(t *testing.T) {
	initTest(t)
	t.Run("LittleTx", func(t *testing.T) {
		bt := NewBTreeDisk[uint64, string](Config{
			RootDir:                  "testdata",
			Name:                     "testbt.littletx",
			TreeM:                    64,
			MaxPageCacheSize:         1024 * 1024,
			MaxFreeListPageCacheSize: 1024 * 1024,
		})
		bt.SetKeyCodec(new(JsonTypeCodec[uint64]))
		bt.SetValCodec(new(JsonTypeCodec[string]))
		require.NoError(t, bt.Init())
		err := bt.OpenWriteTx(func(tx *Tx[uint64, string]) (err error) {
			for i := 0; i < 1024; i++ {
				_, err = tx.Put(uint64(i), "hello world")
				require.NoError(t, err)
			}
			v, found, err := tx.Get(1023)
			require.NoError(t, err)
			v, found, err = tx.Get(512)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "hello world", v)
			_, err = tx.Put(1024, "hello world")
			require.NoError(t, err)
			v, found, err = tx.Del(1022)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "hello world", v)
			v, found, err = tx.Get(1022)
			require.NoError(t, err)
			require.False(t, found)
			return nil
		})
		require.NoError(t, err)
		err = bt.OpenOnlyReadTx(func(tx *Tx[uint64, string]) (err error) {
			v, found, err := tx.Get(1022)
			require.NoError(t, err)
			require.False(t, found)
			v, found, err = tx.Get(512)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "hello world", v)
			_, err = tx.Put(10010, "hello world")
			require.Error(t, err)
			return nil
		})
		require.NoError(t, err)
	})
	t.Run("BatchLittleTx", func(t *testing.T) {
		bt := NewBTreeDisk[uint64, string](Config{
			RootDir:                  "testdata",
			Name:                     "testbt.batchtx",
			TreeM:                    64,
			MaxPageCacheSize:         1024 * 1024,
			MaxFreeListPageCacheSize: 1024 * 1024,
		})
		bt.SetKeyCodec(new(JsonTypeCodec[uint64]))
		bt.SetValCodec(new(JsonTypeCodec[string]))
		require.NoError(t, bt.Init())
		for i := 0; i < 128; i++ {
			err := bt.OpenWriteTx(func(tx *Tx[uint64, string]) (err error) {
				for j := i * 1024; j < (i+1)*1024; j++ {
					_, err = tx.Put(uint64(j), "hello world")
					require.NoError(t, err)
				}
				return nil
			})
			require.NoError(t, err)
		}
	})
	t.Run("ConcurrentTx", func(t *testing.T) {
		bt := NewBTreeDisk[uint64, string](Config{
			RootDir:                  "testdata",
			Name:                     "testbt.concurrenttx",
			TreeM:                    64,
			MaxPageCacheSize:         1024 * 1024,
			MaxFreeListPageCacheSize: 1024 * 1024,
		})
		bt.SetKeyCodec(new(JsonTypeCodec[uint64]))
		bt.SetValCodec(new(JsonTypeCodec[string]))
		require.NoError(t, bt.Init())
		var (
			wg    sync.WaitGroup
			count atomic.Uint64
		)
		wg.Add(128)
		for i := 0; i < 128; i++ {
			go func() {
				err := bt.OpenWriteTx(func(tx *Tx[uint64, string]) (err error) {
					for j := 0; j < 1024; j++ {
						isReplace, err := tx.Put(count.Add(1), "my is concurrent hello world")
						require.NoError(t, err)
						require.False(t, isReplace)
					}
					return nil
				})
				require.NoError(t, err)
				defer wg.Done()
			}()
		}
		wg.Wait()
	})
	t.Run("BigTx", func(t *testing.T) {
		bt := NewBTreeDisk[uint64, string](Config{
			RootDir:                  "testdata",
			Name:                     "testbt.bigtx",
			TreeM:                    64,
			MaxPageCacheSize:         1024 * 1024,
			MaxFreeListPageCacheSize: 1024 * 1024,
		})
		bt.SetKeyCodec(new(JsonTypeCodec[uint64]))
		bt.SetValCodec(new(JsonTypeCodec[string]))
		require.NoError(t, bt.Init())
		err := bt.OpenWriteTx(func(tx *Tx[uint64, string]) (err error) {
			for i := 0; i < 1024*16; i++ {
				isReplace, err := tx.Put(uint64(i), random.GenStringOnAscii(128))
				require.NoError(t, err)
				require.False(t, isReplace)
			}
			return nil
		})
		require.NoError(t, err)
	})
}
