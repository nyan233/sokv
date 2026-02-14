package sokv

import (
	"github.com/stretchr/testify/require"
	"math/rand/v2"
	"os"
	"testing"
)

func initBench(b *testing.B) {
	err := os.RemoveAll("testdata")
	require.NoError(b, err)
	err = os.Mkdir("testdata", 0644)
	if err != nil && !os.IsExist(err) {
		b.Fatal(err)
	}
}

func BenchmarkBTreeDisk(b *testing.B) {
	b.Run("PureRead", func(b *testing.B) {
		initBench(b)
		si := func() *BTreeDisk[uint64, string] {
			bt := NewBTreeDisk[uint64, string](Config{
				RootDir:                  "testdata",
				Name:                     "testbt.reopen",
				TreeM:                    128,
				MaxPageCacheSize:         1024 * 1024,
				MaxFreeListPageCacheSize: 1024 * 1024,
				//CipherFactory: func() (Cipher, error) {
				//	const AesKeyHex = "112233445566778899aabbccddeeff11"
				//	key := make([]byte, hex.DecodedLen(len(AesKeyHex)))
				//	_, err := hex.Decode(key, []byte(AesKeyHex))
				//	if err != nil {
				//		return nil, err
				//	}
				//	return NewAseCipher(key)
				//},
			}, new(Uint64Codec), new(JsonTypeCodec[string]))
			require.NoError(b, bt.Init())
			return bt
		}
		bt := si()
		for i := 0; i < 128; i++ {
			err := bt.BeginWriteTx(func(tx *Tx[uint64, string]) (err error) {
				for j := i * 1024; j < (i+1)*1024; j++ {
					_, err = tx.Put(uint64(j), "hello world")
					require.NoError(b, err)
					//minKey, err := tx.MinKey()
					//require.NoError(b, err)
					//maxKey, err := tx.MaxKey()
					//require.NoError(b, err)
					//b.Logf("btree, minKey=%d, maxKey=%d", minKey, maxKey)
				}
				return nil
			})
			require.NoError(b, err)
		}
		//err := bt.BeginOnlyReadTx(func(tx *Tx[uint64, string]) error {
		//	minKey, err := tx.MinKey()
		//	require.NoError(b, err)
		//	return tx.Range(minKey, func(k uint64, v string) bool {
		//		b.Logf("range, key=%d, value=%s", k, v)
		//		return true
		//	})
		//})
		//require.NoError(b, err)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := bt.BeginOnlyReadTx(func(tx *Tx[uint64, string]) (err error) {
					n := rand.Uint64N(128*1024 - 1)
					_, found, err := tx.Get(n)
					require.NoError(b, err)
					require.True(b, found)
					// require.EqualValues(b, v, "hello world")
					return nil
				})
				require.NoError(b, err)
			}
		})
		//for i := 0; i < b.N; i++ {
		//	err := bt.BeginOnlyReadTx(func(tx *Tx[uint64, string]) (err error) {
		//		n := rand.Uint64N(128*1024 - 1)
		//		_, found, err := tx.Get(n)
		//		require.NoError(b, err)
		//		require.True(b, found)
		//		// require.EqualValues(b, v, "hello world")
		//		return nil
		//	})
		//	require.NoError(b, err)
		//}
	})
}
