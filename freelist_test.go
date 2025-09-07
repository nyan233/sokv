package sokv

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func initTest(t *testing.T) {
	err := os.Mkdir("testdata", 0644)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}
}

func TestFreelistHeap(t *testing.T) {
	initTest(t)
	freelist := newFreelist("testdata/test.db.freelist")
	err := freelist.init()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("testdata/test.db.freelist")
	for i := 766; i < 1024*64; i++ {
		p := createPageIdFromUint64(uint64(i))
		err = freelist.pushOne(p)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 1024; i++ {
		p, found, err := freelist.popOne()
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatal("free list not found")
		}
		require.Equal(t, uint64(i+766), p.ToUint64())
		t.Logf("pop pageId : %d", p.ToUint64())
	}
}
