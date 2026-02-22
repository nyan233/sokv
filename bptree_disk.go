package sokv

import "log/slog"

type bpInternalNode struct {
	typ      uint8
	elems    [][]byte
	subNodes []uint32
}

type bpLeafNodeElem struct {
	key []byte
	val []byte
}

type bpLeafNode struct {
	typ   uint8
	prev  uint32
	next  uint32
	elems []bpLeafNodeElem
}

type bpLeafNodeDiskElemView struct {
	keyLen uint32
	valLen uint32
}

type bpLeafNodeDiskDesc struct {
	pgId         uint32
	pageLink     []pageDesc
	prev         *bpLeafNodeDiskDesc
	next         *bpLeafNodeDiskDesc
	memElems     []bpLeafNodeElem
	elemDiskView []bpLeafNodeDiskElemView
}

type Config struct {
	RootDir                  string
	Name                     string
	MaxPageCacheSize         int
	MaxFreeListPageCacheSize int
	Logger                   *slog.Logger
	CipherFactory            func() (Cipher, error)
	Comparator               func(a, b []byte) int
}

type BPTreeDisk struct {
}
