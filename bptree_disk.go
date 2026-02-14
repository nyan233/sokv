package sokv

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

type BPTreeDisk struct {
}
