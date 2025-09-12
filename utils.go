package sokv

import "unsafe"

func bytesIsZero(data []byte) bool {
	if len(data)%32 != 0 {
		panic("data is not a multiple of 32")
	}
	var v uint64
	for len(data) > 0 {
		v2 := *(*uint64)(unsafe.Pointer(&data[0]))
		v3 := *(*uint64)(unsafe.Pointer(&data[8]))
		v4 := *(*uint64)(unsafe.Pointer(&data[16]))
		v5 := *(*uint64)(unsafe.Pointer(&data[24]))
		v |= v2
		v |= v3
		v |= v4
		v |= v5
		data = data[32:]
	}
	return v == 0
}
