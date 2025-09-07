//go:build unix

package sys

import (
	"golang.org/x/sys/unix"
	"os"
	"runtime"
	"syscall"
)

func MMap(file *os.File, length uint64) (dat []byte, err error) {
	dat, err = unix.Mmap(int(file.Fd()), 0, int(length), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return
}

func MUnmap(file *os.File, dat []byte) (err error) {
	return unix.Munmap(dat)
}

func Remap(file *os.File, newLength uint64, olddat []byte) (dat []byte, err error) {
	if runtime.GOOS == "linux" {
		dat, err = unix.Mremap(olddat, int(newLength), syscall.MAP_PRIVATE)
		return
	} else {
		err = MUnmap(file, olddat)
		if err != nil {
			return
		}
		dat, err = MMap(file, newLength)
		return
	}
}

func GetSysPageSize() int {
	return unix.Getpagesize()
}

func MemLock(dat []byte) (err error) {
	return nil
}

func MemUnlock(dat []byte) (err error) {
	return nil
}
