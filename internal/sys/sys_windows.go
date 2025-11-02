//go:build windows

package sys

import (
	"golang.org/x/sys/windows"
	"os"
	"unsafe"
)

// Windows API constants not defined in golang.org/x/sys/windows
const (
	FILE_MAP_ALL_ACCESS = 0x000F001F // Combines all mapping permissions (read, write, execute, etc.)
)

// SYSTEM_INFO defines the Windows SYSTEM_INFO structure.
type SYSTEM_INFO struct {
	ProcessorArchitecture     uint16
	Reserved                  uint16
	PageSize                  uint32
	MinimumApplicationAddress uintptr
	MaximumApplicationAddress uintptr
	ActiveProcessorMask       uintptr
	NumberOfProcessors        uint32
	ProcessorType             uint32
	AllocationGranularity     uint32
	ProcessorLevel            uint16
	ProcessorRevision         uint16
}

// getSystemInfoProc is the lazy-loaded GetSystemInfo function.
var getSystemInfoProc *windows.LazyProc

// mapViewOfFileExProc is the lazy-loaded MapViewOfFileEx function.
var mapViewOfFileExProc *windows.LazyProc

func init() {
	// Load GetSystemInfo and MapViewOfFileEx from kernel32.dll
	getSystemInfoProc = windows.NewLazySystemDLL("kernel32").NewProc("GetSystemInfo")
	mapViewOfFileExProc = windows.NewLazySystemDLL("kernel32").NewProc("MapViewOfFileEx")
}

// GetSystemInfo retrieves system information.
func GetSystemInfo() (si SYSTEM_INFO, err error) {
	r1, _, err := getSystemInfoProc.Call(uintptr(unsafe.Pointer(&si)))
	if r1 == 0 {
		return si, err
	}
	return si, nil
}

// MapViewOfFileEx maps a file view with a specified base address.
func MapViewOfFileEx(hMap windows.Handle, desiredAccess uint32, fileOffsetHigh, fileOffsetLow uint32, size uintptr, baseAddr uintptr) (addr uintptr, err error) {
	r1, _, err := mapViewOfFileExProc.Call(
		uintptr(hMap),
		uintptr(desiredAccess),
		uintptr(fileOffsetHigh),
		uintptr(fileOffsetLow),
		size,
		baseAddr,
	)
	if r1 == 0 {
		return 0, err
	}
	return r1, nil
}

// MMap maps a file into memory, similar to Unix mmap.
// It attempts to map the file with read and write permissions.
func MMap(file *os.File, length uint64) (dat []byte, err error) {
	// Create file mapping object
	hFile := windows.Handle(file.Fd())
	hMap, err := windows.CreateFileMapping(
		hFile,
		nil,
		windows.PAGE_READWRITE,
		uint32(length>>32), // High-order 32 bits of size
		uint32(length),     // Low-order 32 bits of size
		nil,
	)
	if err != nil {
		return nil, err
	}

	// Map the file into memory
	addr, err := windows.MapViewOfFile(
		hMap,
		FILE_MAP_ALL_ACCESS,
		0, // File offset high
		0, // File offset low
		uintptr(length),
	)
	if err != nil {
		windows.CloseHandle(hMap)
		return nil, err
	}

	// Store the mapping handle in the slice's capacity to clean it up later
	// We use a slice header to manage the mapped memory
	dat = (*[1 << 48]byte)(unsafe.Pointer(addr))[:length:length]

	// close the mapping handle (Windows keeps it open until all views are unmapped)
	windows.CloseHandle(hMap)

	return dat, nil
}

// MUnmap unmaps the memory region, similar to Unix munmap.
func MUnmap(file *os.File, dat []byte) (err error) {
	if len(dat) == 0 {
		return nil
	}
	// Unmap the view
	addr := uintptr(unsafe.Pointer(&dat[0]))
	return windows.UnmapViewOfFile(addr)
}

// Remap adjusts the size of an existing memory mapping, attempting to keep the original base address.
func Remap(file *os.File, newLength uint64, olddat []byte) (dat []byte, err error) {
	if len(olddat) == 0 {
		return MMap(file, newLength)
	}

	// Store the original base address
	baseAddr := uintptr(unsafe.Pointer(&olddat[0]))

	// Unmap the old mapping
	err = MUnmap(file, olddat)
	if err != nil {
		return nil, err
	}

	// Create a new file mapping object with the new size
	hFile := windows.Handle(file.Fd())
	hMap, err := windows.CreateFileMapping(
		hFile,
		nil,
		windows.PAGE_READWRITE,
		uint32(newLength>>32),
		uint32(newLength),
		nil,
	)
	if err != nil {
		return nil, err
	}

	// Attempt to remap at the original address
	addr, err := MapViewOfFileEx(
		hMap,
		FILE_MAP_ALL_ACCESS,
		0, // File offset high
		0, // File offset low
		uintptr(newLength),
		baseAddr, // Try to use the original address
	)
	if err != nil {
		// If mapping at the original address fails, fall back to system-allocated address
		addr, err = windows.MapViewOfFile(
			hMap,
			FILE_MAP_ALL_ACCESS,
			0,
			0,
			uintptr(newLength),
		)
		if err != nil {
			windows.CloseHandle(hMap)
			return nil, err
		}
	}

	// Create slice for the new mapping
	dat = (*[1 << 30]byte)(unsafe.Pointer(addr))[:newLength:newLength]

	// close the mapping handle
	windows.CloseHandle(hMap)

	return dat, nil
}

// GetSysPageSize returns the system's memory page size.
func GetSysPageSize() int {
	si, err := GetSystemInfo()
	if err != nil {
		// Fallback to a default page size (4096 is common on Windows)
		return 4096
	}
	return int(si.PageSize)
}

func MemLock(dat []byte) (err error) {
	return nil
}

func MemUnlock(dat []byte) (err error) {
	return nil
}

func OpenFile(path string) (file *os.File, err error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	// 打开文件，设置 Windows 特定的标志
	// FILE_FLAG_NO_BUFFERING: 禁用缓存
	// FILE_FLAG_WRITE_THROUGH: 直接写入磁盘
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE, // 读写权限
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,                 // 默认安全属性
		windows.OPEN_ALWAYS, // 不存在则新建，存在则打开
		windows.FILE_FLAG_NO_BUFFERING|windows.FILE_FLAG_WRITE_THROUGH,
		0, // 无模板文件
	)
	if err != nil {
		return nil, err
	}

	// 将 syscall.Handle 转换为 os.File
	file = os.NewFile(uintptr(handle), path)
	return file, nil
}
