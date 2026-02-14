package sokv

import "sync/atomic"

type ExportStat struct {
	StorageCacheHit  uint64
	StorageCacheMis  uint64
	FreelistCacheHit uint64
	FreelistCacheMis uint64
	TxCommitSumTs    uint64
}

type iStat struct {
	storageCacheHit   atomic.Uint64
	storageCacheMis   atomic.Uint64
	freelistCacheHit  atomic.Uint64
	freelistCacheMis  atomic.Uint64
	txCommitMaxTime   atomic.Uint64
	txCommitMinTime   atomic.Uint64
	txCommitSumTs     atomic.Uint64
	txCommitCount     atomic.Uint64
	txRollbackCount   atomic.Uint64
	txRollbackMaxTime atomic.Uint64
	txRollbackMinTime atomic.Uint64
}
