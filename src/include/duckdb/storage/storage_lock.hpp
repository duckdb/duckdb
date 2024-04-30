//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_lock.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class StorageLock;

enum class StorageLockType { SHARED = 0, EXCLUSIVE = 1 };

class StorageLockKey {
	friend class StorageLock;

public:
	StorageLockKey(StorageLock &lock, StorageLockType type);
	~StorageLockKey();

private:
	StorageLock &lock;
	StorageLockType type;
};

class StorageLock {
	friend class StorageLockKey;

public:
	StorageLock();

	//! Get an exclusive lock
	unique_ptr<StorageLockKey> GetExclusiveLock();
	//! Get a shared lock
	unique_ptr<StorageLockKey> GetSharedLock();
	//! Try to get an exclusive lock - if we cannot get it immediately we return `nullptr`
	unique_ptr<StorageLockKey> TryGetExclusiveLock();
	//! This is a special method that only exists for checkpointing
	//! This method takes a shared lock, and returns an exclusive lock if the parameter is the only active shared lock
	//! If this method succeeds, we have **both** a shared and exclusive lock active (which normally is not allowed)
	//! But this behavior is required for checkpointing
	unique_ptr<StorageLockKey> TryUpgradeCheckpointLock(StorageLockKey &lock);

private:
	mutex exclusive_lock;
	atomic<idx_t> read_count;

private:
	//! Release an exclusive lock
	void ReleaseExclusiveLock();
	//! Release a shared lock
	void ReleaseSharedLock();
};

} // namespace duckdb
