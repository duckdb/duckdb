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
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
struct StorageLockInternals;

enum class StorageLockType { SHARED = 0, EXCLUSIVE = 1 };

class StorageLockKey {
public:
	StorageLockKey(shared_ptr<StorageLockInternals> internals, StorageLockType type);
	~StorageLockKey();

	StorageLockType GetType() const {
		return type;
	}

private:
	shared_ptr<StorageLockInternals> internals;
	StorageLockType type;
};

class StorageLock {
public:
	StorageLock();
	~StorageLock();

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
	shared_ptr<StorageLockInternals> internals;
};

} // namespace duckdb
