//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_lock.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include <atomic>
#include <mutex>

namespace duckdb {
class StorageLock;

enum class StorageLockType { SHARED = 0, EXCLUSIVE = 1 };

class StorageLockKey {
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

private:
	std::mutex exclusive_lock;
	std::atomic<idx_t> read_count;

private:
	//! Release an exclusive lock
	void ReleaseExclusiveLock();
	//! Release a shared lock
	void ReleaseSharedLock();
};

} // namespace duckdb
