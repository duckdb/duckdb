#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/thread_annotation/thread_annotation.hpp"

#include <condition_variable>

namespace duckdb {

// A writer-priority read-write lock. Exclusive holders wait on a condition variable for all readers to drain
// (instead of busy-spinning); new readers are held off while a writer is active or waiting, so writers cannot be
// starved by a steady stream of readers.
struct StorageLockInternals : enable_shared_from_this<StorageLockInternals> {
public:
	StorageLockInternals() : read_count(0), writer_active(false), writers_waiting(0) {
	}

	mutex state_lock;
	std::condition_variable state_cv;
	//! Number of active shared (read) lock holders
	idx_t read_count;
	//! Whether an exclusive (write) lock is currently held
	bool writer_active;
	//! Number of writers waiting for the exclusive lock (for writer priority)
	idx_t writers_waiting;

public:
	unique_ptr<StorageLockKey> GetExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		unique_lock<mutex> guard(state_lock);
		writers_waiting++;
		state_cv.wait(guard, [&]() { return !writer_active && read_count == 0; });
		writers_waiting--;
		writer_active = true;
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	unique_ptr<StorageLockKey> GetSharedLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		unique_lock<mutex> guard(state_lock);
		// writer priority: do not admit a new reader while a writer is active or waiting
		state_cv.wait(guard, [&]() { return !writer_active && writers_waiting == 0; });
		read_count++;
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::SHARED);
	}

	unique_ptr<StorageLockKey> TryGetExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		lock_guard<mutex> guard(state_lock);
		if (writer_active || read_count != 0) {
			// a writer or readers are active - cannot get the exclusive lock immediately
			return nullptr;
		}
		writer_active = true;
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	unique_ptr<StorageLockKey> TryUpgradeCheckpointLock(StorageLockKey &lock) DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		if (lock.GetType() != StorageLockType::SHARED) {
			throw InternalException("StorageLock::TryUpgradeLock called on an exclusive lock");
		}
		lock_guard<mutex> guard(state_lock);
		if (writer_active || read_count != 1) {
			// other shared locks (or a writer) are active: failed to upgrade
			D_ASSERT(read_count != 0);
			return nullptr;
		}
		// we are the only reader: grant the exclusive lock in addition to the held shared lock (read_count stays 1)
		writer_active = true;
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	void ReleaseExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		{
			lock_guard<mutex> guard(state_lock);
			writer_active = false;
		}
		state_cv.notify_all();
	}
	void ReleaseSharedLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		bool notify;
		{
			lock_guard<mutex> guard(state_lock);
			read_count--;
			notify = read_count == 0;
		}
		if (notify) {
			// the last reader left - wake a waiting writer
			state_cv.notify_all();
		}
	}
};

StorageLockKey::StorageLockKey(shared_ptr<StorageLockInternals> internals_p, StorageLockType type)
    : internals(std::move(internals_p)), type(type) {
}

StorageLockKey::~StorageLockKey() {
	if (type == StorageLockType::EXCLUSIVE) {
		internals->ReleaseExclusiveLock();
	} else {
		D_ASSERT(type == StorageLockType::SHARED);
		internals->ReleaseSharedLock();
	}
}

StorageLock::StorageLock() : internals(make_shared_ptr<StorageLockInternals>()) {
}
StorageLock::~StorageLock() {
}

unique_ptr<StorageLockKey> StorageLock::GetExclusiveLock() {
	return internals->GetExclusiveLock();
}

unique_ptr<StorageLockKey> StorageLock::TryGetExclusiveLock() {
	return internals->TryGetExclusiveLock();
}

unique_ptr<StorageLockKey> StorageLock::GetSharedLock() {
	return internals->GetSharedLock();
}

unique_ptr<StorageLockKey> StorageLock::TryUpgradeCheckpointLock(StorageLockKey &lock) {
	return internals->TryUpgradeCheckpointLock(lock);
}

} // namespace duckdb
