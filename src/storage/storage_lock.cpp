#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/thread_annotation/thread_annotation.hpp"

#include <shared_mutex>

namespace duckdb {

struct StorageLockInternals : enable_shared_from_this<StorageLockInternals> {
public:
	std::shared_mutex lock;

public:
	unique_ptr<StorageLockKey> GetExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		lock.lock();
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	unique_ptr<StorageLockKey> GetSharedLock() {
		lock.lock_shared();
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::SHARED);
	}

	unique_ptr<StorageLockKey> TryGetExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		if (!lock.try_lock()) {
			return nullptr;
		}
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	void ReleaseExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		lock.unlock();
	}
	void ReleaseSharedLock() {
		lock.unlock_shared();
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

} // namespace duckdb
