#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

struct StorageLockInternals : enable_shared_from_this<StorageLockInternals> {
public:
	StorageLockInternals() : read_count(0) {
	}

	mutex exclusive_lock;
	atomic<idx_t> read_count;

public:
	unique_ptr<StorageLockKey> GetExclusiveLock() {
		exclusive_lock.lock();
		while (read_count != 0) {
		}
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	unique_ptr<StorageLockKey> GetSharedLock() {
		exclusive_lock.lock();
		read_count++;
		exclusive_lock.unlock();
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::SHARED);
	}

	unique_ptr<StorageLockKey> TryGetExclusiveLock() {
		if (!exclusive_lock.try_lock()) {
			// could not lock mutex
			return nullptr;
		}
		if (read_count != 0) {
			// there are active readers - cannot get exclusive lock
			exclusive_lock.unlock();
			return nullptr;
		}
		// success!
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	unique_ptr<StorageLockKey> TryUpgradeCheckpointLock(StorageLockKey &lock) {
		if (lock.GetType() != StorageLockType::SHARED) {
			throw InternalException("StorageLock::TryUpgradeLock called on an exclusive lock");
		}
		if (!exclusive_lock.try_lock()) {
			// could not lock mutex
			return nullptr;
		}
		if (read_count != 1) {
			// other shared locks are active: failed to upgrade
			D_ASSERT(read_count != 0);
			exclusive_lock.unlock();
			return nullptr;
		}
		// no other shared locks active: success!
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	void ReleaseExclusiveLock() {
		exclusive_lock.unlock();
	}
	void ReleaseSharedLock() {
		read_count--;
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
