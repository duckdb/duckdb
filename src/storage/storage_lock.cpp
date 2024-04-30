#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

StorageLockKey::StorageLockKey(StorageLock &lock, StorageLockType type) : lock(lock), type(type) {
}

StorageLockKey::~StorageLockKey() {
	if (type == StorageLockType::EXCLUSIVE) {
		lock.ReleaseExclusiveLock();
	} else {
		D_ASSERT(type == StorageLockType::SHARED);
		lock.ReleaseSharedLock();
	}
}

StorageLock::StorageLock() : read_count(0) {
}

unique_ptr<StorageLockKey> StorageLock::GetExclusiveLock() {
	exclusive_lock.lock();
	while (read_count != 0) {
	}
	return make_uniq<StorageLockKey>(*this, StorageLockType::EXCLUSIVE);
}

unique_ptr<StorageLockKey> StorageLock::TryGetExclusiveLock() {
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
	return make_uniq<StorageLockKey>(*this, StorageLockType::EXCLUSIVE);
}

unique_ptr<StorageLockKey> StorageLock::GetSharedLock() {
	exclusive_lock.lock();
	read_count++;
	exclusive_lock.unlock();
	return make_uniq<StorageLockKey>(*this, StorageLockType::SHARED);
}

unique_ptr<StorageLockKey> StorageLock::TryUpgradeCheckpointLock(StorageLockKey &lock) {
	if (lock.type != StorageLockType::SHARED) {
		throw InternalException("StorageLock::TryUpgradeLock called on an exclusive lock");
	}
	exclusive_lock.lock();
	if (read_count != 1) {
		// other shared locks are active: failed to upgrade
		D_ASSERT(read_count != 0);
		exclusive_lock.unlock();
		return nullptr;
	}
	// no other shared locks active: success!
	return make_uniq<StorageLockKey>(*this, StorageLockType::EXCLUSIVE);
}

void StorageLock::ReleaseExclusiveLock() {
	exclusive_lock.unlock();
}

void StorageLock::ReleaseSharedLock() {
	read_count--;
}

} // namespace duckdb
