#include "duckdb/transaction/checkpoint_lock.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"

#include <condition_variable>

namespace duckdb {

struct CheckpointLockInternals : enable_shared_from_this<CheckpointLockInternals> {
public:
	unique_ptr<CheckpointLockKey> GetSharedLock() {
		unique_lock<mutex> guard(lock);
		condition.wait(guard, [&] { return !exclusive; });
		shared_count++;
		return make_uniq<CheckpointLockKey>(shared_from_this(), CheckpointLockType::SHARED);
	}

	unique_ptr<CheckpointLockKey> TryGetExclusiveLock() {
		lock_guard<mutex> guard(lock);
		if (exclusive || shared_count != 0) {
			return nullptr;
		}
		exclusive = true;
		return make_uniq<CheckpointLockKey>(shared_from_this(), CheckpointLockType::EXCLUSIVE);
	}

	unique_ptr<CheckpointLockKey> TryUpgrade(CheckpointLockKey &key) {
		if (key.type != CheckpointLockType::SHARED || key.internals.get() != this) {
			throw InternalException("CheckpointLockCoordinator::TryUpgrade called with an invalid shared lock");
		}
		lock_guard<mutex> guard(lock);
		if (exclusive || shared_count != 1) {
			return nullptr;
		}
		exclusive = true;
		return make_uniq<CheckpointLockKey>(shared_from_this(), CheckpointLockType::EXCLUSIVE);
	}

	void ReleaseSharedLock() {
		lock_guard<mutex> guard(lock);
		D_ASSERT(shared_count > 0);
		shared_count--;
	}

	void ReleaseExclusiveLock() {
		lock_guard<mutex> guard(lock);
		D_ASSERT(exclusive);
		exclusive = false;
		condition.notify_all();
	}

private:
	mutex lock;
	std::condition_variable condition;
	idx_t shared_count = 0;
	bool exclusive = false;
};

CheckpointLockKey::CheckpointLockKey(shared_ptr<CheckpointLockInternals> internals_p, CheckpointLockType type_p)
    : internals(std::move(internals_p)), type(type_p) {
}

CheckpointLockKey::~CheckpointLockKey() {
	if (type == CheckpointLockType::EXCLUSIVE) {
		internals->ReleaseExclusiveLock();
	} else {
		D_ASSERT(type == CheckpointLockType::SHARED);
		internals->ReleaseSharedLock();
	}
}

CheckpointLockCoordinator::CheckpointLockCoordinator() : internals(make_shared_ptr<CheckpointLockInternals>()) {
}

CheckpointLockCoordinator::~CheckpointLockCoordinator() {
}

unique_ptr<CheckpointLockKey> CheckpointLockCoordinator::GetSharedLock() {
	return internals->GetSharedLock();
}

unique_ptr<CheckpointLockKey> CheckpointLockCoordinator::TryGetExclusiveLock() {
	return internals->TryGetExclusiveLock();
}

unique_ptr<CheckpointLockKey> CheckpointLockCoordinator::TryUpgrade(CheckpointLockKey &lock) {
	return internals->TryUpgrade(lock);
}

} // namespace duckdb
