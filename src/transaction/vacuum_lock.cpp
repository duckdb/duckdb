#include "duckdb/transaction/vacuum_lock.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"

#include <condition_variable>

namespace duckdb {

struct VacuumLockInternals : enable_shared_from_this<VacuumLockInternals> {
public:
	unique_ptr<VacuumLockKey> GetSharedLock() {
		unique_lock<mutex> guard(lock);
		condition.wait(guard, [&] { return !exclusive; });
		shared_count++;
		return make_uniq<VacuumLockKey>(shared_from_this(), VacuumLockType::SHARED);
	}

	unique_ptr<VacuumLockKey> TryGetExclusiveLock() {
		lock_guard<mutex> guard(lock);
		if (exclusive || shared_count != 0) {
			return nullptr;
		}
		exclusive = true;
		return make_uniq<VacuumLockKey>(shared_from_this(), VacuumLockType::EXCLUSIVE);
	}

	void ReleaseSharedLock() {
		lock_guard<mutex> guard(lock);
		D_ASSERT(shared_count > 0);
		shared_count--;
		if (shared_count == 0) {
			condition.notify_all();
		}
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

VacuumLockKey::VacuumLockKey(shared_ptr<VacuumLockInternals> internals_p, VacuumLockType type_p)
    : internals(std::move(internals_p)), type(type_p) {
}

VacuumLockKey::~VacuumLockKey() {
	if (type == VacuumLockType::EXCLUSIVE) {
		internals->ReleaseExclusiveLock();
	} else {
		D_ASSERT(type == VacuumLockType::SHARED);
		internals->ReleaseSharedLock();
	}
}

VacuumLockCoordinator::VacuumLockCoordinator() : internals(make_shared_ptr<VacuumLockInternals>()) {
}

VacuumLockCoordinator::~VacuumLockCoordinator() {
}

unique_ptr<VacuumLockKey> VacuumLockCoordinator::GetSharedLock() {
	return internals->GetSharedLock();
}

unique_ptr<VacuumLockKey> VacuumLockCoordinator::TryGetExclusiveLock() {
	return internals->TryGetExclusiveLock();
}

} // namespace duckdb
