#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation/thread_annotation.hpp"

#include <condition_variable>
#include <shared_mutex>

namespace duckdb {

struct StorageLockInternals : enable_shared_from_this<StorageLockInternals> {
public:
	unique_ptr<StorageLockKey> GetExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		WaitForExclusiveAccess();
		lock.lock();
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	unique_ptr<StorageLockKey> GetSharedLock() {
		{
			unique_lock<mutex> guard(gate_lock);
			gate_condition.wait(guard, [&] { return !writer_active && waiting_writers == 0; });
			active_readers++;
		}
		lock.lock_shared();
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::SHARED);
	}

	unique_ptr<StorageLockKey> TryGetExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		{
			lock_guard<mutex> guard(gate_lock);
			if (writer_active || waiting_writers != 0 || active_readers != 0) {
				return nullptr;
			}
			writer_active = true;
		}
		if (!lock.try_lock()) {
			lock_guard<mutex> guard(gate_lock);
			writer_active = false;
			gate_condition.notify_all();
			return nullptr;
		}
		return make_uniq<StorageLockKey>(shared_from_this(), StorageLockType::EXCLUSIVE);
	}

	void ReleaseExclusiveLock() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		lock.unlock();
		lock_guard<mutex> guard(gate_lock);
		D_ASSERT(writer_active);
		writer_active = false;
		gate_condition.notify_all();
	}

	void ReleaseSharedLock() {
		lock.unlock_shared();
		lock_guard<mutex> guard(gate_lock);
		D_ASSERT(active_readers > 0);
		active_readers--;
		if (active_readers == 0) {
			gate_condition.notify_all();
		}
	}

private:
	void WaitForExclusiveAccess() {
		unique_lock<mutex> guard(gate_lock);
		waiting_writers++;
		gate_condition.wait(guard, [&] { return !writer_active && active_readers == 0; });
		waiting_writers--;
		writer_active = true;
	}

private:
	std::shared_mutex lock;
	mutex gate_lock;
	std::condition_variable gate_condition;
	idx_t active_readers = 0;
	idx_t waiting_writers = 0;
	bool writer_active = false;
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
