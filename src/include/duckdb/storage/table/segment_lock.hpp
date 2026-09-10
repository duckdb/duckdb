//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/segment_lock.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"

namespace duckdb {

struct DUCKDB_CAPABILITY("mutex") DUCKDB_SCOPED_CAPABILITY SegmentLock {
public:
	SegmentLock() {
	}
	explicit SegmentLock(annotated_mutex &lock) DUCKDB_ACQUIRE(lock) : lock(lock) {
	}
	~SegmentLock() DUCKDB_RELEASE() = default;
	// disable copy constructors
	SegmentLock(const SegmentLock &other) = delete;
	SegmentLock &operator=(const SegmentLock &) = delete;
	//! enable move constructors
	SegmentLock(SegmentLock &&other) noexcept {
		std::swap(lock, other.lock);
	}
	SegmentLock &operator=(SegmentLock &&other) noexcept {
		std::swap(lock, other.lock);
		return *this;
	}

	void Release() DUCKDB_RELEASE() {
		lock.unlock();
	}

	//! Verify ownership when a lock is passed through an iterator or another helper.
	void AssertHeld() const DUCKDB_ASSERT_CAPABILITY(this) {
		D_ASSERT(lock.owns_lock());
	}
	void AssertHeld(const annotated_mutex &mutex) const DUCKDB_ASSERT_CAPABILITY(mutex) {
		D_ASSERT(lock.owns_lock());
		D_ASSERT(lock.mutex() == &mutex);
	}

private:
	unique_lock<mutex> lock;
};

} // namespace duckdb
