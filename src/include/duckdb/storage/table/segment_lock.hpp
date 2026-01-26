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

struct SegmentLock {
public:
	SegmentLock() {
	}
	explicit SegmentLock(mutex &mu) : lock(mu) {
	}
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

	void Release() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		lock.unlock();
	}

private:
	unique_lock<mutex> lock;
};

} // namespace duckdb
