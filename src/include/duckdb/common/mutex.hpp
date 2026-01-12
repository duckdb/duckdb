//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/mutex.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/thread_annotation/mutex.hpp"

namespace duckdb {
// Annotated mutex implementation.
class DUCKDB_CAPABILITY("mutex") mutex : public internal::mutex_impl_t<mutex> {
private:
	using Impl = internal::mutex_impl_t<mutex>;

public:
	void lock() DUCKDB_ACQUIRE() {
		Impl::lock();
	}
	void unlock() DUCKDB_RELEASE() {
		Impl::unlock();
	}
	bool try_lock() DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock();
	}
};

// Annotated lock_guard implementation.
template <typename M>
class DUCKDB_SCOPED_CAPABILITY lock_guard : public internal::lock_impl_t<lock_guard<M>> {
private:
	using Impl = internal::lock_impl_t<lock_guard<M>>;

public:
	explicit lock_guard(M &m) DUCKDB_ACQUIRE(m) : Impl(m) {
	}
	lock_guard(M &m, std::adopt_lock_t t) DUCKDB_REQUIRES(m) : Impl(m, t) {
	}

	// Disable copy and enable move.
	lock_guard(const lock_guard &) = delete;
	lock_guard &operator=(const lock_guard &) = delete;
	lock_guard(lock_guard &&) = default;
	lock_guard &operator=(lock_guard &&) = default;

	~lock_guard() DUCKDB_RELEASE() = default;
};

// Annotated unique_lock implementation.
template <typename M>
class DUCKDB_SCOPED_CAPABILITY unique_lock : public internal::lock_impl_t<unique_lock<M>> {
private:
	using Impl = internal::lock_impl_t<unique_lock<M>>;

public:
	unique_lock() = default;
	explicit unique_lock(M &m) DUCKDB_ACQUIRE(m) : Impl(m) {
	}
	unique_lock(M &m, std::defer_lock_t t) noexcept DUCKDB_EXCLUDES(m) : Impl(m, t) {
	}
	unique_lock(M &m, std::try_to_lock_t t) DUCKDB_TRY_ACQUIRE(true, m) : Impl(m, t) {
	}
	unique_lock(M &m, std::adopt_lock_t t) DUCKDB_REQUIRES(m) : Impl(m, t) {
	}

	// Disable copy and enable move.
	unique_lock(const unique_lock &) = delete;
	unique_lock &operator=(const unique_lock &) = delete;
	unique_lock(unique_lock &&) = default;
	unique_lock &operator=(unique_lock &&) = default;

	~unique_lock() DUCKDB_RELEASE() = default;

	void lock() DUCKDB_ACQUIRE() {
		Impl::lock();
	}
	bool try_lock() DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock();
	}
	template <typename R, typename P>
	bool try_lock_for(const std::chrono::duration<R, P> &timeout) DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock_for(timeout);
	}
	template <typename C, typename D>
	bool try_lock_until(const std::chrono::time_point<C, D> &timeout) DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock_until(timeout);
	}
	void unlock() DUCKDB_RELEASE() {
		Impl::unlock();
	}
};
} // namespace duckdb
