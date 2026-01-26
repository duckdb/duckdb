//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread_annotation/mutex.hpp
//
//
//===----------------------------------------------------------------------===//

// Bind annotated mutex and lock type with standard implementation, so that duckdb::unique_lock<duckdb::mutex> could
// inherit std::unique_lock<std::mutex>.

#pragma once

#include <mutex>

namespace duckdb {

// Forward declaration for annotated mutex types.
class mutex;

// Forward declaration for annotated lock types.
template <typename M>
class unique_lock;
template <typename M>
class lock_guard;

namespace internal {

// Type alias for mutex types.
template <typename T>
struct standard_impl {
	using type = T;
};
template <typename T>
using standard_impl_t = typename standard_impl<T>::type;

// Specialization for `std::mutex`.
template <>
struct standard_impl<::duckdb::mutex> {
	using type = std::mutex;
};

// Type alias for lock types.
template <typename M>
using mutex_impl_t = standard_impl_t<M>;

// Specialization for `std::unique_lock`.
template <typename M>
struct standard_impl<::duckdb::unique_lock<M>> {
	using type = std::unique_lock<mutex_impl_t<M>>;
};

// Specialization for `std::lock_guard`.
template <typename M>
struct standard_impl<::duckdb::lock_guard<M>> {
	using type = std::lock_guard<mutex_impl_t<M>>;
};

template <typename L>
using lock_impl_t = standard_impl_t<L>;

} // namespace internal
} // namespace duckdb
