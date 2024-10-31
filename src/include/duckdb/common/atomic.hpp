//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/atomic.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

namespace duckdb {

using std::atomic;

//! NOTE: When repeatedly trying to atomically set a value in a loop, you can use as the loop condition:
//! * std::atomic_compare_exchange_weak
//! * std::atomic::compare_exchange_weak
//! If not used as a loop condition, use:
//! * std::atomic_compare_exchange_strong
//! * std::atomic::compare_exchange_strong
//! If this is not done correctly, we may get correctness issues when using older compiler versions (see: issue #14389)
//! Performance may be optimized using std::memory_order, but NOT at the cost of correctness.
//! For correct examples of this, see concurrentqueue.h

} // namespace duckdb
