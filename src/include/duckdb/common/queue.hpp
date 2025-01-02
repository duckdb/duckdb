//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/queue.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/deque.hpp"

#include <queue>

namespace duckdb {

template <class T, class DEQUE = deque<T>>
using queue = std::queue<T, DEQUE>;

} // namespace duckdb
