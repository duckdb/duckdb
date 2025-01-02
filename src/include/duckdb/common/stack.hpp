//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/stack.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/deque.hpp"

#include <stack>

namespace duckdb {

template <class T, class DEQUE = deque<T>>
using stack = std::stack<T, DEQUE>;

}
