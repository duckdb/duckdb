//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/query_parameters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

#include <functional>

namespace duckdb {

enum class QueryResultMemoryType : uint8_t { IN_MEMORY, BUFFER_MANAGED };

struct QueryParameters {
	QueryResultMemoryType memory_type = QueryResultMemoryType::IN_MEMORY;
	//! Called whenever the result's observable state may have changed. Created with the query and
	//! cleared when it ends. Callback rules: see QueryResultNotifier
	std::function<void()> notify_callback = nullptr;
};

} // namespace duckdb
