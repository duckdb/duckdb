//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_parameters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class QueryResultStreamingMode : uint8_t { DO_NOT_ALLOW, ALLOW };

enum class QueryResultMemoryManagementType : uint8_t { IN_MEMORY, BUFFER_MANAGED };

struct QueryParameters {
	QueryResultStreamingMode streaming_mode = QueryResultStreamingMode::DO_NOT_ALLOW;
	QueryResultMemoryManagementType memory_management_type = QueryResultMemoryManagementType::IN_MEMORY;
};

} // namespace duckdb
