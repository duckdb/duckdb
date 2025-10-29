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

enum class QueryResultOutputType : uint8_t { FORCE_MATERIALIZED, ALLOW_STREAMING };

enum class QueryResultMemoryType : uint8_t { IN_MEMORY, BUFFER_MANAGED };

struct QueryParameters {
	QueryResultOutputType output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	QueryResultMemoryType memory_type = QueryResultMemoryType::IN_MEMORY;
};

} // namespace duckdb
