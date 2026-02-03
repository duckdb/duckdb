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
	QueryParameters() {
	}
	QueryParameters(bool allow_streaming) // NOLINT: allow implicit conversion
	    : output_type(allow_streaming ? QueryResultOutputType::ALLOW_STREAMING
	                                  : QueryResultOutputType::FORCE_MATERIALIZED) {
	}
	QueryParameters(QueryResultOutputType output_type) // NOLINT: allow implicit conversion
	    : output_type(output_type) {
	}
	QueryResultOutputType output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	QueryResultMemoryType memory_type = QueryResultMemoryType::IN_MEMORY;
};

} // namespace duckdb
