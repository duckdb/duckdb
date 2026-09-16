//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_parameters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/query_result_memory_type.hpp"
#include "duckdb/common/enums/result_eagerness.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"

namespace duckdb {

struct QueryParameters {
	//! Arguments for a parameterized statement (may be null)
	optional_ptr<identifier_map_t<BoundParameterData>> statement_args;
	//! FORCED by Query and Execute. AUTO defers to the statement: a statement whose eagerness is
	//! FORCED is still settled at submission
	ResultEagerness result_eagerness = ResultEagerness::AUTO;
	//! Where a retained result keeps its rows: the default allocator, or the buffer manager so a
	//! large result can spill to disk
	QueryResultMemoryType memory_type = QueryResultMemoryType::IN_MEMORY;
};

} // namespace duckdb
