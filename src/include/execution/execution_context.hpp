//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "execution/physical_operator.hpp"
#include "main/result.hpp"

namespace duckdb {
class DuckDB;

class ExecutionContext {
public:
	DuckDBResult internal_result;
	unique_ptr<PhysicalOperator> physical_plan;
	unique_ptr<PhysicalOperatorState> physical_state;
	unique_ptr<DataChunk> first_chunk;
};
} // namespace duckdb
