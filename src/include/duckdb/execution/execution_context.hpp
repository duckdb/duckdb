//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {
class DuckDB;

class ExecutionContext {
public:
	unique_ptr<PhysicalOperator> physical_plan;
	unique_ptr<PhysicalOperatorState> physical_state;

public:
	void Reset() {
		physical_plan = nullptr;
		physical_state = nullptr;
	}
};
} // namespace duckdb
