//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parallel/parallel_state.hpp"

namespace duckdb {
class PhysicalOperator;

//! TaskContext holds task specific information relating to the excution
class TaskContext {
public:
	TaskContext() {
	}

	//! Per-operator task info
	unordered_map<const PhysicalOperator *, ParallelState *> task_info;
};

} // namespace duckdb
