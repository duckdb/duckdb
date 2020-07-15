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

namespace duckdb {
class PhysicalOperator;

class OperatorTaskInfo {
public:
	virtual ~OperatorTaskInfo() {}
};

//! TaskContext holds task specific information relating to the excution
class TaskContext {
public:
	TaskContext() {}

	//! Per-operator task info
	unordered_map<PhysicalOperator*, unique_ptr<OperatorTaskInfo>> task_info;
};

} // namespace duckdb
