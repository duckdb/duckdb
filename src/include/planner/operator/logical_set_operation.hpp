//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_set_operation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalSetOperation : public LogicalOperator {
public:
	LogicalSetOperation(index_t table_index, index_t column_count, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom, LogicalOperatorType type)
	    : LogicalOperator(type), table_index(table_index), column_count(column_count) {
		assert(type == LogicalOperatorType::UNION || type == LogicalOperatorType::EXCEPT ||
		       type == LogicalOperatorType::INTERSECT);
		children.push_back(move(top));
		children.push_back(move(bottom));
	}

	index_t table_index;
	index_t column_count;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
