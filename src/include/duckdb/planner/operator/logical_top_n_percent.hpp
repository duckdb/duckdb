//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_top_n_percent.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalTopNPercent represents a comibination of ORDER BY and LIMIT clause, using Min/Max Heap
class LogicalTopNPercent : public LogicalOperator {
public:
	LogicalTopNPercent(vector<BoundOrderByNode> orders, double limit_percent, int64_t offset)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_TOP_N_PERCENT), orders(move(orders)),
	      limit_percent(limit_percent), offset(offset) {
	}

	vector<BoundOrderByNode> orders;
	//! The maximum amount of elements to emit
	double limit_percent;
	//! The offset from the start to begin emitting elements
	int64_t offset;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
