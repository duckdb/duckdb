//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_order_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_query_node.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalOrder represents a comibination of ORDER BY and LIMIT clause, using Min/Max Heap
class LogicalOrderAndLimit : public LogicalOperator {
public:
	LogicalOrderAndLimit(vector<BoundOrderByNode> orders, int64_t limit, int64_t offset)
	    : LogicalOperator(LogicalOperatorType::ORDER_BY_LIMIT), orders(move(orders)), limit(limit), offset(offset) {
	}

	vector<BoundOrderByNode> orders;
	//! The maximum amount of elements to emit
	int64_t limit;
	//! The offset from the start to begin emitting elements
	int64_t offset;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
