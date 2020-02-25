//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalAggregate : public LogicalOperator {
public:
	LogicalAggregate(idx_t group_index, idx_t aggregate_index, vector<unique_ptr<Expression>> select_list);

	//! The table index for the groups of the LogicalAggregate
	idx_t group_index;
	//! The table index for the aggregates of the LogicalAggregate
	idx_t aggregate_index;
	//! The set of groups (optional).
	vector<unique_ptr<Expression>> groups;

public:
	string ParamsToString() const override;

	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
