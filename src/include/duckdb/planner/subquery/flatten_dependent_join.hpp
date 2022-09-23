//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/flatten_dependent_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! The FlattenDependentJoins class is responsible for pushing the dependent join down into the plan to create a
//! flattened subquery
struct FlattenDependentJoins {
	FlattenDependentJoins(Binder &binder, const vector<CorrelatedColumnInfo> &correlated, bool perform_delim = true,
	                      bool any_join = false);

	//! Detects which Logical Operators have correlated expressions that they are dependent upon, filling the
	//! has_correlated_expressions map.
	bool DetectCorrelatedExpressions(LogicalOperator *op);

	//! Push the dependent join down a LogicalOperator
	unique_ptr<LogicalOperator> PushDownDependentJoin(unique_ptr<LogicalOperator> plan);

	Binder &binder;
	ColumnBinding base_binding;
	idx_t delim_offset;
	idx_t data_offset;
	unordered_map<LogicalOperator *, bool> has_correlated_expressions;
	column_binding_map_t<idx_t> correlated_map;
	column_binding_map_t<idx_t> replacement_map;
	const vector<CorrelatedColumnInfo> &correlated_columns;
	vector<LogicalType> delim_types;

	bool perform_delim;
	bool any_join;

private:
	unique_ptr<LogicalOperator> PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan,
	                                                          bool &parent_propagate_null_values);
};

} // namespace duckdb
