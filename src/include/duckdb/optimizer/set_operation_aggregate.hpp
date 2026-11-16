//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/set_operation_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

//! Rewrites EXCEPT ALL and INTERSECT ALL, planned as a SEMI / ANTI join of two row_number() windows over all columns,
//! into one GROUP BY all columns over the UNION ALL of both inputs that counts the rows per side and emits every
//! group as often as the set operation asks for.
//! See set_operation_aggregate.cpp for the conditions and the argument that the rewrite is exact.
class SetOperationAggregate {
public:
	explicit SetOperationAggregate(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! The columns of the set operation: the type and the binding in the left and in the right input
	struct SetOperationColumn {
		LogicalType type;
		ColumnBinding left;
		ColumnBinding right;
	};

	bool Match(LogicalOperator &op, vector<SetOperationColumn> &columns, bool &is_except) const;
	void Rewrite(LogicalOperator &op, const vector<SetOperationColumn> &columns, bool is_except);
	unique_ptr<LogicalOperator> AddSideMarkers(unique_ptr<LogicalOperator> child,
	                                           const vector<SetOperationColumn> &columns, bool is_left);

	Optimizer &optimizer;
};

} // namespace duckdb
