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

class LogicalSetOperation;

//! Chooses how an EXCEPT ALL / INTERSECT ALL runs, after the filters have been moved around the set operation: as one
//! GROUP BY all columns over the UNION ALL of both children that counts the rows per side and emits every group as
//! often as the set operation asks for, or, where rows that compare equal can differ, as a SEMI / ANTI join of the
//! numbered rows. See set_operation_aggregate.cpp.
class SetOperationAggregate {
public:
	//! Without use_aggregate every set operation becomes the join
	SetOperationAggregate(Optimizer &optimizer, bool use_aggregate);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	bool CanUseAggregate(LogicalSetOperation &setop);
	bool FewGroupsOfManyRows(LogicalSetOperation &setop);
	bool OneSideMuchSmaller(LogicalSetOperation &setop);
	unique_ptr<LogicalOperator> CreateAggregate(LogicalSetOperation &setop);
	unique_ptr<LogicalOperator> AddSideMarkers(unique_ptr<LogicalOperator> child, bool is_left);

	Optimizer &optimizer;
	bool use_aggregate;
};

} // namespace duckdb
