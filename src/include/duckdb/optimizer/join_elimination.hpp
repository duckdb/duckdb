//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "constraint_propagator.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class JoinElimination {
public:
	explicit JoinElimination() {
	}

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op,
	                                             unordered_set<TableIndex> ref_table_ids, bool outer_is_distinct,
	                                             ConstraintPropagator &propagator, bool &changed);

	static unique_ptr<LogicalOperator> TryEliminateJoin(unique_ptr<LogicalOperator> op,
	                                                    const unordered_set<TableIndex> &ref_table_ids,
	                                                    bool outer_is_distinct, ConstraintPropagator &propagator);
};

} // namespace duckdb
