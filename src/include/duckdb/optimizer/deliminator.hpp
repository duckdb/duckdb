//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/deliminator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class Optimizer;
class DeliminatorPlanUpdater;

//! The Deliminator optimizer traverses the logical operator tree and removes any redundant DelimGets/DelimJoins
class Deliminator {
public:
	Deliminator() {
	}
	//! Perform DelimJoin elimination
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! Find Joins with a DelimGet that can be removed
	void FindCandidates(unique_ptr<LogicalOperator> *op_ptr, vector<unique_ptr<LogicalOperator> *> &candidates);
	//! Try to remove a Join with a DelimGet, returns true if it was successful
	bool RemoveCandidate(unique_ptr<LogicalOperator> *plan, unique_ptr<LogicalOperator> *candidate,
	                     DeliminatorPlanUpdater &updater);
	//! Try to remove an inequality Join with a DelimGet, returns true if it was successful
	bool RemoveInequalityCandidate(unique_ptr<LogicalOperator> *plan, unique_ptr<LogicalOperator> *candidate,
	                               DeliminatorPlanUpdater &updater);
};

} // namespace duckdb
