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
struct DelimCandidate;

//! The Deliminator optimizer traverses the logical operator tree and removes any redundant DelimGets/DelimJoins
class Deliminator {
public:
	explicit Deliminator(ClientContext &context) : context(context) {
	}
	//! Perform DelimJoin elimination
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! Find Joins with a DelimGet that can be removed
	void FindCandidates(unique_ptr<LogicalOperator> &op, vector<DelimCandidate> &candidates);
	void FindJoinWithDelimGet(unique_ptr<LogicalOperator> &op, DelimCandidate &candidate);

	bool RemoveJoinWithDelimGet(unique_ptr<LogicalOperator> &join);

private:
	ClientContext &context;
	optional_ptr<LogicalOperator> root;
};

} // namespace duckdb
