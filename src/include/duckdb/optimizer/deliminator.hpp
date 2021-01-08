//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/deliminator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class Optimizer;

//! The Deliminator optimizer traverses the logical operator tree and removes any redundant DelimGets/DelimJoins
class Deliminator {
public:
	Deliminator() {
	}

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! TODO
	void FindCandidates(unique_ptr<LogicalOperator> *op_ptr, vector<unique_ptr<LogicalOperator> *> &candidates);
	//! TODO
	bool RemoveCandidate(unique_ptr<LogicalOperator> *op_ptr, expression_map_t<Expression *> &expr_map, unique_ptr<LogicalOperator> *temp_ptr);
	//! Replace references to a removed DelimGet, remove DelimJoins if all their DelimGets are gone
	void UpdatePlan(LogicalOperator &op, expression_map_t<Expression *> &expr_map);
};

} // namespace duckdb
