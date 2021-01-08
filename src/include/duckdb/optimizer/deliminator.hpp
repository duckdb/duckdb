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
    bool RemoveCandidate(unique_ptr<LogicalOperator> *op_ptr, column_binding_map_t<unique_ptr<Expression>> &mapping);
	//! Replace references to a removed DelimGet, remove DelimJoins if all their DelimGets are gone
	void UpdatePlan(LogicalOperator &op, column_binding_map_t<unique_ptr<Expression>> &cb_map);
};

} // namespace duckdb
