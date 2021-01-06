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
	//! Find DelimGets and remove them if they prove to be redundant
	void RemoveRedundantDelims(unique_ptr<LogicalOperator> *op_ptr);
	//! Replace references to a removed DelimGet, remove DelimJoins if all their DelimGets are gone
	void UpdatePlan(LogicalOperator &op, vector<unique_ptr<Expression>> &from, vector<unique_ptr<Expression>> &to);

	LogicalOperator *root;
};

} // namespace duckdb
