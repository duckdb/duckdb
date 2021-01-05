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

class Deliminator {
public:
	Deliminator(Optimizer &optimizer) : optimizer(optimizer) {
	}
	//! Remove redundant duplicate-eliminated joins and/or delim gets
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

private:
	Optimizer &optimizer;
	LogicalOperator *root;

	//! TODO
	void RemoveRedundantDelimGets(unique_ptr<LogicalOperator> *op_ptr);
	//! TODO
	void ReplaceRemovedBindings(LogicalOperator &op, vector<unique_ptr<Expression>> &from,
	                            vector<unique_ptr<Expression>> &to);
};

} // namespace duckdb
