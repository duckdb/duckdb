//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/deliminator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

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
    unique_ptr<LogicalOperator> FindDelimGets(unique_ptr<LogicalOperator> op);
    //! TODO
    void Replace(Expression &expr, vector<ColumnBinding> &from, vector<ColumnBinding> &to);
	//! TODO
    unique_ptr<LogicalOperator> RemoveDelimJoins(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb
