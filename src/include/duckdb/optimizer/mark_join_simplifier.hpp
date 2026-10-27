//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/mark_join_simplifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalFilter;
class Optimizer;

//! Convert filtered MARK joins while preserving their output schema.
class MarkJoinSimplifier {
public:
	explicit MarkJoinSimplifier(Optimizer &optimizer);
	void Optimize(unique_ptr<LogicalOperator> &plan);

private:
	void Rewrite(unique_ptr<LogicalOperator> &op, LogicalOperator &root);
	bool TrySimplify(LogicalFilter &filter, LogicalOperator &root);

private:
	Optimizer &optimizer;
};

} // namespace duckdb
