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

class BindingReplacementGraph;
class NotNullExpressionAnalyzer;
class Optimizer;

//! Convert filtered MARK joins while preserving their output schema.
class MarkJoinSimplifier {
public:
	explicit MarkJoinSimplifier(Optimizer &optimizer);
	void Optimize(unique_ptr<LogicalOperator> &plan);

private:
	void Rewrite(unique_ptr<LogicalOperator> &op, NotNullExpressionAnalyzer &nullability,
	             BindingReplacementGraph &replacements, vector<reference<LogicalOperator>> &ancestors);
	bool TrySimplify(unique_ptr<LogicalOperator> &op, NotNullExpressionAnalyzer &nullability,
	                 BindingReplacementGraph &replacements);

	bool TrySimplifyDisjunction(unique_ptr<LogicalOperator> &op, NotNullExpressionAnalyzer &nullability,
	                            const vector<reference<LogicalOperator>> &ancestors,
	                            BindingReplacementGraph &replacements);

private:
	Optimizer &optimizer;
};

} // namespace duckdb
