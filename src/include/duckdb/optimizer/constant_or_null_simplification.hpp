//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/constant_or_null_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {
class ClientContext;
class NotNullExpressionAnalyzer;

//! Proves that an expression is non-NULL on the rows the predicate is evaluated over
using NotNullProof = std::function<bool(Expression &)>;

class ConstantOrNullSimplification {
public:
	explicit ConstantOrNullSimplification(ClientContext &context);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op, bool plan_has_side_effects);
	unique_ptr<Expression> SimplifyExpression(unique_ptr<Expression> expr, const NotNullProof &proof,
	                                          bool allow_folding);
	unique_ptr<LogicalOperator> OptimizeFilter(unique_ptr<LogicalOperator> op, bool plan_has_side_effects);

private:
	ClientContext &context;
};

} // namespace duckdb
