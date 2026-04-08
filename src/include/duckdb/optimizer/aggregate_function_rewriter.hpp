//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/aggregate_function_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Optimizer;
class AggregateRewriteRule;
class LogicalOperator;

//! Rewrites aggregate functions. Currently registered rules:
//! AVG(x) -> SUM(x) / COUNT(x)
//! SUM(x + C) -> SUM(x) + C * COUNT(x)
class AggregateFunctionRewriter {
public:
	explicit AggregateFunctionRewriter(Optimizer &optimizer);
	~AggregateFunctionRewriter();

	void Optimize(unique_ptr<LogicalOperator> &op);

private:
	Optimizer &optimizer;
	vector<unique_ptr<AggregateRewriteRule>> rules;
};

} // namespace duckdb
