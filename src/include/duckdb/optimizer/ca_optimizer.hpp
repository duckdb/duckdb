//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/ca_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {

class CommonAggregateOptimizer : public LogicalOperatorVisitor {
public:
	void VisitOperator(LogicalOperator &op) override;

private:
	typedef unordered_map<Expression *,
	                      vector<decltype(std::declval<BoundColumnRefExpression>().binding.column_index) *>,
	                      ExpressionHashFunction, ExpressionEquality>
	    aggregate_to_bound_ref_map_t;

	void find_bound_references(Expression &expression, const LogicalAggregate &aggregate,
	                           aggregate_to_bound_ref_map_t &aggregate_to_projection_map);

	LogicalAggregate *find_logical_aggregate(vector<Expression *> &expressions, LogicalOperator &projection);
	void ExtractCommonAggregateExpressions(LogicalOperator &projection);
};

} // namespace duckdb
