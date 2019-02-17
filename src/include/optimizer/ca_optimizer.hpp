//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/ca_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_projection.hpp"
#include "planner/operator/logical_aggregate.hpp"

#include "parser/expression/bound_expression.hpp"

using namespace duckdb;
using namespace std;


namespace duckdb {

class CommonAggregateOptimizer : public LogicalOperatorVisitor {
public:
    void VisitOperator(LogicalOperator &op) override;

private:
    typedef unordered_map<Expression*, vector<decltype(std::declval<BoundExpression>().index)*> , ExpressionHashFunction, ExpressionEquality> aggregate_to_bound_ref_map_t;

    void find_bound_references(Expression& expression, const LogicalAggregate& aggregate, aggregate_to_bound_ref_map_t& aggregate_to_projection_map, size_t& nr_of_groups);

    LogicalAggregate* find_logical_aggregate(const vector<unique_ptr<LogicalOperator>>& child_operators);
    void ExtractCommonAggregateExpressions(LogicalOperator &projection);
};

}