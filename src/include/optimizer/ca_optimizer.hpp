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

#include <list>

using namespace duckdb;
using namespace std;


namespace duckdb {

class CommonAggregateOptimizer : public LogicalOperatorVisitor {

public:
    void VisitOperator(LogicalOperator &op) override;

private:
    typedef unordered_map<Expression*, list<size_t> , ExpressionHashFunction, ExpressionEquality> aggregate_to_projection_map_t;

    LogicalAggregate* find_logical_aggregate(const vector<unique_ptr<LogicalOperator>>& child_operators);
    void ExtractCommonAggregateExpressions(LogicalOperator &projection);
};

}