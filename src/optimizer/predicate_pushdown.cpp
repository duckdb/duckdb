#include "duckdb/optimizer/predicate_pushdown.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> PredicatePushdown::Optimize(unique_ptr<LogicalOperator> op) {
    if (op->type == LogicalOperatorType::FILTER && op->children[0]->type == LogicalOperatorType::GET) {
        return PushdownFilterPredicates(move(op));
    }
    for (auto &child : op->children) {
        child = Optimize(move(child));
    }
    return op;
}

unique_ptr<LogicalOperator> PredicatePushdown::PushdownFilterPredicates(unique_ptr<LogicalOperator> op) {
    assert(op->type == LogicalOperatorType::FILTER);
    auto &filter = (LogicalFilter &)*op;
        for (auto& expression: filter.expressions){
            if (expression->expression_class == ExpressionClass::BOUND_COMPARISON){
                auto &bcexpression = (BoundComparisonExpression &)*expression;
                if(bcexpression.type == ExpressionType::COMPARE_EQUAL){
                    if (bcexpression.left->type == ExpressionType::BOUND_COLUMN_REF && bcexpression.right->type == ExpressionType::VALUE_CONSTANT){

                    }

                }
            }


        expression->IsScalar();
    }
//    filter.
    auto get = (LogicalGet *)op->children[0].get();
}
