#include "duckdb/optimizer/predicate_pushdown.hpp"

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
    filter.
    auto get = (LogicalGet *)op->children[0].get();
}
