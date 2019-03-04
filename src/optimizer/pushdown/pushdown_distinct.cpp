#include "optimizer/filter_pushdown.hpp"

#include "planner/operator/logical_distinct.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> FilterPushdown::PushdownDistinct(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::DISTINCT);
	assert(op->children.size() == 1);
	op->children[0] = Rewrite(move(op->children[0]));
	return op;
}
