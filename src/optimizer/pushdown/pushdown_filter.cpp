#include "optimizer/filter_pushdown.hpp"
#include "planner/operator/logical_empty_result.hpp"
#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownFilter(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::FILTER);
	auto &filter = (LogicalFilter &)*op;
	// filter: gather the filters and remove the filter from the set of operations
	for (size_t i = 0; i < filter.expressions.size(); i++) {
		if (AddFilter(move(filter.expressions[i]))) {
			// filter statically evaluates to false, strip tree
			return make_unique<LogicalEmptyResult>(move(op));
		}
	}
	return Rewrite(move(filter.children[0]));
}
