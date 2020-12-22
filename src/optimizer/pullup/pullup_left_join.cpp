#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {
using namespace std;

unique_ptr<LogicalOperator> FilterPullup::PullupLeftJoin(unique_ptr<LogicalOperator> op) {
	auto &join = (LogicalJoin &)*op;
	D_ASSERT(join.join_type == JoinType::LEFT);
	D_ASSERT(op->type != LogicalOperatorType::LOGICAL_DELIM_JOIN);

	FilterPullup left_pullup(true, is_set_operation);
	FilterPullup right_pullup(false, is_set_operation);

	op->children[0] = left_pullup.Rewrite(move(op->children[0]));
	op->children[1] = right_pullup.Rewrite(move(op->children[1]));

	// check only for filters from the LHS
	if(left_pullup.filters_expr_pullup.size() > 0 && right_pullup.filters_expr_pullup.size() == 0) {
		return GeneratePullupFilter(move(op), left_pullup.filters_expr_pullup);
	}
	return op;
}

} // namespace duckdb