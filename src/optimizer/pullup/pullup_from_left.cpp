#include <memory>
#include <utility>

#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPullup::PullupFromLeft(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_EXCEPT || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);

	FilterPullup left_pullup(true, can_add_column);
	FilterPullup right_pullup(false, can_add_column);

	op->children[0] = left_pullup.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pullup.Rewrite(std::move(op->children[1]));

	// check only for filters from the LHS
	if (!left_pullup.filters_expr_pullup.empty() && right_pullup.filters_expr_pullup.empty()) {
		return GeneratePullupFilter(std::move(op), left_pullup.filters_expr_pullup);
	}
	return op;
}

} // namespace duckdb
