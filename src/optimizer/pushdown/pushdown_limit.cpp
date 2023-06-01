#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownLimit(unique_ptr<LogicalOperator> op) {
	auto &limit = op->Cast<LogicalLimit>();

	if (!limit.limit && limit.limit_val == 0) {
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}

	return FinishPushdown(std::move(op));
}

} // namespace duckdb
