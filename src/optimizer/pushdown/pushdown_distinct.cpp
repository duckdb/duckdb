#include <memory>
#include <utility>

#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownDistinct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_DISTINCT);
	auto &distinct = op->Cast<LogicalDistinct>();
	if (!distinct.order_by) {
		// regular DISTINCT - can just push down
		op->children[0] = Rewrite(std::move(op->children[0]));
		return op;
	}
	// no pushdown through DISTINCT ON (yet?)
	return FinishPushdown(std::move(op));
}

} // namespace duckdb
