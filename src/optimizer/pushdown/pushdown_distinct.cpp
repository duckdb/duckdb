#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownDistinct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_DISTINCT);
	auto &distinct = op->Cast<LogicalDistinct>();
	if (!distinct.order_by) {
		// regular DISTINCT - push down non-volatile filters; volatile filters stay above
		// the DISTINCT since pushing them down changes the rows they are evaluated on
		FilterPushdown child_pushdown(optimizer, convert_mark_joins, projection_mode);
		for (idx_t i = 0; i < filters.size(); i++) {
			auto &f = *filters[i];
			if (f.filter->IsVolatile()) {
				// volatile filter: cannot push below DISTINCT
				continue;
			}
			if (child_pushdown.AddFilter(std::move(f.filter)) == FilterResult::UNSATISFIABLE) {
				return make_uniq<LogicalEmptyResult>(std::move(op));
			}
			filters.erase_at(i);
			i--;
		}
		child_pushdown.GenerateFilters();
		op->children[0] = child_pushdown.Rewrite(std::move(op->children[0]));
		return PushFinalFilters(std::move(op));
	}
	// no pushdown through DISTINCT ON (yet?)
	return FinishPushdown(std::move(op));
}

} // namespace duckdb
