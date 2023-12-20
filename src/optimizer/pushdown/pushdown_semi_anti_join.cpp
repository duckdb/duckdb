#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownSemiAntiJoin(unique_ptr<LogicalOperator> op,
                                                              unordered_set<idx_t> &left_bindings,
                                                              unordered_set<idx_t> &right_bindings) {
	auto &join = op->Cast<LogicalJoin>();

	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
//	for (idx_t i = 0; i < filters.size(); i++) {
//		// first create a copy of the filter
//		auto right_filter = make_uniq<Filter>();
//		right_filter->filter = filters[i]->filter->Copy();
//
//		// in the original filter, rewrite references to the result of the union into references to the left_index
//		ReplaceSetOpBindings(left_bindings, *filters[i], *filters[i]->filter, setop);
//		// in the copied filter, rewrite references to the result of the union into references to the right_index
//		ReplaceSetOpBindings(right_bindings, *right_filter, *right_filter->filter, setop);
//
//		// extract bindings again
//		filters[i]->ExtractBindings();
//		right_filter->ExtractBindings();
//
//		// move the filters into the child pushdown nodes
//		left_pushdown.filters.push_back(std::move(filters[i]));
//		right_pushdown.filters.push_back(std::move(right_filter));
//	}

	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));

	bool left_empty = op->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	bool right_empty = op->children[1]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	if (left_empty && right_empty) {
		// both empty: return empty result
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}

	// filter pushdown happens before join order optimization, so left_anti and left_semi are not possible yet here
	if (left_empty) {
		// left child is empty result
		switch (join.join_type) {
		case JoinType::ANTI:
		case JoinType::SEMI:
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			break;
		}
	} else if (right_empty) {
		// right child is empty result
		switch (join.join_type) {
		case JoinType::ANTI:
			// just return the left child.
			return std::move(op->children[0]);
		case JoinType::SEMI:
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			break;
		}
	}
	return FinishPushdown(std::move(op));
}

} // namespace duckdb
