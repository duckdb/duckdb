#include <memory>
#include <unordered_set>
#include <utility>

#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
struct TableIndex;

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownSingleJoin(unique_ptr<LogicalOperator> op,
                                                               unordered_set<TableIndex> &left_bindings,
                                                               unordered_set<TableIndex> &right_bindings) {
	D_ASSERT(op->Cast<LogicalJoin>().join_type == JoinType::SINGLE);
	FilterPushdown left_pushdown(optimizer, convert_mark_joins), right_pushdown(optimizer, convert_mark_joins);
	// now check the set of filters
	for (idx_t i = 0; i < filters.size(); i++) {
		auto side = JoinSide::GetJoinSide(filters[i]->bindings, left_bindings, right_bindings);
		if (side == JoinSide::LEFT) {
			// bindings match left side: push into left
			left_pushdown.filters.push_back(std::move(filters[i]));
			// erase the filter from the list of filters
			filters.erase_at(i);
			i--;
		}
	}
	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));
	return PushFinalFilters(std::move(op));
}

} // namespace duckdb
