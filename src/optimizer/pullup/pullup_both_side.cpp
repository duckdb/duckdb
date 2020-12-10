#include "duckdb/optimizer/filter_pullup.hpp"

namespace duckdb {
using namespace std;

unique_ptr<LogicalOperator> FilterPullup::PullupBothSide(unique_ptr<LogicalOperator> op) {
	FilterPullup left_pullup(optimizer, root_pullup_node_ptr, true);
	FilterPullup right_pullup(optimizer, root_pullup_node_ptr, true);
	op->children[0] = left_pullup.Rewrite(move(op->children[0]));
	op->children[1] = right_pullup.Rewrite(move(op->children[1]));
	// just a filter from the LHS
	if(left_pullup.filters_pullup.size() > 0 && right_pullup.filters_pullup.size() == 0) {
		auto filter = move(left_pullup.filters_pullup[0]);
		filter->children.push_back(move(op));
		return filter;
	}
	// just a filter from the RHS
	if(right_pullup.filters_pullup.size() > 0 && left_pullup.filters_pullup.size() == 0) {
		auto filter = move(right_pullup.filters_pullup[0]);
		filter->children.push_back(move(op));
		return filter;
	}
	if(left_pullup.filters_pullup.size() > 0 && right_pullup.filters_pullup.size() > 0) {
		// Appending into a single logical filter the filter expressions from both side and return it
		auto left_filter  = move(left_pullup.filters_pullup[0]);
		auto right_filter  = move(right_pullup.filters_pullup[0]);
		for(idx_t i=0; i < right_filter->expressions.size(); ++i) {
			left_filter->expressions.push_back(move(right_filter->expressions[i]));
		}
		left_filter->children.push_back(move(op));
		return left_filter;
	}

	return op;
}

} // namespace duckdb
