#include "duckdb/planner/filter/zone_map_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

FilterPropagateResult ZonemapFilter::CheckStatistics(BaseStatistics &stats) {
	if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
		auto &const_compare = child_filter->Cast<ConstantFilter>();
		if (const_compare.constant.type().IsTemporal()) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	}
	return child_filter->CheckStatistics(stats);
}

string ZonemapFilter::ToString(const string &column_name) {
	return child_filter->ToString(column_name);
}

unique_ptr<Expression> ZonemapFilter::ToExpression(const Expression &column) const {
	return nullptr;
}

unique_ptr<TableFilter> ZonemapFilter::Copy() const {
	auto copy = make_uniq<ZonemapFilter>();
	copy->child_filter = child_filter->Copy();
	return copy;
}

} // namespace duckdb
