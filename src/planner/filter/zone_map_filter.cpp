#include "duckdb/planner/filter/zone_map_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

namespace duckdb {

ZoneMapFilter::ZoneMapFilter() : TableFilter(TableFilterType::ZONE_MAP) {
}

FilterPropagateResult ZoneMapFilter::CheckStatistics(BaseStatistics &stats) {
	if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
		auto &const_compare = child_filter->Cast<ConstantFilter>();
		if (const_compare.constant.type().IsTemporal()) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return child_filter->CheckStatistics(stats);
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

string ZoneMapFilter::ToString(const string &column_name) {
	D_ASSERT(child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON);
	auto const_filter = child_filter->Cast<ConstantFilter>();
	auto val = const_filter.constant;
	return val.ToSQLString() + " IN ZoneMap(row_group)";
}

unique_ptr<Expression> ZoneMapFilter::ToExpression(const Expression &column) const {
	return child_filter->ToExpression(column);
}

unique_ptr<TableFilter> ZoneMapFilter::Copy() const {
	auto copy = make_uniq<ZoneMapFilter>();
	copy->child_filter = child_filter->Copy();
	return duckdb::unique_ptr_cast<ZoneMapFilter, TableFilter>(std::move(copy));
}

} // namespace duckdb
