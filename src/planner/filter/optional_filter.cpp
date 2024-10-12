#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

OptionalFilter::OptionalFilter() : TableFilter(TableFilterType::OPTIONAL_FILTER) {
}

FilterPropagateResult OptionalFilter::CheckStatistics(BaseStatistics &stats) {
	return child_filter->CheckStatistics(stats);
}

string OptionalFilter::ToString(const string &column_name) {
	return string("optional: ") + child_filter->ToString(column_name);
}

unique_ptr<Expression> OptionalFilter::ToExpression(const Expression &column) const {
	return child_filter->ToExpression(column);
}

unique_ptr<TableFilter> OptionalFilter::Copy() const {
	auto copy = make_uniq<OptionalFilter>();
	copy->child_filter = child_filter->Copy();
	return duckdb::unique_ptr_cast<OptionalFilter, TableFilter>(std::move(copy));
}

} // namespace duckdb
