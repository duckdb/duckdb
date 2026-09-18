#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

LegacyOptionalFilter::LegacyOptionalFilter(unique_ptr<TableFilter> filter)
    : TableFilter(TableFilterType::LEGACY_OPTIONAL_FILTER), child_filter(std::move(filter)) {
}

unique_ptr<Expression> LegacyOptionalFilter::ToExpression(const Expression &column) const {
	auto child_expr = child_filter ? child_filter->ToExpression(column) : nullptr;
	return CreateOptionalFilterExpression(std::move(child_expr), column.GetReturnType());
}

} // namespace duckdb
