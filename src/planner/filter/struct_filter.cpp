#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

StructFilter::StructFilter(idx_t child_idx_p, string child_name_p, unique_ptr<TableFilter> child_filter_p)
    : TableFilter(TableFilterType::STRUCT_EXTRACT), child_idx(child_idx_p), child_name(std::move(child_name_p)),
      child_filter(std::move(child_filter_p)) {
}

FilterPropagateResult StructFilter::CheckStatistics(BaseStatistics &stats) {
	D_ASSERT(stats.GetType().id() == LogicalTypeId::STRUCT);
	// Check the child statistics
	auto &child_stats = StructStats::GetChildStats(stats, child_idx);
	return child_filter->CheckStatistics(child_stats);
}

string StructFilter::ToString(const string &column_name) {
	return child_filter->ToString(column_name + "." + child_name);
}

bool StructFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<StructFilter>();
	return other.child_idx == child_idx && StringUtil::CIEquals(other.child_name, child_name) &&
	       other.child_filter->Equals(*child_filter);
}

unique_ptr<TableFilter> StructFilter::Copy() const {
	return make_uniq<StructFilter>(child_idx, child_name, child_filter->Copy());
}

unique_ptr<Expression> StructFilter::ToExpression(const Expression &column) const {
	auto &child_type = StructType::GetChildType(column.return_type, child_idx);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(column.Copy());
	arguments.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(child_idx))));
	auto child = make_uniq<BoundFunctionExpression>(child_type, StructExtractFun::IndexExtractFunction(),
	                                                std::move(arguments), StructExtractFun::GetBindData(child_idx));
	return child_filter->ToExpression(*child);
}
} // namespace duckdb
