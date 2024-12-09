#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

DynamicFilter::DynamicFilter() : TableFilter(TableFilterType::DYNAMIC_FILTER) {
}

DynamicFilter::DynamicFilter(shared_ptr<DynamicFilterData> filter_data_p)
    : TableFilter(TableFilterType::DYNAMIC_FILTER), filter_data(std::move(filter_data_p)) {
}

FilterPropagateResult DynamicFilter::CheckStatistics(BaseStatistics &stats) {
	if (!filter_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	lock_guard<mutex> l(filter_data->lock);
	if (!filter_data->initialized) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return filter_data->filter->CheckStatistics(stats);
}

string DynamicFilter::ToString(const string &column_name) {
	if (filter_data) {
		return "Dynamic Filter (" + column_name + ")";
	} else {
		return "Empty Dynamic Filter (" + column_name + ")";
	}
}

unique_ptr<Expression> DynamicFilter::ToExpression(const Expression &column) const {
	if (!filter_data || !filter_data->initialized) {
		auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
		return std::move(bound_constant);
	}
	lock_guard<mutex> l(filter_data->lock);
	return filter_data->filter->ToExpression(column);
}

bool DynamicFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<DynamicFilter>();
	return other.filter_data.get() == filter_data.get();
}

unique_ptr<TableFilter> DynamicFilter::Copy() const {
	return make_uniq<DynamicFilter>(filter_data);
}

void DynamicFilterData::SetValue(Value val) {
	if (val.IsNull()) {
		return;
	}
	lock_guard<mutex> l(lock);
	filter->Cast<ConstantFilter>().constant = std::move(val);
	initialized = true;
}

void DynamicFilterData::Reset() {
	lock_guard<mutex> l(lock);
	initialized = false;
}

} // namespace duckdb
