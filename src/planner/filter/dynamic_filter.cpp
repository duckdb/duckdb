#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {

LegacyDynamicFilter::LegacyDynamicFilter() : TableFilter(TableFilterType::LEGACY_DYNAMIC_FILTER) {
}

LegacyDynamicFilter::LegacyDynamicFilter(shared_ptr<DynamicFilterData> filter_data_p)
    : TableFilter(TableFilterType::LEGACY_DYNAMIC_FILTER), filter_data(std::move(filter_data_p)) {
}

DynamicFilterData::DynamicFilterData(ExpressionType comparison_type_p, Value constant_p)
    : comparison_type(comparison_type_p), constant(std::move(constant_p)) {
}

unique_ptr<Expression> DynamicFilterData::ToExpression(const Expression &column) const {
	return BoundComparisonExpression::Create(comparison_type, column.Copy(),
	                                         make_uniq<BoundConstantExpression>(constant));
}

bool DynamicFilterData::CompareValue(ExpressionType comparison_type, const Value &constant, const Value &value) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return ValueOperations::Equals(value, constant);
	case ExpressionType::COMPARE_NOTEQUAL:
		return ValueOperations::NotEquals(value, constant);
	case ExpressionType::COMPARE_GREATERTHAN:
		return ValueOperations::GreaterThan(value, constant);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ValueOperations::GreaterThanEquals(value, constant);
	case ExpressionType::COMPARE_LESSTHAN:
		return ValueOperations::LessThan(value, constant);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ValueOperations::LessThanEquals(value, constant);
	default:
		throw InternalException("Unknown comparison type for DynamicFilter: " + EnumUtil::ToString(comparison_type));
	}
}

FilterPropagateResult DynamicFilterData::CheckStatistics(BaseStatistics &stats, ExpressionType comparison_type,
                                                         const Value &constant) {
	switch (constant.type().InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return NumericStats::CheckZonemap(stats, comparison_type, array_ptr<const Value>(constant));
	case PhysicalType::VARCHAR:
		if (stats.GetStatsType() == StatisticsType::STRING_STATS) {
			return StringStats::CheckZonemap(stats, comparison_type, array_ptr<const Value>(constant));
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

unique_ptr<Expression> LegacyDynamicFilter::ToExpression(const Expression &column) const {
	if (!filter_data || !filter_data->initialized) {
		auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
		return std::move(bound_constant);
	}
	lock_guard<mutex> l(filter_data->lock);
	return filter_data->ToExpression(column);
}

void DynamicFilterData::SetValue(Value val) {
	if (val.IsNull()) {
		return;
	}
	lock_guard<mutex> l(lock);
	constant = std::move(val);
	initialized = true;
}

void DynamicFilterData::Reset() {
	lock_guard<mutex> l(lock);
	initialized = false;
}

} // namespace duckdb
