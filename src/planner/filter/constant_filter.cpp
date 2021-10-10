#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"

namespace duckdb {

ConstantFilter::ConstantFilter(ExpressionType comparison_type_p, Value constant_p)
    : TableFilter(TableFilterType::CONSTANT_COMPARISON), comparison_type(comparison_type_p),
      constant(move(constant_p)) {
}

FilterPropagateResult ConstantFilter::CheckStatistics(BaseStatistics &stats) {
	D_ASSERT(constant.type().id() == stats.type.id());
	switch (constant.type().InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return ((NumericStatistics &)stats).CheckZonemap(comparison_type, constant);
	case PhysicalType::VARCHAR:
		return ((StringStatistics &)stats).CheckZonemap(comparison_type, constant.ToString());
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

string ConstantFilter::ToString(const string &column_name) {
	return column_name + ExpressionTypeToOperator(comparison_type) + constant.ToString();
}

bool ConstantFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	auto &other = (ConstantFilter &)other_p;
	return other.comparison_type == comparison_type && other.constant == constant;
}

} // namespace duckdb
