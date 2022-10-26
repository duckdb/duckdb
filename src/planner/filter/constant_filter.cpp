#include "duckdb/planner/filter/constant_filter.hpp"

#include "duckdb/common/field_writer.hpp"
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
		return ((StringStatistics &)stats).CheckZonemap(comparison_type, StringValue::Get(constant));
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

void ConstantFilter::Serialize(FieldWriter &writer) const {
	writer.WriteField(comparison_type);
	writer.WriteSerializable(constant);
}

unique_ptr<TableFilter> ConstantFilter::Deserialize(FieldReader &source) {
	auto comparision_type = source.ReadRequired<ExpressionType>();
	auto constant = source.ReadRequiredSerializable<Value, Value>();
	return make_unique<ConstantFilter>(comparision_type, constant);
}

} // namespace duckdb
