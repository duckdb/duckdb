#include "duckdb/storage/statistics/statistics_operations.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsOperations::NumericNumericCast(const BaseStatistics *input_, LogicalType target) {
	auto &input = (NumericStatistics &) *input_;

	Value min = input.min, max = input.max;
	if (!min.TryCastAs(target) || !max.TryCastAs(target)) {
		// overflow in cast: bailout
		return nullptr;
	}
	auto stats = make_unique<NumericStatistics>(target, move(min), move(max));
	stats->has_null = input.has_null;
	return stats;
}

unique_ptr<BaseStatistics> StatisticsOperations::NumericCastSwitch(const BaseStatistics *input, LogicalType target) {
	switch(target.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return StatisticsOperations::NumericNumericCast(input, target);
	default:
		return nullptr;
	}
}

unique_ptr<BaseStatistics> StatisticsOperations::Cast(const BaseStatistics *input, LogicalType target) {
	if (!input) {
		return nullptr;
	}
	switch(input->type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return StatisticsOperations::NumericCastSwitch(input, target);
	default:
		return nullptr;
	}
}

unique_ptr<BaseStatistics> StatisticsOperations::FromValue(const Value &input) {
	switch(input.type().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE: {
		auto result = make_unique<NumericStatistics>(input.type(), input, input);
		result->has_null = input.is_null;
		return move(result);
	}
	default:
		return nullptr;
	}
}

}