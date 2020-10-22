#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

NumericStatistics::NumericStatistics(LogicalType type_p) : BaseStatistics(move(type_p)) {
	min = Value::MaximumValue(type.InternalType());
	max = Value::MinimumValue(type.InternalType());
}

NumericStatistics::NumericStatistics(LogicalType type_p, Value min_p, Value max_p) :
    BaseStatistics(move(type_p)), min(move(min_p)), max(move(max_p)) {

}

void NumericStatistics::Merge(const BaseStatistics &other_p) {
	auto &other = (const NumericStatistics &) other_p;
	has_null = has_null || other.has_null;
	if (other.min < min) {
		min = other.min;
	}
	if (other.max > max) {
		max = other.max;
	}
}

bool NumericStatistics::CheckZonemap(ExpressionType comparison_type, Value constant) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return constant >= min && constant <= max;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return constant <= max;
	case ExpressionType::COMPARE_GREATERTHAN:
		return constant < max;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return constant >= min;
	case ExpressionType::COMPARE_LESSTHAN:
		return constant > min;
	default:
		throw InternalException("Operation not implemented");
	}
}

unique_ptr<BaseStatistics> NumericStatistics::Copy() {
	auto stats = make_unique<NumericStatistics>(type, min, max);
	stats->has_null = has_null;
	return move(stats);
}

void NumericStatistics::Serialize(Serializer &serializer) {
	BaseStatistics::Serialize(serializer);
	min.Serialize(serializer);
	max.Serialize(serializer);
}

unique_ptr<BaseStatistics> NumericStatistics::Deserialize(Deserializer &source, LogicalType type) {
	auto min = Value::Deserialize(source);
	auto max = Value::Deserialize(source);
	return make_unique_base<BaseStatistics, NumericStatistics>(type, min, max);
}

string NumericStatistics::ToString() {
	return StringUtil::Format("Numeric Statistics<%s> [Has Null: %s, Min: %s, Max: %s]",
		type.ToString(), has_null ? "true" : "false", min.ToString(), max.ToString());
}

}