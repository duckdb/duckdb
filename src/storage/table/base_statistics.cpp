#include "duckdb/storage/table/numeric_statistics.hpp"
#include "duckdb/storage/table/string_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> BaseStatistics::Copy() {
	auto statistics = make_unique<BaseStatistics>();
	statistics->has_null = has_null;
	return statistics;
}

void BaseStatistics::Serialize(Serializer &serializer) {
	serializer.Write<bool>(has_null);
}

void BaseStatistics::Merge(const BaseStatistics &other) {
	has_null = has_null || other.has_null;
}

unique_ptr<BaseStatistics> BaseStatistics::CreateEmpty(LogicalType type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return make_unique<NumericStatistics<int8_t>>();
	case PhysicalType::INT16:
		return make_unique<NumericStatistics<int16_t>>();
	case PhysicalType::INT32:
		return make_unique<NumericStatistics<int32_t>>();
	case PhysicalType::INT64:
		return make_unique<NumericStatistics<int64_t>>();
	case PhysicalType::INT128:
		return make_unique<NumericStatistics<hugeint_t>>();
	case PhysicalType::FLOAT:
		return make_unique<NumericStatistics<float>>();
	case PhysicalType::DOUBLE:
		return make_unique<NumericStatistics<double>>();
	case PhysicalType::VARCHAR:
		return make_unique<StringStatistics>(type.id() == LogicalTypeId::BLOB);
	case PhysicalType::INTERVAL:
		return make_unique<BaseStatistics>();
	default:
		throw InternalException("Unimplemented type for base statistics");
	}
}

unique_ptr<BaseStatistics> BaseStatistics::Deserialize(Deserializer &source, LogicalType type) {
	auto has_null = source.Read<bool>();
	unique_ptr<BaseStatistics> result;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		result = NumericStatistics<int8_t>::Deserialize(source);
		break;
	case PhysicalType::INT16:
		result = NumericStatistics<int16_t>::Deserialize(source);
		break;
	case PhysicalType::INT32:
		result = NumericStatistics<int32_t>::Deserialize(source);
		break;
	case PhysicalType::INT64:
		result = NumericStatistics<int64_t>::Deserialize(source);
		break;
	case PhysicalType::INT128:
		result = NumericStatistics<hugeint_t>::Deserialize(source);
		break;
	case PhysicalType::FLOAT:
		result = NumericStatistics<float>::Deserialize(source);
		break;
	case PhysicalType::DOUBLE:
		result = NumericStatistics<double>::Deserialize(source);
		break;
	case PhysicalType::VARCHAR:
		result = StringStatistics::Deserialize(source);
		break;
	case PhysicalType::INTERVAL:
		result = make_unique<BaseStatistics>();
		break;
	default:
		throw InternalException("Unimplemented type for SEGMENT statistics");
	}
	result->has_null = has_null;
	return result;
}

string BaseStatistics::ToString() {
	return StringUtil::Format("Base Statistics [Has Null: %s]", has_null ? "true" : "false");
}

}
