#include "duckdb/storage/table/numeric_statistics.hpp"
#include "duckdb/storage/table/string_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> BaseStatistics::Copy() {
	auto statistics = make_unique<BaseStatistics>();
	statistics->has_null = has_null;
	return statistics;
}

void BaseStatistics::Serialize(Serializer &serializer) {
	serializer.Write<bool>(has_null);
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

}
