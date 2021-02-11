#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

BaseStatistics::BaseStatistics(LogicalType type) : type(move(type)), has_null(false) {
}

BaseStatistics::~BaseStatistics() {
}

unique_ptr<BaseStatistics> BaseStatistics::Copy() {
	auto statistics = make_unique<BaseStatistics>(type);
	statistics->has_null = has_null;
	return statistics;
}

void BaseStatistics::Merge(const BaseStatistics &other) {
	has_null = has_null || other.has_null;
}

unique_ptr<BaseStatistics> BaseStatistics::CreateEmpty(LogicalType type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return make_unique<NumericStatistics>(move(type));
	case PhysicalType::VARCHAR:
		return make_unique<StringStatistics>(move(type));
	case PhysicalType::INTERVAL:
		return make_unique<BaseStatistics>(move(type));
	default:
		return nullptr;
	}
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
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		result = NumericStatistics::Deserialize(source, move(type));
		break;
	case PhysicalType::VARCHAR:
		result = StringStatistics::Deserialize(source, move(type));
		break;
	case PhysicalType::INTERVAL:
		result = make_unique<BaseStatistics>(move(type));
		break;
	default:
		throw InternalException("Unimplemented type for statistics deserialization");
	}
	result->has_null = has_null;
	return result;
}

string BaseStatistics::ToString() {
	return StringUtil::Format("Base Statistics [Has Null: %s]", has_null ? "true" : "false");
}

void BaseStatistics::Verify(Vector &vector, idx_t count) {
	D_ASSERT(vector.type == this->type);
	if (!has_null) {
		if (VectorOperations::HasNull(vector, count)) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as not having NULL values, but vector contains null values: %s",
			    vector.ToString(count));
		}
	}
}

} // namespace duckdb
