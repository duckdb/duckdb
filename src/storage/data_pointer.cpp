#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

DataPointer::DataPointer(BaseStatistics stats) : statistics(std::move(stats)) {
}

DataPointer::DataPointer(DataPointer &&other) noexcept : statistics(std::move(other.statistics)) {
	std::swap(row_start, other.row_start);
	std::swap(tuple_count, other.tuple_count);
	std::swap(block_pointer, other.block_pointer);
	std::swap(compression_type, other.compression_type);
	std::swap(segment_state, other.segment_state);
}

DataPointer &DataPointer::operator=(DataPointer &&other) noexcept {
	std::swap(row_start, other.row_start);
	std::swap(tuple_count, other.tuple_count);
	std::swap(block_pointer, other.block_pointer);
	std::swap(compression_type, other.compression_type);
	std::swap(statistics, other.statistics);
	std::swap(segment_state, other.segment_state);
	return *this;
}

unique_ptr<ColumnSegmentState> ColumnSegmentState::Deserialize(Deserializer &deserializer) {
	auto compression_type = deserializer.Get<CompressionType>();
	auto &db = deserializer.Get<DatabaseInstance &>();
	auto &type = deserializer.Get<const LogicalType &>();

	auto compression_function = DBConfig::GetConfig(db).GetCompressionFunction(compression_type, type.InternalType());
	if (!compression_function || !compression_function->deserialize_state) {
		throw SerializationException("Deserializing a ColumnSegmentState but could not find deserialize method");
	}
	return compression_function->deserialize_state(deserializer);
}

} // namespace duckdb
