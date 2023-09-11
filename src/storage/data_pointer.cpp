#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

unique_ptr<ColumnSegmentState> ColumnSegmentState::Deserialize(Deserializer &deserializer) {
	auto compression_type = deserializer.Get<CompressionType>();
	auto &db = deserializer.Get<DatabaseInstance &>();
	auto &type = deserializer.Get<LogicalType &>();
	auto compression_function = DBConfig::GetConfig(db).GetCompressionFunction(compression_type, type.InternalType());
	if (!compression_function || !compression_function->deserialize_state) {
		throw SerializationException("Deserializing a ColumnSegmentState but could not find deserialize method");
	}
	return compression_function->deserialize_state(deserializer);
}

void DataPointer::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "row_start", row_start);
	serializer.WriteProperty(101, "tuple_count", tuple_count);
	serializer.WriteProperty(102, "block_pointer", block_pointer);
	serializer.WriteProperty(103, "compression_type", compression_type);
	serializer.WriteProperty(104, "statistics", statistics);
	serializer.WritePropertyWithDefault(105, "segment_state", segment_state, unique_ptr<ColumnSegmentState>());
}

DataPointer DataPointer::Deserialize(Deserializer &deserializer) {
	auto row_start = deserializer.ReadProperty<uint64_t>(100, "row_start");
	auto tuple_count = deserializer.ReadProperty<uint64_t>(101, "tuple_count");
	auto block_pointer = deserializer.ReadProperty<BlockPointer>(102, "block_pointer");
	auto compression_type = deserializer.ReadProperty<CompressionType>(103, "compression_type");
	auto statistics = deserializer.ReadProperty<BaseStatistics>(104, "statistics");
	DataPointer result(std::move(statistics));
	result.row_start = row_start;
	result.tuple_count = tuple_count;
	result.block_pointer = block_pointer;
	result.compression_type = compression_type;

	// deserialize the segment state (if any)
	deserializer.Set<CompressionType>(compression_type);
	deserializer.ReadPropertyWithDefault(105, "segment_state", result.segment_state, unique_ptr<ColumnSegmentState>());
	deserializer.Unset<CompressionType>();
	return result;
}

}
