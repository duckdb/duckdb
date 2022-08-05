#include "duckdb/planner/operator/logical_chunk_get.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

//
//
////! The table index in the current bind context
//idx_t table_index;
////! The types of the chunk
//vector<LogicalType> chunk_types;
////! The chunk collection to scan
//unique_ptr<ChunkCollection> collection;

void LogicalChunkGet::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteRegularSerializableList(chunk_types);
	writer.WriteField(collection->ChunkCount());
	for (auto& chunk : collection->Chunks()) {
		chunk->Serialize(writer.GetSerializer());
	}
}

unique_ptr<LogicalOperator> LogicalChunkGet::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                         FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto chunk_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto chunk_count = reader.ReadRequired<idx_t>();
	auto collection = make_unique<ChunkCollection>(context);
	for (idx_t i = 0; i < chunk_count; i++) {
		DataChunk chunk;
		chunk.Deserialize(reader.GetSource());
		collection->Append(chunk);
	}
	return make_unique<LogicalChunkGet>(table_index, move(chunk_types), move(collection));
}


} // namespace duckdb
