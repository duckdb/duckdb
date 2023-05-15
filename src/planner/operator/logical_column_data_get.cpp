#include "duckdb/planner/operator/logical_column_data_get.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalColumnDataGet::LogicalColumnDataGet(idx_t table_index, vector<LogicalType> types,
                                           unique_ptr<ColumnDataCollection> collection)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection)) {
	D_ASSERT(types.size() > 0);
	chunk_types = std::move(types);
}

vector<ColumnBinding> LogicalColumnDataGet::GetColumnBindings() {
	return GenerateColumnBindings(table_index, chunk_types.size());
}

void LogicalColumnDataGet::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteRegularSerializableList(chunk_types);
	writer.WriteField(collection->ChunkCount());
	for (auto &chunk : collection->Chunks()) {
		chunk.Serialize(writer.GetSerializer());
	}
}

unique_ptr<LogicalOperator> LogicalColumnDataGet::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto chunk_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto chunk_count = reader.ReadRequired<idx_t>();
	auto collection = make_uniq<ColumnDataCollection>(state.gstate.context, chunk_types);
	for (idx_t i = 0; i < chunk_count; i++) {
		DataChunk chunk;
		chunk.Deserialize(reader.GetSource());
		collection->Append(chunk);
	}
	return make_uniq<LogicalColumnDataGet>(table_index, std::move(chunk_types), std::move(collection));
}

vector<idx_t> LogicalColumnDataGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalColumnDataGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
