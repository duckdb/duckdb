#include "duckdb/planner/operator/logical_column_data_get.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static vector<column_t> GenerateColumnDataColumnIds(idx_t column_count) {
	vector<column_t> column_ids;
	column_ids.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		column_ids.push_back(i);
	}
	return column_ids;
}

LogicalColumnDataGet::LogicalColumnDataGet(TableIndex table_index, vector<LogicalType> types,
                                           unique_ptr<ColumnDataCollection> collection_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection_p)) {
	D_ASSERT(!types.empty());
	chunk_types = std::move(types);
	SetColumnIds(GenerateColumnDataColumnIds(chunk_types.size()));
}

LogicalColumnDataGet::LogicalColumnDataGet(TableIndex table_index, vector<LogicalType> types,
                                           ColumnDataCollection &to_scan)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index), collection(to_scan) {
	D_ASSERT(!types.empty());
	chunk_types = std::move(types);
	SetColumnIds(GenerateColumnDataColumnIds(chunk_types.size()));
}

LogicalColumnDataGet::LogicalColumnDataGet(TableIndex table_index, vector<LogicalType> types,
                                           optionally_owned_ptr<ColumnDataCollection> collection_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection_p)) {
	D_ASSERT(!types.empty());
	chunk_types = std::move(types);
	SetColumnIds(GenerateColumnDataColumnIds(chunk_types.size()));
}

void LogicalColumnDataGet::SetColumnIds(vector<column_t> column_ids_p) {
	for (auto column_id : column_ids_p) {
		if (column_id >= chunk_types.size()) {
			throw SerializationException("LogicalColumnDataGet column id %llu is out of bounds for %llu chunk types",
			                             column_id, chunk_types.size());
		}
	}
	column_ids = std::move(column_ids_p);
	ResolveTypes();
}

const vector<column_t> &LogicalColumnDataGet::GetColumnIds() const {
	return column_ids;
}

vector<ColumnBinding> LogicalColumnDataGet::GetColumnBindings() {
	return GenerateColumnBindings(table_index, column_ids.size());
}

vector<TableIndex> LogicalColumnDataGet::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalColumnDataGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

void LogicalColumnDataGet::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<TableIndex>(200, "table_index", table_index);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(201, "chunk_types", chunk_types);
	serializer.WritePropertyWithDefault<optionally_owned_ptr<ColumnDataCollection>>(202, "collection", collection);
	serializer.WritePropertyWithDefault<vector<column_t>>(203, "column_ids", column_ids,
	                                                      GenerateColumnDataColumnIds(chunk_types.size()));
}

unique_ptr<LogicalOperator> LogicalColumnDataGet::Deserialize(Deserializer &deserializer) {
	auto table_index = deserializer.ReadPropertyWithDefault<TableIndex>(200, "table_index");
	auto chunk_types = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(201, "chunk_types");
	auto collection =
	    deserializer.ReadPropertyWithDefault<optionally_owned_ptr<ColumnDataCollection>>(202, "collection");
	auto result = duckdb::unique_ptr<LogicalColumnDataGet>(
	    new LogicalColumnDataGet(table_index, std::move(chunk_types), std::move(collection)));
	if (deserializer.CanDeserializeProperty(203, "column_ids")) {
		vector<column_t> column_ids;
		deserializer.ReadProperty(203, "column_ids", column_ids);
		result->SetColumnIds(std::move(column_ids));
	}
	return std::move(result);
}

} // namespace duckdb
