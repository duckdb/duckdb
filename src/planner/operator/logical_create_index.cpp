#include "duckdb/planner/operator/logical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/function/function_serialization.hpp"

namespace duckdb {

void LogicalCreateIndex::Serialize(FieldWriter &writer) const {

	writer.WriteOptional(info);
	table.Serialize(writer.GetSerializer());
	FunctionSerializer::SerializeBase<TableFunction>(writer, function, bind_data.get());
	writer.WriteSerializableList(unbound_expressions);

	writer.Finalize();
}

unique_ptr<LogicalOperator> LogicalCreateIndex::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {

	auto &context = state.gstate.context;
	auto catalog_info = TableCatalogEntry::Deserialize(reader.GetSource(), context);

	auto table =
	    Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, catalog_info->schema, catalog_info->table);
	auto unbound_expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);

	auto create_info = reader.ReadOptional<CreateInfo>(nullptr);
	if (create_info->type != CatalogType::INDEX_ENTRY) {
		throw InternalException("Unexpected type: '%s', expected '%s'", CatalogTypeToString(create_info->type),
		                        CatalogTypeToString(CatalogType::INDEX_ENTRY));
	}

	CreateInfo *raw_create_info_ptr = create_info.release();
	CreateIndexInfo *raw_create_index_info_ptr = static_cast<CreateIndexInfo *>(raw_create_info_ptr);
	unique_ptr<CreateIndexInfo> uptr_create_index_info = unique_ptr<CreateIndexInfo> {raw_create_index_info_ptr};
	auto info = unique_ptr<CreateIndexInfo> {static_cast<CreateIndexInfo *>(create_info.release())};

	unique_ptr<FunctionData> bind_data;
	bool has_deserialize;
	auto function = FunctionSerializer::DeserializeBaseInternal<TableFunction, TableFunctionCatalogEntry>(
	    reader, state.gstate, CatalogType::TABLE_FUNCTION_ENTRY, bind_data, has_deserialize);

	reader.Finalize();
	return make_unique<LogicalCreateIndex>(std::move(bind_data), std::move(info), std::move(unbound_expressions), *table,
	                                       std::move(function));
}

} // namespace duckdb
