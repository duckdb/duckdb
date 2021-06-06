#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBSchemasData : public FunctionOperatorData {
	DuckDBSchemasData() : offset(0) {
	}

	vector<SchemaCatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBSchemasBind(ClientContext &context, vector<Value> &inputs,
                                                              unordered_map<string, Value> &named_parameters,
                                                              vector<LogicalType> &input_table_types,
                                                              vector<string> &input_table_names,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	names.emplace_back("id");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("schema_owner");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("default_character_set_catalog");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("default_character_set_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("default_character_set_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("sql_path");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData> DuckDBSchemasInit(ClientContext &context, const FunctionData *bind_data,
                                                               const vector<column_t> &column_ids,
                                                               TableFilterCollection *filters) {
	auto result = make_unique<DuckDBSchemasData>();

	// scan all the schemas and collect them
	Catalog::GetCatalog(context).ScanSchemas(
	    context, [&](CatalogEntry *entry) { result->entries.push_back((SchemaCatalogEntry *)entry); });
	// get the temp schema as well
	result->entries.push_back(context.temporary_objects.get());

	return move(result);
}

void DuckDBSchemasFunction(ClientContext &context, const FunctionData *bind_data,
                                       FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &data = (DuckDBSchemasData &)*operator_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];

		// return values:
		// "id", PhysicalType::BIGINT
		output.SetValue(0, count, Value::BIGINT(entry->oid));
		// "catalog_name", PhysicalType::VARCHAR
		output.SetValue(1, count, Value());
		// "schema_name", PhysicalType::VARCHAR
		output.SetValue(2, count, Value(entry->name));
		// "schema_owner", PhysicalType::VARCHAR
		output.SetValue(3, count, Value());
		// "default_character_set_catalog", PhysicalType::VARCHAR
		output.SetValue(4, count, Value());
		// "default_character_set_schema", PhysicalType::VARCHAR
		output.SetValue(5, count, Value());
		// "default_character_set_name", PhysicalType::VARCHAR
		output.SetValue(6, count, Value());
		// "sql_path", PhysicalType::VARCHAR
		output.SetValue(7, count, Value());

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBSchemasFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_schemas", {}, DuckDBSchemasFunction,
	                              DuckDBSchemasBind, DuckDBSchemasInit));
}

} // namespace duckdb
