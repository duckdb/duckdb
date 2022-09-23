#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBSchemasData : public GlobalTableFunctionState {
	DuckDBSchemasData() : offset(0) {
	}

	vector<SchemaCatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBSchemasBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBSchemasInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBSchemasData>();

	// scan all the schemas and collect them
	Catalog::GetCatalog(context).ScanSchemas(
	    context, [&](CatalogEntry *entry) { result->entries.push_back((SchemaCatalogEntry *)entry); });
	// get the temp schema as well
	result->entries.push_back(ClientData::Get(context).temporary_objects.get());

	return move(result);
}

void DuckDBSchemasFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBSchemasData &)*data_p.global_state;
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
		// "oid", PhysicalType::BIGINT
		output.SetValue(0, count, Value::BIGINT(entry->oid));
		// "schema_name", PhysicalType::VARCHAR
		output.SetValue(1, count, Value(entry->name));
		// "internal", PhysicalType::BOOLEAN
		output.SetValue(2, count, Value::BOOLEAN(entry->internal));
		// "sql", PhysicalType::VARCHAR
		output.SetValue(3, count, Value());

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBSchemasFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_schemas", {}, DuckDBSchemasFunction, DuckDBSchemasBind, DuckDBSchemasInit));
}

} // namespace duckdb
