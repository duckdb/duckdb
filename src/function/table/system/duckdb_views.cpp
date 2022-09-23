#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBViewsData : public GlobalTableFunctionState {
	DuckDBViewsData() : offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBViewsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("view_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("view_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("temporary");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("column_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBViewsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBViewsData>();

	// scan all the schemas for tables and collect themand collect them
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::VIEW_ENTRY, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	};

	// check the temp schema as well
	ClientData::Get(context).temporary_objects->Scan(context, CatalogType::VIEW_ENTRY,
	                                                 [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

void DuckDBViewsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBViewsData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];

		if (entry->type != CatalogType::VIEW_ENTRY) {
			continue;
		}
		auto &view = (ViewCatalogEntry &)*entry;

		// return values:
		// schema_name, LogicalType::VARCHAR
		output.SetValue(0, count, Value(view.schema->name));
		// schema_oid, LogicalType::BIGINT
		output.SetValue(1, count, Value::BIGINT(view.schema->oid));
		// view_name, LogicalType::VARCHAR
		output.SetValue(2, count, Value(view.name));
		// view_oid, LogicalType::BIGINT
		output.SetValue(3, count, Value::BIGINT(view.oid));
		// internal, LogicalType::BOOLEAN
		output.SetValue(4, count, Value::BOOLEAN(view.internal));
		// temporary, LogicalType::BOOLEAN
		output.SetValue(5, count, Value::BOOLEAN(view.temporary));
		// column_count, LogicalType::BIGINT
		output.SetValue(6, count, Value::BIGINT(view.types.size()));
		// sql, LogicalType::VARCHAR
		output.SetValue(7, count, Value(view.ToSQL()));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBViewsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_views", {}, DuckDBViewsFunction, DuckDBViewsBind, DuckDBViewsInit));
}

} // namespace duckdb
