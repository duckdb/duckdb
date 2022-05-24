#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/matview_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBMatViewsData : public FunctionOperatorData {
	DuckDBMatViewsData() : offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBMatViewsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("matview_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("matview_oid");
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

unique_ptr<FunctionOperatorData> DuckDBMatViewsInit(ClientContext &context, const FunctionData *bind_data,
                                                    const vector<column_t> &column_ids,
                                                    TableFilterCollection *filters) {
	auto result = make_unique<DuckDBMatViewsData>();

	// scan all the schemas for tables and collect them and collect them
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::MATVIEW_ENTRY,
		             [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	};

	// check the temp schema as well
	ClientData::Get(context).temporary_objects->Scan(context, CatalogType::MATVIEW_ENTRY,
	                                                 [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

void DuckDBMatViewsFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                            DataChunk &output) {
	auto &data = (DuckDBMatViewsData &)*operator_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];

		if (entry->type != CatalogType::MATVIEW_ENTRY) {
			continue;
		}
		auto &view = (MatViewCatalogEntry &)*entry;

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
		output.SetValue(6, count, Value::BIGINT(view.columns.size()));
		// sql, LogicalType::VARCHAR
		output.SetValue(7, count, Value(view.ToSQL()));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBMatViewsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_matviews", {}, DuckDBMatViewsFunction, DuckDBMatViewsBind, DuckDBMatViewsInit));
}

} // namespace duckdb
