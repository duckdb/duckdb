#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

struct DuckDBIndexesData : public FunctionOperatorData {
	DuckDBIndexesData() : offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBIndexesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("index_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("index_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("is_unique");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("is_primary");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("expressions");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData> DuckDBIndexesInit(ClientContext &context, const FunctionData *bind_data,
                                                   const vector<column_t> &column_ids, TableFilterCollection *filters) {
	auto result = make_unique<DuckDBIndexesData>();

	// scan all the schemas for tables and collect themand collect them
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	};

	// check the temp schema as well
	context.temporary_objects->Scan(context, CatalogType::INDEX_ENTRY,
	                                [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

void DuckDBIndexesFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                           DataChunk *input, DataChunk &output) {
	auto &data = (DuckDBIndexesData &)*operator_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];

		auto &index = (IndexCatalogEntry &)*entry;
		// return values:

		// schema_name, VARCHAR
		output.SetValue(0, count, Value(index.schema->name));
		// schema_oid, BIGINT
		output.SetValue(1, count, Value::BIGINT(index.schema->oid));
		// index_name, VARCHAR
		output.SetValue(2, count, Value(index.name));
		// index_oid, BIGINT
		output.SetValue(3, count, Value::BIGINT(index.oid));
		// table_name, VARCHAR
		output.SetValue(4, count, Value(index.info->table));
		// table_oid, BIGINT
		// find the table in the catalog
		auto &catalog = Catalog::GetCatalog(context);
		auto table_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, index.info->schema, index.info->table);
		output.SetValue(5, count, Value::BIGINT(table_entry->oid));
		// is_unique, BOOLEAN
		output.SetValue(6, count, Value::BOOLEAN(index.index->IsUnique()));
		// is_primary, BOOLEAN
		output.SetValue(7, count, Value::BOOLEAN(index.index->IsPrimary()));
		// expressions, VARCHAR
		output.SetValue(8, count, Value());
		// sql, VARCHAR
		output.SetValue(9, count, Value(index.ToSQL()));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBIndexesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_indexes", {}, DuckDBIndexesFunction, DuckDBIndexesBind, DuckDBIndexesInit));
}

} // namespace duckdb
