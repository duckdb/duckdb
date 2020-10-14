#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

struct SQLiteMasterData : public FunctionOperatorData {
	SQLiteMasterData() : offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> sqlite_master_bind(ClientContext &context, vector<Value> &inputs,
                                                   unordered_map<string, Value> &named_parameters,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("type");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("tbl_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("rootpage");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("sql");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData> sqlite_master_init(ClientContext &context, const FunctionData *bind_data,
                                                    vector<column_t> &column_ids,
                                                    unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	auto result = make_unique<SQLiteMasterData>();

	// scan all the schemas for tables and views and collect them
	Catalog::GetCatalog(context).schemas->Scan(context, [&](CatalogEntry *entry) {
		auto schema = (SchemaCatalogEntry *)entry;
		schema->tables.Scan(context, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
		schema->indexes.Scan(context, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	});

	return move(result);
}

void sqlite_master(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                   DataChunk &output) {
	auto &data = (SQLiteMasterData &)*operator_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}

	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		if (entry->internal) {
			continue;
		}

		// return values:
		// "type", PhysicalType::VARCHAR
		string table_name = entry->name;
		const char *type_str;
		switch (entry->type) {
		case CatalogType::TABLE_ENTRY:
			type_str = "table";
			break;
		case CatalogType::SCHEMA_ENTRY:
			type_str = "schema";
			break;
		case CatalogType::TABLE_FUNCTION_ENTRY:
			type_str = "function";
			break;
		case CatalogType::VIEW_ENTRY:
			type_str = "view";
			break;
		case CatalogType::INDEX_ENTRY: {
			auto &index = (IndexCatalogEntry &) *entry;
			table_name = index.info->table;
			type_str = "index";
			break;
		}
		default:
			type_str = "unknown";
		}
		output.SetValue(0, count, Value(type_str));
		// "name", PhysicalType::VARCHAR
		output.SetValue(1, count, Value(entry->name));
		// "tbl_name", PhysicalType::VARCHAR
		output.SetValue(2, count, Value(table_name));
		// "rootpage", PhysicalType::INT32
		output.SetValue(3, count, Value::INTEGER(0));
		// "sql", PhysicalType::VARCHAR
		output.SetValue(4, count, Value(entry->ToSQL()));
		count++;
	}
	output.SetCardinality(count);
}

void SQLiteMaster::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sqlite_master", {}, sqlite_master, sqlite_master_bind, sqlite_master_init));
}

} // namespace duckdb
