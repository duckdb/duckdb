#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

struct SQLiteMasterData : public TableFunctionData {
	SQLiteMasterData() : initialized(false), offset(0) {
	}

	bool initialized;
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

	// initialize the function data structure
	return make_unique<SQLiteMasterData>();
}

void sqlite_master(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((SQLiteMasterData *)dataptr);
	assert(input.size() == 0);
	if (!data.initialized) {
		// scan all the schemas
		auto &transaction = Transaction::GetTransaction(context);
		Catalog::GetCatalog(context).schemas->Scan(transaction, [&](CatalogEntry *entry) {
			auto schema = (SchemaCatalogEntry *)entry;
			schema->tables.Scan(transaction, [&](CatalogEntry *entry) { data.entries.push_back(entry); });
		});
		data.initialized = true;
	}

	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)data.entries.size());
	output.SetCardinality(next - data.offset);

	// start returning values
	// either fill up the chunk or return all the remaining columns
	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto &entry = data.entries[i];

		// return values:
		// "type", PhysicalType::VARCHAR
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
		default:
			type_str = "unknown";
		}
		output.SetValue(0, index, Value(type_str));
		// "name", PhysicalType::VARCHAR
		output.SetValue(1, index, Value(entry->name));
		// "tbl_name", PhysicalType::VARCHAR
		output.SetValue(2, index, Value(entry->name));
		// "rootpage", PhysicalType::INT32
		output.SetValue(3, index, Value::INTEGER(0));
		// "sql", PhysicalType::VARCHAR
		output.SetValue(4, index, Value(entry->ToSQL()));
	}
	data.offset = next;
}

void SQLiteMaster::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sqlite_master", {}, sqlite_master_bind, sqlite_master, nullptr));
}

} // namespace duckdb
