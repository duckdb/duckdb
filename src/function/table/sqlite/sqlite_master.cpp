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
	index_t offset;
};

FunctionData *sqlite_master_init(ClientContext &context) {
	// initialize the function data structure
	return new SQLiteMasterData();
}

string GenerateQuery(CatalogEntry *entry) {
	// generate a query from a catalog entry
	if (entry->type == CatalogType::TABLE) {
		// FIXME: constraints
		stringstream ss;
		auto table = (TableCatalogEntry *)entry;
		ss << "CREATE TABLE " << table->name << "(";

		for (index_t i = 0; i < table->columns.size(); i++) {
			auto &column = table->columns[i];
			ss << column.name << " " << SQLTypeToString(column.type);
			if (i + 1 < table->columns.size()) {
				ss << ", ";
			}
		}

		ss << ");";
		return ss.str();
	} else {
		return "[Unknown]";
	}
}

void sqlite_master(ClientContext &context, DataChunk &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((SQLiteMasterData *)dataptr);
	if (!data.initialized) {
		// scan all the schemas
		auto &transaction = Transaction::GetTransaction(context);
		Catalog::GetCatalog(context).schemas.Scan(transaction, [&](CatalogEntry *entry) {
			auto schema = (SchemaCatalogEntry *)entry;
			schema->tables.Scan(transaction, [&](CatalogEntry *entry) { data.entries.push_back(entry); });
		});
		data.initialized = true;
	}

	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	index_t next = min(data.offset + STANDARD_VECTOR_SIZE, (index_t)data.entries.size());
	output.SetCardinality(next - data.offset);

	// start returning values
	// either fill up the chunk or return all the remaining columns
	for (index_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto &entry = data.entries[i];

		// return values:
		// "type", TypeId::VARCHAR
		const char *type_str;
		switch (entry->type) {
		case CatalogType::TABLE:
			type_str = "table";
			break;
		case CatalogType::SCHEMA:
			type_str = "schema";
			break;
		case CatalogType::TABLE_FUNCTION:
			type_str = "function";
			break;
		case CatalogType::VIEW:
			type_str = "view";
			break;
		default:
			type_str = "unknown";
		}
		output.SetValue(0, index, Value(type_str));
		// "name", TypeId::VARCHAR
		output.SetValue(1, index, Value(entry->name));
		// "tbl_name", TypeId::VARCHAR
		output.SetValue(2, index, Value(entry->name));
		// "rootpage", TypeId::INT32
		output.SetValue(3, index, Value::INTEGER(0));
		// "sql", TypeId::VARCHAR
		output.SetValue(4, index, Value(GenerateQuery(entry)));
	}
	data.offset = next;
}

} // namespace duckdb
