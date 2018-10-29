
#include "function/table_function/sqlite_master.hpp"

#include "catalog/catalog.hpp"
#include "common/exception.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"

using namespace std;

namespace duckdb {
namespace function {

struct SQLiteMasterData : public TableFunctionData {
	SQLiteMasterData() : initialized(false), offset(0) {
	}

	bool initialized;
	vector<CatalogEntry *> entries;
	size_t offset;
};

TableFunctionData *sqlite_master_init(ClientContext &context) {
	// initialize the function data structure
	return new SQLiteMasterData();
}

void sqlite_master(ClientContext &context, DataChunk &input, DataChunk &output,
                   TableFunctionData *dataptr) {
	auto &data = *((SQLiteMasterData *)dataptr);
	if (!data.initialized) {
		// scan all the schemas
		auto &transaction = context.ActiveTransaction();
		context.db.catalog.schemas.Scan(transaction, [&](CatalogEntry *entry) {
			auto schema = (SchemaCatalogEntry *)entry;
			schema->tables.Scan(transaction, [&](CatalogEntry *entry) {
				data.entries.push_back(entry);
			});
		});
		data.initialized = true;
	}

	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	size_t next = min(data.offset + STANDARD_VECTOR_SIZE, data.entries.size());

	size_t output_count = next - data.offset;
	for (size_t j = 0; j < output.column_count; j++) {
		output.data[j].count = output_count;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	for (size_t i = data.offset; i < next; i++) {
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
		output.data[0].SetValue(index, Value(type_str));
		// "name", TypeId::VARCHAR
		output.data[1].SetValue(index, Value(entry->name));
		// "tbl_name", TypeId::VARCHAR
		output.data[2].SetValue(index, Value(entry->name));
		// "rootpage", TypeId::INTEGER
		output.data[3].SetValue(index, Value::INTEGER(0));
		// "sql", TypeId::VARCHAR
		output.data[4].SetValue(
		    index, Value("CREATE TABLE...")); // FIXME: generate SQL from table
		                                      // definition
	}
	data.offset = next;
}

} // namespace function
} // namespace duckdb
