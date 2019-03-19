#include "function/table_function/pragma_table_info.hpp"

#include "catalog/catalog.hpp"
#include "common/exception.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

using namespace std;

namespace duckdb {
namespace function {

struct PragmaTableFunctionData : public TableFunctionData {
	PragmaTableFunctionData() : entry(nullptr), offset(0) {
	}

	TableCatalogEntry *entry;
	size_t offset;
};

TableFunctionData *pragma_table_info_init(ClientContext &context) {
	// initialize the function data structure
	return new PragmaTableFunctionData();
}

void pragma_table_info(ClientContext &context, DataChunk &input, DataChunk &output, TableFunctionData *dataptr) {
	auto &data = *((PragmaTableFunctionData *)dataptr);
	if (!data.entry) {
		// first call: load the entry from the catalog
		if (input.size() != 1) {
			throw Exception("Expected a single table name as input");
		}
		if (input.column_count != 1 || input.data[0].type != TypeId::VARCHAR) {
			throw Exception("Expected a single table name as input");
		}
		auto table_name = input.data[0].GetValue(0).str_value;
		// look up the table name in the catalog
		auto &catalog = context.db.catalog;
		data.entry = catalog.GetTable(context.ActiveTransaction(), DEFAULT_SCHEMA, table_name);
	}

	if (data.offset >= data.entry->columns.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	size_t next = min(data.offset + STANDARD_VECTOR_SIZE, data.entry->columns.size());
	size_t output_count = next - data.offset;
	for (size_t j = 0; j < output.column_count; j++) {
		output.data[j].count = output_count;
	}

	for (size_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto &column = data.entry->columns[i];
		// return values:
		// "cid", TypeId::INTEGER
		output.data[0].SetValue(index, Value::INTEGER(column.oid));
		// "name", TypeId::VARCHAR
		output.data[1].SetValue(index, Value(column.name));
		// "type", TypeId::VARCHAR
		output.data[2].SetValue(index, Value(SQLTypeToString(column.type)));
		// "notnull", TypeId::BOOLEAN
		// FIXME: look at constraints
		output.data[3].SetValue(index, Value::BOOLEAN(false));
		// "dflt_value", TypeId::BOOLEAN
		output.data[4].SetValue(index, Value::BOOLEAN(column.has_default));
		// "pk", TypeId::BOOLEAN
		// FIXME: look at constraints
		output.data[5].SetValue(index, Value::BOOLEAN(false));
	}
	data.offset = next;
}

} // namespace function
} // namespace duckdb
