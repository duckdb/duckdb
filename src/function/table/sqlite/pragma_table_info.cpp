#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

struct PragmaTableFunctionData : public TableFunctionData {
	PragmaTableFunctionData() : entry(nullptr), offset(0) {
	}

	TableCatalogEntry *entry;
	index_t offset;
};

FunctionData *pragma_table_info_init(ClientContext &context) {
	// initialize the function data structure
	return new PragmaTableFunctionData();
}

void pragma_table_info(ClientContext &context, DataChunk &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((PragmaTableFunctionData *)dataptr);
	if (!data.entry) {
		// first call: load the entry from the catalog
		if (input.size() != 1) {
			throw Exception("Expected a single table name as input");
		}
		if (input.column_count() != 1 || input.data[0].type != TypeId::VARCHAR) {
			throw Exception("Expected a single table name as input");
		}
		auto table_name = input.GetValue(0, 0).str_value;
		// look up the table name in the catalog
		auto &catalog = Catalog::GetCatalog(context);
		data.entry = catalog.GetEntry<TableCatalogEntry>(context, DEFAULT_SCHEMA, table_name);
	}

	if (data.offset >= data.entry->columns.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	index_t next = min(data.offset + STANDARD_VECTOR_SIZE, (index_t)data.entry->columns.size());
	output.SetCardinality(next - data.offset);

	for (index_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto &column = data.entry->columns[i];
		// return values:
		// "cid", TypeId::INT32
		assert(column.oid < (index_t)std::numeric_limits<int32_t>::max());

		output.SetValue(0, index, Value::INTEGER((int32_t)column.oid));
		// "name", TypeId::VARCHAR
		output.SetValue(1, index, Value(column.name));
		// "type", TypeId::VARCHAR
		output.SetValue(2, index, Value(SQLTypeToString(column.type)));
		// "notnull", TypeId::BOOL
		// FIXME: look at constraints
		output.SetValue(3, index, Value::BOOLEAN(false));
		// "dflt_value", TypeId::VARCHAR
		string def_value = column.default_value ? column.default_value->ToString() : "NULL";
		output.SetValue(4, index, Value(def_value));
		// "pk", TypeId::BOOL
		// FIXME: look at constraints
		output.SetValue(5, index, Value::BOOLEAN(false));
	}
	data.offset = next;
}

} // namespace duckdb
