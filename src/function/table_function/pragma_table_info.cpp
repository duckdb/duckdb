#include "function/table_function/pragma_table_info.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

struct PragmaTableFunctionData : public FunctionData {
	PragmaTableFunctionData() : entry(nullptr), offset(0) {
	}

	unique_ptr<FunctionData> Copy() override {
		throw NotImplementedException("Copy not required for table-producing function");
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
		if (input.column_count != 1 || input.data[0].type != TypeId::VARCHAR) {
			throw Exception("Expected a single table name as input");
		}
		auto table_name = input.data[0].GetValue(0).str_value;
		// look up the table name in the catalog
		auto &catalog = context.catalog;
		data.entry = catalog.GetTable(context.ActiveTransaction(), DEFAULT_SCHEMA, table_name);
	}

	if (data.offset >= data.entry->columns.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	index_t next = min(data.offset + STANDARD_VECTOR_SIZE, (index_t)data.entry->columns.size());
	index_t output_count = next - data.offset;
	for (index_t j = 0; j < output.column_count; j++) {
		output.data[j].count = output_count;
	}

	for (index_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto &column = data.entry->columns[i];
		// return values:
		// "cid", TypeId::INTEGER
		assert(column.oid < std::numeric_limits<int32_t>::max());

		output.data[0].SetValue(index, Value::INTEGER((int32_t)column.oid));
		// "name", TypeId::VARCHAR
		output.data[1].SetValue(index, Value(column.name));
		// "type", TypeId::VARCHAR
		output.data[2].SetValue(index, Value(SQLTypeToString(column.type)));
		// "notnull", TypeId::BOOLEAN
		// FIXME: look at constraints
		output.data[3].SetValue(index, Value::BOOLEAN(false));
		// "dflt_value", TypeId::VARCHAR
		string def_value = column.default_value ? column.default_value->ToString() : "NULL";
		output.data[4].SetValue(index, Value(def_value));
		// "pk", TypeId::BOOLEAN
		// FIXME: look at constraints
		output.data[5].SetValue(index, Value::BOOLEAN(false));
	}
	data.offset = next;
}

} // namespace duckdb
