#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

struct PragmaTableFunctionData : public TableFunctionData {
	PragmaTableFunctionData() : entry(nullptr), offset(0) {
	}

	CatalogEntry *entry;
	idx_t offset;
};

static unique_ptr<FunctionData> pragma_table_info_bind(ClientContext &context, vector<Value> inputs,
                                                       vector<SQLType> &return_types, vector<string> &names) {
	names.push_back("cid");
	return_types.push_back(SQLType::INTEGER);

	names.push_back("name");
	return_types.push_back(SQLType::VARCHAR);

	names.push_back("type");
	return_types.push_back(SQLType::VARCHAR);

	names.push_back("notnull");
	return_types.push_back(SQLType::BOOLEAN);

	names.push_back("dflt_value");
	return_types.push_back(SQLType::VARCHAR);

	names.push_back("pk");
	return_types.push_back(SQLType::BOOLEAN);

	return make_unique<PragmaTableFunctionData>();
}

static void pragma_table_info_table(PragmaTableFunctionData &data, TableCatalogEntry *table, DataChunk &output) {
	if (data.offset >= table->columns.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)table->columns.size());
	output.SetCardinality(next - data.offset);

	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto &column = table->columns[i];
		// return values:
		// "cid", TypeId::INT32
		assert(column.oid < (idx_t)std::numeric_limits<int32_t>::max());

		output.SetValue(0, index, Value::INTEGER((int32_t)column.oid));
		// "name", TypeId::VARCHAR
		output.SetValue(1, index, Value(column.name));
		// "type", TypeId::VARCHAR
		output.SetValue(2, index, Value(SQLTypeToString(column.type)));
		// "notnull", TypeId::BOOL
		// FIXME: look at constraints
		output.SetValue(3, index, Value::BOOLEAN(false));
		// "dflt_value", TypeId::VARCHAR
		Value def_value = column.default_value ? Value(column.default_value->ToString()) : Value();
		output.SetValue(4, index, def_value);
		// "pk", TypeId::BOOL
		// FIXME: look at constraints
		output.SetValue(5, index, Value::BOOLEAN(false));
	}
	data.offset = next;
}

static void pragma_table_info_view(PragmaTableFunctionData &data, ViewCatalogEntry *view, DataChunk &output) {
	if (data.offset >= view->types.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)view->types.size());
	output.SetCardinality(next - data.offset);

	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto type = view->types[index];
		auto &name = view->aliases[index];
		// return values:
		// "cid", TypeId::INT32

		output.SetValue(0, index, Value::INTEGER((int32_t)index));
		// "name", TypeId::VARCHAR
		output.SetValue(1, index, Value(name));
		// "type", TypeId::VARCHAR
		output.SetValue(2, index, Value(SQLTypeToString(type)));
		// "notnull", TypeId::BOOL
		output.SetValue(3, index, Value::BOOLEAN(false));
		// "dflt_value", TypeId::VARCHAR
		output.SetValue(4, index, Value());
		// "pk", TypeId::BOOL
		output.SetValue(5, index, Value::BOOLEAN(false));
	}
	data.offset = next;
}

static void pragma_table_info(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((PragmaTableFunctionData *)dataptr);
	if (!data.entry) {
		// first call: load the entry from the catalog
		assert(input.size() == 1);

		string schema, table_name;
		auto range_var = input[0].GetValue<string>();
		Catalog::ParseRangeVar(range_var, schema, table_name);

		// look up the table name in the catalog
		auto &catalog = Catalog::GetCatalog(context);
		data.entry = catalog.GetEntry(context, CatalogType::TABLE, schema, table_name);
	}
	switch (data.entry->type) {
	case CatalogType::TABLE:
		pragma_table_info_table(data, (TableCatalogEntry *)data.entry, output);
		break;
	case CatalogType::VIEW:
		pragma_table_info_view(data, (ViewCatalogEntry *)data.entry, output);
		break;
	default:
		throw NotImplementedException("Unimplemented catalog type for pragma_table_info");
	}
}

void PragmaTableInfo::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("pragma_table_info", {SQLType::VARCHAR}, pragma_table_info_bind, pragma_table_info, nullptr));
}

} // namespace duckdb
