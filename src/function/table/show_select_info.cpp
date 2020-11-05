#include "duckdb/function/table/sqlite_functions.hpp"
#include "duckdb/parser/parsed_data/show_select_info.hpp"

#include "duckdb/catalog/catalog.hpp"
//#include "duckdb/common/exception.hpp"

//#include <algorithm>

using namespace std;

namespace duckdb {

struct ShowSelectFunctionData : public TableFunctionData {
	ShowSelectFunctionData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<FunctionData> show_select_info_bind(ClientContext &context, vector<Value> inputs,
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

	return make_unique<ShowSelectFunctionData>();
}

static void show_select_info_schema(ShowSelectFunctionData &data, ShowSelectInfo *info, DataChunk &output) {
	if (data.offset >= info->types.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)info->types.size());
	output.SetCardinality(next - data.offset);

	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto type = info->types[index];
		auto &name = info->aliases[index];
		// return values:
		// "cid", TypeId::INT32

		output.SetValue(0, index, Value::INTEGER((int32_t)index));
		// "name", TypeId::VARCHAR
		//output.SetValue(1, index, Value(name));
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

static void show_select_info(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	/*auto &data = *((ShowSelectFunctionData *)dataptr);

  auto info = CreateInfo(CatalogType::VIEW, DEFAULT_SCHEMA)
  data.entry.Initialize(info);

	if (!data.entry) {
		// first call: load the entry from the catalog
		assert(input.size() == 1);

		string schema, table_name;
		auto range_var = input[0].GetValue<string>();
		Catalog::ParseRangeVar(range_var, schema, table_name);

		// look up the table name in the catalog
		auto &catalog = Catalog::GetCatalog(context);
		data.entry = catalog.GetEntry(context, CatalogType::TABLE, schema, table_name);
	}*/

}

void ShowSelectTableInfo::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("show_select_info", {SQLType::VARCHAR}, show_select_info_bind, show_select_info, nullptr));
}

} // namespace duckdb
