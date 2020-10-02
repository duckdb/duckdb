#include "duckdb/function/table/sqlite_functions.hpp"
#include "duckdb/parser/parsed_data/show_select_info.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

struct ShowSelectFunctionData : public TableFunctionData {
	ShowSelectFunctionData() : offset(0) {
	}

	//CatalogEntry *entry;
	idx_t offset;
};

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
