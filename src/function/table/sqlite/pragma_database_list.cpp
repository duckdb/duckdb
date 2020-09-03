#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/storage/storage_manager.hpp"

using namespace std;

namespace duckdb {

struct PragmaDatabaseListData : public TableFunctionData {
	PragmaDatabaseListData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> pragma_database_list_bind(ClientContext &context, vector<Value> &inputs,
                                                          unordered_map<string, Value> &named_parameters,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("seq");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("file");
	return_types.push_back(LogicalType::VARCHAR);

	// initialize the function data structure
	return make_unique<PragmaDatabaseListData>();
}

void pragma_database_list(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((PragmaDatabaseListData *)dataptr);
	if (data.finished) {
		return;
	}

	output.SetCardinality(1);
	output.data[0].SetValue(0, Value::INTEGER(0));
	output.data[1].SetValue(0, Value("main"));
	output.data[2].SetValue(0, Value(StorageManager::GetStorageManager(context).GetDBPath()));

	data.finished = true;
}

void PragmaDatabaseList::RegisterFunction(BuiltinFunctions &set) {
	// FIXME
	// set.AddFunction(
	//     TableFunction("pragma_database_list", {}, pragma_database_list_bind, pragma_database_list, nullptr));
}

} // namespace duckdb
