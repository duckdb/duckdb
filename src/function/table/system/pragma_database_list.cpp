#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

struct PragmaDatabaseListData : public GlobalTableFunctionState {
	PragmaDatabaseListData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaDatabaseListBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("seq");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("file");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> PragmaDatabaseListInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<PragmaDatabaseListData>();
}

void PragmaDatabaseListFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (PragmaDatabaseListData &)*data_p.global_state;
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
	set.AddFunction(TableFunction("pragma_database_list", {}, PragmaDatabaseListFunction, PragmaDatabaseListBind,
	                              PragmaDatabaseListInit));
}

} // namespace duckdb
