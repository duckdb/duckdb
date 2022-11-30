#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

struct PragmaDatabaseListData : public GlobalTableFunctionState {
	PragmaDatabaseListData() : index(0) {
	}

	vector<AttachedDatabase *> databases;
	idx_t index;
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
	auto result = make_unique<PragmaDatabaseListData>();
	auto &db_manager = DatabaseManager::Get(context);
	result->databases = db_manager.GetDatabases();
	return move(result);
}

void PragmaDatabaseListFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (PragmaDatabaseListData &)*data_p.global_state;

	idx_t count = 0;
	for (; count < STANDARD_VECTOR_SIZE && data.index < data.databases.size(); data.index++, count++) {
		output.data[0].SetValue(count, Value::INTEGER(data.index));
		output.data[1].SetValue(count, Value(data.databases[data.index]->GetName()));
		output.data[2].SetValue(count, Value(data.databases[data.index]->GetStorageManager().GetDBPath()));
	}
	output.SetCardinality(count);
}

void PragmaDatabaseList::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_database_list", {}, PragmaDatabaseListFunction, PragmaDatabaseListBind,
	                              PragmaDatabaseListInit));
}

} // namespace duckdb
