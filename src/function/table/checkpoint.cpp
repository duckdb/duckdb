#include "duckdb/function/table/range.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

static unique_ptr<FunctionData> checkpoint_bind(ClientContext &context, vector<Value> &inputs,
                                            unordered_map<string, Value> &named_parameters,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
	return nullptr;
}

static void checkpoint_function(ClientContext &context, const FunctionData *bind_data_,
                            FunctionOperatorData *operator_state, DataChunk &output) {
	// FIXME: obtain lock on all connections and on connection manager to ensure no queries are running (besides us)
	auto &storage = StorageManager::GetStorageManager(context);
	storage.CreateCheckpoint();
}

void CheckpointFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction checkpoint("checkpoint", {}, checkpoint_function, checkpoint_bind);
	set.AddFunction(checkpoint);
}

} // namespace duckdb
