#include "duckdb/function/table/range.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

static unique_ptr<FunctionData> checkpoint_bind(ClientContext &context, vector<Value> &inputs,
                                                unordered_map<string, Value> &named_parameters,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
	return nullptr;
}

template <bool FORCE>
static void checkpoint_function(ClientContext &context, const FunctionData *bind_data_,
                                FunctionOperatorData *operator_state, DataChunk &output) {
	auto &transaction_manager = TransactionManager::Get(context);
	transaction_manager.Checkpoint(context, FORCE);
}

void CheckpointFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction checkpoint("checkpoint", {}, checkpoint_function<false>, checkpoint_bind);
	set.AddFunction(checkpoint);
	TableFunction force_checkpoint("force_checkpoint", {}, checkpoint_function<true>, checkpoint_bind);
	set.AddFunction(force_checkpoint);
}

} // namespace duckdb
