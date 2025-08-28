#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

struct PragmaMemoryInfoData : public GlobalTableFunctionState {
	PragmaMemoryInfoData() : finished(false) {
	}

	Value buffer_pool_used_bytes;
	Value buffer_pool_max_bytes;
	bool finished;
};

static unique_ptr<FunctionData> PragmaMemoryInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("buffer_pool_used_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("buffer_pool_max_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> PragmaMemoryInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<PragmaMemoryInfoData>();

	auto &buffer_manager = BufferManager::GetBufferManager(context);
	result->buffer_pool_used_bytes = Value::BIGINT(NumericCast<int64_t>(buffer_manager.GetUsedMemory()));
	result->buffer_pool_max_bytes = Value::BIGINT(NumericCast<int64_t>(buffer_manager.GetMaxMemory()));

	return std::move(result);
}

void PragmaMemoryInfoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaMemoryInfoData>();

	if (data.finished) {
		// signal end of output
		return;
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, data.buffer_pool_used_bytes);
	output.SetValue(1, 0, data.buffer_pool_max_bytes);

	data.finished = true;
}

void PragmaMemoryInfo::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("pragma_memory_info", {}, PragmaMemoryInfoFunction, PragmaMemoryInfoBind, PragmaMemoryInfoInit));
}

} // namespace duckdb
