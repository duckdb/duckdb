#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct PragmaDatabaseSizeData : public GlobalTableFunctionState {
	PragmaDatabaseSizeData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaDatabaseSizeBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_size");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("block_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("total_blocks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("used_blocks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("free_blocks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("wal_size");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("memory_usage");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("memory_limit");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> PragmaDatabaseSizeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<PragmaDatabaseSizeData>();
}

void PragmaDatabaseSizeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (PragmaDatabaseSizeData &)*data_p.global_state;
	if (data.finished) {
		return;
	}
	// FIXME: loop over all attached databases
	auto &db = context.db->GetDatabaseManager().GetDefaultDatabase();
	auto &storage = db.GetStorageManager();
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	auto ds = storage.GetDatabaseSize();

	output.SetCardinality(1);
	output.data[0].SetValue(0, Value(StringUtil::BytesToHumanReadableString(ds.bytes)));
	output.data[1].SetValue(0, Value::BIGINT(ds.block_size));
	output.data[2].SetValue(0, Value::BIGINT(ds.total_blocks));
	output.data[3].SetValue(0, Value::BIGINT(ds.used_blocks));
	output.data[4].SetValue(0, Value::BIGINT(ds.free_blocks));
	output.data[5].SetValue(0, Value(StringUtil::BytesToHumanReadableString(ds.wal_size)));

	output.data[6].SetValue(0, Value(StringUtil::BytesToHumanReadableString(buffer_manager.GetUsedMemory())));
	auto max_memory = buffer_manager.GetMaxMemory();
	output.data[7].SetValue(0, max_memory == (idx_t)-1 ? Value("Unlimited")
	                                                   : Value(StringUtil::BytesToHumanReadableString(max_memory)));

	data.finished = true;
}

void PragmaDatabaseSize::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_database_size", {}, PragmaDatabaseSizeFunction, PragmaDatabaseSizeBind,
	                              PragmaDatabaseSizeInit));
}

} // namespace duckdb
