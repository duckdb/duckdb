#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

struct PragmaDatabaseSizeData : public FunctionOperatorData {
	PragmaDatabaseSizeData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaDatabaseSizeBind(ClientContext &context, vector<Value> &inputs,
                                                       unordered_map<string, Value> &named_parameters,
                                                       vector<LogicalType> &input_table_types,
                                                       vector<string> &input_table_names,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_size");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("block_size");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("total_blocks");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("used_blocks");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("free_blocks");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("wal_size");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("memory_usage");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("memory_limit");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData> PragmaDatabaseSizeInit(ClientContext &context, const FunctionData *bind_data,
                                                        const vector<column_t> &column_ids,
                                                        TableFilterCollection *filters) {
	return make_unique<PragmaDatabaseSizeData>();
}

static string BytesToHumanReadableString(idx_t bytes) {
	string db_size;
	auto kilobytes = bytes / 1000;
	auto megabytes = kilobytes / 1000;
	kilobytes -= megabytes * 1000;
	auto gigabytes = megabytes / 1000;
	megabytes -= gigabytes * 1000;
	auto terabytes = gigabytes / 1000;
	gigabytes -= terabytes * 1000;
	if (terabytes > 0) {
		return to_string(terabytes) + "." + to_string(gigabytes / 100) + "TB";
	} else if (gigabytes > 0) {
		return to_string(gigabytes) + "." + to_string(megabytes / 100) + "GB";
	} else if (megabytes > 0) {
		return to_string(megabytes) + "." + to_string(kilobytes / 100) + "MB";
	} else if (kilobytes > 0) {
		return to_string(kilobytes) + "KB";
	} else {
		return to_string(bytes) + " bytes";
	}
}

void PragmaDatabaseSizeFunction(ClientContext &context, const FunctionData *bind_data,
                                FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &data = (PragmaDatabaseSizeData &)*operator_state;
	if (data.finished) {
		return;
	}
	auto &storage = StorageManager::GetStorageManager(context);
	auto &block_manager = BlockManager::GetBlockManager(context);
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	output.SetCardinality(1);
	if (!storage.InMemory()) {
		auto total_blocks = block_manager.TotalBlocks();
		auto block_size = Storage::BLOCK_ALLOC_SIZE;
		auto free_blocks = block_manager.FreeBlocks();
		auto used_blocks = total_blocks - free_blocks;
		auto bytes = (total_blocks * block_size);
		auto wal_size = storage.GetWriteAheadLog()->GetWALSize();
		output.data[0].SetValue(0, Value(BytesToHumanReadableString(bytes)));
		output.data[1].SetValue(0, Value::BIGINT(block_size));
		output.data[2].SetValue(0, Value::BIGINT(total_blocks));
		output.data[3].SetValue(0, Value::BIGINT(used_blocks));
		output.data[4].SetValue(0, Value::BIGINT(free_blocks));
		output.data[5].SetValue(0, Value(BytesToHumanReadableString(wal_size)));
	} else {
		output.data[0].SetValue(0, Value());
		output.data[1].SetValue(0, Value());
		output.data[2].SetValue(0, Value());
		output.data[3].SetValue(0, Value());
		output.data[4].SetValue(0, Value());
		output.data[5].SetValue(0, Value());
	}
	output.data[6].SetValue(0, Value(BytesToHumanReadableString(buffer_manager.GetUsedMemory())));
	auto max_memory = buffer_manager.GetMaxMemory();
	output.data[7].SetValue(0, max_memory == (idx_t)-1 ? Value("Unlimited")
	                                                   : Value(BytesToHumanReadableString(max_memory)));

	data.finished = true;
}

void PragmaDatabaseSize::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_database_size", {}, PragmaDatabaseSizeFunction, PragmaDatabaseSizeBind,
	                              PragmaDatabaseSizeInit));
}

} // namespace duckdb
