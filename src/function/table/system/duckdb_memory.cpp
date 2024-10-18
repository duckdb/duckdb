#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

struct DuckDBMemoryData : public GlobalTableFunctionState {
	DuckDBMemoryData() : offset(0) {
	}

	vector<MemoryInformation> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBMemoryBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("tag");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("memory_usage_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("temporary_storage_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBMemoryInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBMemoryData>();

	result->entries = BufferManager::GetBufferManager(context).GetMemoryUsageInfo();
	return std::move(result);
}

void DuckDBMemoryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBMemoryData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		// return values:
		idx_t col = 0;
		// tag, VARCHAR
		output.SetValue(col++, count, EnumUtil::ToString(entry.tag));
		// memory_usage_bytes, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(entry.size)));
		// temporary_storage_bytes, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(entry.evicted_data)));
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBMemoryFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_memory", {}, DuckDBMemoryFunction, DuckDBMemoryBind, DuckDBMemoryInit));
}

} // namespace duckdb
