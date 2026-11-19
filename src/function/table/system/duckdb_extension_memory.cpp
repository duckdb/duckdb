#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/extension/capi_allocator.hpp"
#include "duckdb/main/extension_manager.hpp"

namespace duckdb {

struct DuckDBExtensionMemoryEntry {
	string extension_name;
	idx_t memory_usage;
};

struct DuckDBExtensionMemoryData : public GlobalTableFunctionState {
	vector<DuckDBExtensionMemoryEntry> entries;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBExtensionMemoryBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types,
                                                          vector<Identifier> &names) {
	names.emplace_back("extension_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("memory_usage_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBExtensionMemoryInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBExtensionMemoryData>();
	auto &manager = ExtensionManager::Get(context);
	for (auto &name : manager.GetExtensions()) {
		auto info = manager.GetExtensionInfo(name);
		if (info && info->is_loaded) {
			auto usage = ExtensionAllocator::GetUsage(info->orig_ext_name);
			if (usage.IsValid()) {
				result->entries.push_back({name, usage.GetIndex()});
			}
		}
	}
	return std::move(result);
}

static void DuckDBExtensionMemoryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBExtensionMemoryData>();
	auto count = MinValue<idx_t>(data.entries.size() - data.offset, STANDARD_VECTOR_SIZE);
	auto name_writer = FlatVector::Writer<string_t>(output.data[0], count);
	auto usage_writer = FlatVector::Writer<int64_t>(output.data[1], count);
	for (idx_t i = 0; i < count; i++) {
		auto &entry = data.entries[data.offset++];
		name_writer.WriteValue(string_t(entry.extension_name));
		usage_writer.WriteValue(NumericCast<int64_t>(entry.memory_usage));
	}
}

void DuckDBExtensionMemoryFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_extension_memory", {}, DuckDBExtensionMemoryFunction,
	                              DuckDBExtensionMemoryBind, DuckDBExtensionMemoryInit));
}

} // namespace duckdb
