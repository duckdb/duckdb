#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

struct DuckDBTemporaryFilesData : public GlobalTableFunctionState {
	DuckDBTemporaryFilesData() : offset(0) {
	}

	vector<TemporaryFileInformation> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBTemporaryFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("size");
	return_types.emplace_back(LogicalType::BIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBTemporaryFilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBTemporaryFilesData>();

	result->entries = BufferManager::GetBufferManager(context).GetTemporaryFiles();
	return std::move(result);
}

void DuckDBTemporaryFilesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBTemporaryFilesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;

	// path, VARCHAR
	auto &path = output.data[0];
	// size, BIGINT
	auto &size = output.data[1];

	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		path.Append(Value(entry.path));
		size.Append(Value::BIGINT(NumericCast<int64_t>(entry.size)));
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBTemporaryFilesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_temporary_files", {}, DuckDBTemporaryFilesFunction, DuckDBTemporaryFilesBind,
	                              DuckDBTemporaryFilesInit));
}

} // namespace duckdb
