#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"

namespace duckdb {

//! Both functions answer with a list of temporary files - the one that is there, and the one that
//! was reclaimed - so they share everything but where the list comes from.
struct TemporaryFilesData : public GlobalTableFunctionState {
	vector<TemporaryFileInformation> entries;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> TemporaryFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("size");
	return_types.emplace_back(LogicalType::BIGINT);

	return nullptr;
}

static void TemporaryFilesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<TemporaryFilesData>();
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, data.entries.size() - data.offset);

	auto path = FlatVector::Writer<string_t>(output.data[0], count);
	auto size = FlatVector::Writer<int64_t>(output.data[1], count);
	for (idx_t i = 0; i < count; i++) {
		auto &entry = data.entries[data.offset++];
		path.WriteValue(string_t(entry.path));
		size.WriteValue(NumericCast<int64_t>(entry.size));
	}
}

//! Whether to describe the whole directory or only what this instance owns.
struct TemporaryFilesBindData : public FunctionData {
	bool external = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<TemporaryFilesBindData>();
		result->external = external;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		return external == other.Cast<TemporaryFilesBindData>().external;
	}
};

static unique_ptr<FunctionData> DuckDBTemporaryFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	TemporaryFilesBind(context, input, return_types, names);
	auto result = make_uniq<TemporaryFilesBindData>();
	for (auto &entry : input.named_parameters) {
		if (entry.first == "external") {
			result->external = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
		}
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckDBTemporaryFilesInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto result = make_uniq<TemporaryFilesData>();
	auto &bind_data = input.bind_data->Cast<TemporaryFilesBindData>();
	result->entries = BufferManager::GetBufferManager(context).GetTemporaryFiles(bind_data.external);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> CleanupTemporaryDirectoryInit(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto result = make_uniq<TemporaryFilesData>();
	auto &temp_directory = BufferManager::GetBufferManager(context).GetTemporaryDirectory();
	if (temp_directory.empty()) {
		return std::move(result);
	}
	// the database's file system, not the context's: this reclaims duckdb's own files, and it has to
	// judge them by exactly the rule the sweep on first spill judges them by
	auto &fs = FileSystem::GetFileSystem(*context.db);
	result->entries = ReapAbandonedTemporaryFiles(fs, temp_directory);
	return std::move(result);
}

//! Whether the initialization should reclaim what other processes abandoned. It cannot be a
//! setting: the question is only ever asked once per directory, at the moment it is first used.
struct InitializeTemporaryDirectoryData : public FunctionData {
	bool cleanup = true;
	//! Whether being beaten to the initialization is acceptable. Only when the choice matches -
	//! being second is forgiven, being overruled is not.
	bool silent = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<InitializeTemporaryDirectoryData>();
		result->cleanup = cleanup;
		result->silent = silent;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		auto &rhs = other.Cast<InitializeTemporaryDirectoryData>();
		return cleanup == rhs.cleanup && silent == rhs.silent;
	}
};

static unique_ptr<FunctionData> InitializeTemporaryDirectoryBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<Identifier> &names) {
	TemporaryFilesBind(context, input, return_types, names);
	auto result = make_uniq<InitializeTemporaryDirectoryData>();
	for (auto &entry : input.named_parameters) {
		if (entry.first == "cleanup") {
			result->cleanup = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
		} else if (entry.first == "silent") {
			result->silent = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
		}
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> InitializeTemporaryDirectoryInit(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	auto result = make_uniq<TemporaryFilesData>();
	auto &bind_data = input.bind_data->Cast<InitializeTemporaryDirectoryData>();
	result->entries =
	    BufferManager::GetBufferManager(context).InitializeTemporaryDirectory(bind_data.cleanup, bind_data.silent);
	return std::move(result);
}

void DuckDBTemporaryFilesFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction temporary_files("duckdb_temporary_files", {}, TemporaryFilesFunction, DuckDBTemporaryFilesBind,
	                              DuckDBTemporaryFilesInit);
	temporary_files.named_parameters["external"] = LogicalType::BOOLEAN;
	set.AddFunction(temporary_files);
	set.AddFunction(TableFunction("cleanup_temporary_directory", {}, TemporaryFilesFunction, TemporaryFilesBind,
	                              CleanupTemporaryDirectoryInit));

	TableFunction initialize("initialize_temporary_directory", {}, TemporaryFilesFunction,
	                         InitializeTemporaryDirectoryBind, InitializeTemporaryDirectoryInit);
	initialize.named_parameters["cleanup"] = LogicalType::BOOLEAN;
	initialize.named_parameters["silent"] = LogicalType::BOOLEAN;
	set.AddFunction(initialize);
}

} // namespace duckdb
