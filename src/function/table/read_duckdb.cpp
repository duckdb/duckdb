#include "duckdb/function/table/read_duckdb.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"

namespace duckdb {

struct DuckDBMultiFileInfo : MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface> CreateInterface(ClientContext &context);

	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override;
	bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<string> &expected_names,
	                     vector<LogicalType> &expected_types) override;
	bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override;
	void FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options, const vector<string> &expected_names,
	                      const vector<LogicalType> &expected_types) override;
	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;
	void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                MultiFileBindData &bind_data) override;
	void FinalizeBindData(MultiFileBindData &multi_file_data) override;
	optional_idx MaxThreads(const MultiFileBindData &bind_data_p, const MultiFileGlobalState &global_state,
	                        FileExpandResult expand_result) override;
	unique_ptr<GlobalTableFunctionState> InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data,
	                                                           MultiFileGlobalState &global_state) override;
	unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &, GlobalTableFunctionState &) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        BaseUnionData &union_data, const MultiFileBindData &bind_data_p) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        const OpenFileInfo &file, idx_t file_idx,
	                                        const MultiFileBindData &bind_data) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                        BaseFileReaderOptions &options,
	                                        const MultiFileOptions &file_options) override;
	void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                   LocalTableFunctionState &local_state) override;
	unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) override;
	FileGlobInput GetGlobInput() override;
};

unique_ptr<MultiFileReaderInterface> DuckDBMultiFileInfo::CreateInterface(ClientContext &context) {
	return make_uniq<DuckDBMultiFileInfo>();
}

unique_ptr<BaseFileReaderOptions> DuckDBMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                         optional_ptr<TableFunctionInfo> info) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

bool DuckDBMultiFileInfo::ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
                                          BaseFileReaderOptions &options, vector<string> &expected_names,
                                          vector<LogicalType> &expected_types) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

bool DuckDBMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &val,
                                      MultiFileOptions &file_options, BaseFileReaderOptions &options) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

void DuckDBMultiFileInfo::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
                                           const vector<string> &expected_names,
                                           const vector<LogicalType> &expected_types) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

unique_ptr<TableFunctionData> DuckDBMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                      unique_ptr<BaseFileReaderOptions> options) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

void DuckDBMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                     MultiFileBindData &bind_data) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

void DuckDBMultiFileInfo::FinalizeBindData(MultiFileBindData &multi_file_data) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

optional_idx DuckDBMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data_p,
                                             const MultiFileGlobalState &global_state, FileExpandResult expand_result) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

unique_ptr<GlobalTableFunctionState> DuckDBMultiFileInfo::InitializeGlobalState(ClientContext &context,
                                                                                MultiFileBindData &bind_data,
                                                                                MultiFileGlobalState &global_state) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

unique_ptr<LocalTableFunctionState> DuckDBMultiFileInfo::InitializeLocalState(ExecutionContext &,
                                                                              GlobalTableFunctionState &) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                             BaseUnionData &union_data,
                                                             const MultiFileBindData &bind_data_p) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                             const OpenFileInfo &file, idx_t file_idx,
                                                             const MultiFileBindData &bind_data) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                             BaseFileReaderOptions &options,
                                                             const MultiFileOptions &file_options) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

void DuckDBMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                        LocalTableFunctionState &local_state) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

unique_ptr<NodeStatistics> DuckDBMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

FileGlobInput DuckDBMultiFileInfo::GetGlobInput() {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

TableFunction ReadDuckDBTableFunction::GetFunction() {
	MultiFileFunction<DuckDBMultiFileInfo> read_duckdb("read_duckdb");
	return static_cast<TableFunction>(read_duckdb);
}

} // namespace duckdb
