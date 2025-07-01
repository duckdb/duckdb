//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_multi_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_schema.hpp"

namespace duckdb {

class CSVFileReaderOptions : public BaseFileReaderOptions {
public:
	CSVFileReaderOptions() = default;
	explicit CSVFileReaderOptions(CSVReaderOptions options_p) : options(std::move(options_p)) {
	}

	CSVReaderOptions options;
};

struct CSVSchemaDiscovery {
	static CSVSchema SchemaDiscovery(ClientContext &context, shared_ptr<CSVBufferManager> &buffer_manager,
	                                 CSVReaderOptions &options, const MultiFileOptions &file_options,
	                                 vector<LogicalType> &return_types, vector<string> &names,
	                                 MultiFileList &multi_file_list);
};

struct CSVMultiFileInfo : public MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface> InitializeInterface(ClientContext &context, MultiFileReader &reader,
	                                                                MultiFileList &file_list);

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
};

} // namespace duckdb
