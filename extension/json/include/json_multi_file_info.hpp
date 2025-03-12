//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_multi_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file_reader_function.hpp"
#include "json_reader_options.hpp"

namespace duckdb {

class JSONFileReaderOptions : public BaseFileReaderOptions {
public:
	JSONReaderOptions options;
};

struct JSONMultiFileInfo {
	static unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                           optional_ptr<TableFunctionInfo> info);
	static bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                            BaseFileReaderOptions &options, vector<string> &expected_names,
	                            vector<LogicalType> &expected_types);
	static bool ParseOption(ClientContext &context, const string &key, const Value &val,
	                        MultiFileReaderOptions &file_options, BaseFileReaderOptions &options);
	static void FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
	                             const vector<string> &expected_names, const vector<LogicalType> &expected_types);
	static unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                        unique_ptr<BaseFileReaderOptions> options);
	static void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                       MultiFileBindData &bind_data);
	static void FinalizeBindData(MultiFileBindData &multi_file_data);
	static void GetBindInfo(const TableFunctionData &bind_data, BindInfo &info);
	static optional_idx MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
	                               FileExpandResult expand_result);
	static unique_ptr<GlobalTableFunctionState>
	InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data, MultiFileGlobalState &global_state);
	static unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &context,
	                                                                GlobalTableFunctionState &global_state);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               BaseUnionData &union_data, const MultiFileBindData &bind_data_p);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               const string &filename, idx_t file_idx,
	                                               const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const string &filename,
	                                               JSONReaderOptions &options,
	                                               const MultiFileReaderOptions &file_options);
	static shared_ptr<BaseUnionData> GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx);
	static void FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &gstate_p);
	static bool TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader,
	                              GlobalTableFunctionState &gstate, LocalTableFunctionState &lstate);
	static void Scan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk);
	static void FinishFile(ClientContext &context, GlobalTableFunctionState &global_state, BaseFileReader &reader);
	static void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                          LocalTableFunctionState &local_state);
	static unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count);
	static unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, BaseFileReader &reader, const string &name);
	static double GetProgressInFile(ClientContext &context, const BaseFileReader &reader);
	static void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result);
};

} // namespace duckdb
