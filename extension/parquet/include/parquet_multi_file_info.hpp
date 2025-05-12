//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_multi_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

class ParquetFileReaderOptions : public BaseFileReaderOptions {
public:
	explicit ParquetFileReaderOptions(ParquetOptions options_p) : options(std::move(options_p)) {
	}
	explicit ParquetFileReaderOptions(ClientContext &context) : options(context) {
	}

	ParquetOptions options;
};

struct ParquetMultiFileInfo {
	static unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                           optional_ptr<TableFunctionInfo> info);
	static bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                            BaseFileReaderOptions &options, vector<string> &expected_names,
	                            vector<LogicalType> &expected_types);
	static bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                        BaseFileReaderOptions &options);
	static void FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options_p,
	                             const vector<string> &expected_names, const vector<LogicalType> &expected_types);
	static void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                       MultiFileBindData &bind_data);
	static unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                        unique_ptr<BaseFileReaderOptions> options);
	static void FinalizeBindData(MultiFileBindData &multi_file_data);
	static void GetBindInfo(const TableFunctionData &bind_data, BindInfo &info);
	static optional_idx MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
	                               FileExpandResult expand_result);
	static unique_ptr<GlobalTableFunctionState>
	InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data, MultiFileGlobalState &global_state);
	static unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &, GlobalTableFunctionState &);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               BaseUnionData &union_data, const MultiFileBindData &bind_data_p);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               const OpenFileInfo &file, idx_t file_idx,
	                                               const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                               ParquetOptions &options, const MultiFileOptions &file_options);
	static shared_ptr<BaseUnionData> GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx);
	static void FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &);
	static void Scan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk);
	static bool TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader,
	                              GlobalTableFunctionState &gstate, LocalTableFunctionState &lstate);
	static void FinishFile(ClientContext &context, GlobalTableFunctionState &global_state, BaseFileReader &reader);
	static void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                          LocalTableFunctionState &local_state);
	static unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count);
	static unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, BaseFileReader &reader, const string &name);
	static double GetProgressInFile(ClientContext &context, const BaseFileReader &reader);
	static void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result);
};

class ParquetScanFunction {
public:
	static TableFunctionSet GetFunctionSet();
};

} // namespace duckdb
