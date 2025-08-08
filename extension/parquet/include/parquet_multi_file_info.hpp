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

struct ParquetReadBindData : public TableFunctionData {
	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;
	idx_t explicit_cardinality = 0; // can be set to inject exterior cardinality knowledge (e.g. from a data lake)
	unique_ptr<ParquetFileReaderOptions> options;

	ParquetOptions &GetParquetOptions() {
		return options->options;
	}
	const ParquetOptions &GetParquetOptions() const {
		return options->options;
	}

	shared_ptr<BaseFileReaderOptions> GetFileReaderOptions() override {
		auto file_reader_options = make_shared_ptr<ParquetFileReaderOptions>(options->options);
		return file_reader_options;
	};

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ParquetReadBindData>();
		result->initial_file_cardinality = initial_file_cardinality;
		result->initial_file_row_groups = initial_file_row_groups;
		result->explicit_cardinality = explicit_cardinality;
		result->options = make_uniq<ParquetFileReaderOptions>(options->options);
		return std::move(result);
	}
};

struct ParquetMultiFileInfo : MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface> InitializeInterface(ClientContext &context, MultiFileReader &reader,
	                                                                MultiFileList &file_list);

	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override;
	bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<string> &expected_names,
	                     vector<LogicalType> &expected_types) override;
	bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override;
	void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                MultiFileBindData &bind_data) override;
	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;
	void FinalizeBindData(MultiFileBindData &multi_file_data) override;
	void GetBindInfo(const TableFunctionData &bind_data, BindInfo &info) override;
	optional_idx MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
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
	unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) override;
	virtual void ResetCardinality(MultiFileBindData &multi_file_data) override;
	void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result) override;
	unique_ptr<MultiFileReaderInterface> Copy() override;
};

class ParquetScanFunction {
public:
	static TableFunctionSet GetFunctionSet();
};

} // namespace duckdb
