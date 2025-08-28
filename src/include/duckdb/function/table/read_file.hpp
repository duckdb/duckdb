//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/read_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct ReadFileBindData : public TableFunctionData {
	unique_ptr<BaseFileReaderOptions> options;

	static constexpr const idx_t FILE_NAME_COLUMN = 0;
	static constexpr const idx_t FILE_CONTENT_COLUMN = 1;
	static constexpr const idx_t FILE_SIZE_COLUMN = 2;
	static constexpr const idx_t FILE_LAST_MODIFIED_COLUMN = 3;
};

struct ReadFileGlobalState : public GlobalTableFunctionState {
	ReadFileGlobalState() {
	}

	shared_ptr<MultiFileList> file_list;
	vector<idx_t> column_ids;
	bool requires_file_open = false;
};

struct ReadBlobOperation {
	static constexpr const char *NAME = "read_blob";
	static constexpr const char *FILE_TYPE = "blob";

	static inline LogicalType TYPE() {
		return LogicalType::BLOB;
	}
};

struct ReadTextOperation {
	static constexpr const char *NAME = "read_text";
	static constexpr const char *FILE_TYPE = "text";

	static inline LogicalType TYPE() {
		return LogicalType::VARCHAR;
	}
};

template <class OP>
struct DirectMultiFileInfo : MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface> CreateInterface(ClientContext &context);
	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override;
	bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<string> &expected_names,
	                     vector<LogicalType> &expected_types) override;
	bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override;
	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;
	void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                MultiFileBindData &bind_data) override;
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
	unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) override;
	FileGlobInput GetGlobInput() override;
};

} // namespace duckdb
