//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/table_function_multi_file_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
class ThreadContext;

//! Appends the next files of the list to "result", together with the options to open them - returns false when
//! there are no more files. A function that implements this hands over its files directly, rather than as rows
//! that are converted back into files
typedef bool (*table_function_scan_files_t)(ClientContext &context, TableFunctionInput &input,
                                            vector<OpenFileInfo> &result);
//! The entries shown when the list is displayed (e.g. the globs it was created from) - at most max_files of them
typedef vector<OpenFileInfo> (*table_function_display_files_t)(const FunctionData &bind_data, optional_idx max_files);

//! How a table function provides the files of a TableFunctionMultiFileList - all optional
struct TableFunctionMultiFileListSettings {
	table_function_scan_files_t scan_files = nullptr;
	table_function_display_files_t display_files = nullptr;
};

//! MultiFileList whose files are produced by a table function, which is run lazily - one chunk at a time - as files
//! are requested. The first column the function produces holds the files: a VARCHAR path, or a file struct (a
//! STRUCT or VARIANT) that also holds the options to open the file with - see OpenFileInfo::FromValue
class TableFunctionMultiFileList : public LazyMultiFileList {
public:
	//! Bind the function over the given inputs and list the files it produces
	TableFunctionMultiFileList(ClientContext &context, TableFunction function, vector<Value> inputs,
	                           named_argument_map_t named_parameters = named_argument_map_t(),
	                           TableFunctionMultiFileListSettings settings = TableFunctionMultiFileListSettings());
	//! List the files the function produces for a bind that has already happened
	TableFunctionMultiFileList(ClientContext &context, TableFunction function, unique_ptr<FunctionData> bind_data,
	                           vector<LogicalType> return_types,
	                           TableFunctionMultiFileListSettings settings = TableFunctionMultiFileListSettings());
	~TableFunctionMultiFileList() override;

	vector<OpenFileInfo> GetDisplayFileList(optional_idx max_files = optional_idx()) const override;

protected:
	bool ExpandNextPath() const override;

private:
	//! Initialize the state of the function - happens on the first expansion, so creating the list does no work
	void InitializeFunction() const;
	//! Read the files from the first column of the chunk the function produced
	void ReadFiles(DataChunk &output) const;

private:
	//! The ClientContext the function is run in
	ClientContext &context;
	//! The table function that produces the files
	TableFunction function;
	TableFunctionMultiFileListSettings settings;
	unique_ptr<FunctionData> bind_data;
	//! The types the function produces - the first one holds the files
	vector<LogicalType> return_types;

	mutable bool initialized = false;
	mutable unique_ptr<ThreadContext> thread_context;
	mutable unique_ptr<GlobalTableFunctionState> global_state;
	mutable unique_ptr<LocalTableFunctionState> local_state;
	mutable DataChunk chunk;
};

} // namespace duckdb
