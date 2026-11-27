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

//! MultiFileList whose files are produced by a table function from the catalog, which is run lazily - one chunk at
//! a time - as files are requested. The first column the function produces holds the files: a VARCHAR path, or a
//! file struct (a STRUCT or VARIANT) that holds the options to open the file with - see OpenFileInfo::FromValue.
//! An optional second column of type MAP(VARCHAR, ...) holds (further) options to open the file with.
//! The function is run outside of a pipeline, so it cannot have a local state
class TableFunctionMultiFileList : public LazyMultiFileList {
public:
	//! Look up the built-in table function in the system catalog, bind it over the given inputs and list the files it
	//! produces
	TableFunctionMultiFileList(ClientContext &context, const Identifier &function_name, vector<Value> inputs,
	                           named_argument_map_t named_parameters = named_argument_map_t());
	//! List the files of an already bound table function - no catalog lookup, the bind data is owned by the list
	TableFunctionMultiFileList(ClientContext &context, TableFunction function, unique_ptr<FunctionData> bind_data,
	                           vector<LogicalType> return_types);
	~TableFunctionMultiFileList() override;

	//! The bind data of the function that produces the files
	optional_ptr<FunctionData> GetBindData() const {
		return bind_data.get();
	}
	ClientContext &GetClientContext() const {
		return context;
	}

	//! Displays the files the function was called with (e.g. the globs), rather than the files they expand to
	vector<OpenFileInfo> GetDisplayFileList(optional_idx max_files = optional_idx()) const override;

protected:
	bool ExpandNextPath() const override;

private:
	//! Initialize the state of the function - happens on the first expansion, so creating the list does no work
	void InitializeFunction() const;
	//! Read the files from the chunk the function produced
	void ReadFiles(DataChunk &output) const;

private:
	//! The ClientContext the function is run in
	ClientContext &context;
	//! The table function that produces the files
	TableFunction function;
	unique_ptr<FunctionData> bind_data;
	//! The types the function produces - the first one holds the files
	vector<LogicalType> return_types;
	//! The files the function was called with, if its first input is a file or a list of files
	vector<OpenFileInfo> input_files;

	mutable bool initialized = false;
	mutable unique_ptr<GlobalTableFunctionState> global_state;
	mutable DataChunk chunk;
};

} // namespace duckdb
