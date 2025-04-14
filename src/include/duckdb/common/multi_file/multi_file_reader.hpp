//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/multi_file/union_by_name.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"

namespace duckdb {
class TableFunction;
class TableFunctionSet;
class TableFilterSet;
class LogicalGet;
class Expression;
class ClientContext;
class DataChunk;

enum class ReaderInitializeType { INITIALIZED, SKIP_READING_FILE };

//! The MultiFileReader class provides a set of helper methods to handle scanning from multiple files
struct MultiFileReader {
public:
	static constexpr column_t COLUMN_IDENTIFIER_FILENAME = UINT64_C(9223372036854775808);
	static constexpr column_t COLUMN_IDENTIFIER_FILE_ROW_NUMBER = UINT64_C(9223372036854775809);

public:
	virtual ~MultiFileReader();

	//! Create a MultiFileReader for a specific TableFunction, using its function name for errors
	DUCKDB_API static unique_ptr<MultiFileReader> Create(const TableFunction &table_function);
	//! Create a default MultiFileReader, function_name is used for errors
	DUCKDB_API static unique_ptr<MultiFileReader> CreateDefault(const string &function_name = "");

	//! Create a LIST Value from a vector of strings (list of file paths)
	static Value CreateValueFromFileList(const vector<string> &files);

	//! Add the parameters for multi-file readers (e.g. union_by_name, filename) to a table function
	DUCKDB_API static void AddParameters(TableFunction &table_function);
	//! Creates a table function set from a single reader function (including e.g. list parameters, etc)
	DUCKDB_API static TableFunctionSet CreateFunctionSet(TableFunction table_function);

	//! Parse a Value containing 1 or more paths into a vector of paths. Note: no expansion is performed here
	DUCKDB_API virtual vector<string> ParsePaths(const Value &input);
	//! Create a MultiFileList from a vector of paths. Any globs will be expanded using the default filesystem
	DUCKDB_API virtual shared_ptr<MultiFileList>
	CreateFileList(ClientContext &context, const vector<string> &paths,
	               FileGlobOptions options = FileGlobOptions::DISALLOW_EMPTY);
	//! Shorthand for ParsePaths + CreateFileList
	DUCKDB_API shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const Value &input,
	                                                    FileGlobOptions options = FileGlobOptions::DISALLOW_EMPTY);

	//! Parse the named parameters of a multi-file reader
	DUCKDB_API virtual bool ParseOption(const string &key, const Value &val, MultiFileOptions &options,
	                                    ClientContext &context);
	//! Perform filter pushdown into the MultiFileList. Returns a new MultiFileList if filters were pushed down
	DUCKDB_API virtual unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
	                                                                   const MultiFileOptions &options,
	                                                                   MultiFilePushdownInfo &info,
	                                                                   vector<unique_ptr<Expression>> &filters);
	DUCKDB_API virtual unique_ptr<MultiFileList>
	DynamicFilterPushdown(ClientContext &context, const MultiFileList &files, const MultiFileOptions &options,
	                      const vector<string> &names, const vector<LogicalType> &types,
	                      const vector<column_t> &column_ids, TableFilterSet &filters);
	//! Try to use the MultiFileReader for binding. Returns true if a bind could be made, returns false if the
	//! MultiFileReader can not perform the bind and binding should be performed on 1 or more files in the MultiFileList
	//! directly.
	DUCKDB_API virtual bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                             vector<string> &names, MultiFileReaderBindData &bind_data);
	//! Bind the options of the multi-file reader, potentially emitting any extra columns that are required
	DUCKDB_API virtual void BindOptions(MultiFileOptions &options, MultiFileList &files,
	                                    vector<LogicalType> &return_types, vector<string> &names,
	                                    MultiFileReaderBindData &bind_data);

	//! Initialize global state used by the MultiFileReader
	DUCKDB_API virtual unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
	                      const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
	                      const vector<MultiFileColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids);

	//! Finalize the bind phase of the multi-file reader after we know (1) the required (output) columns, and (2) the
	//! pushed down table filters
	DUCKDB_API virtual void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                                     const MultiFileReaderBindData &options,
	                                     const vector<MultiFileColumnDefinition> &global_columns,
	                                     const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                                     optional_ptr<MultiFileReaderGlobalState> global_state);

	//! Create all required mappings from the global types/names to the file-local types/names
	DUCKDB_API virtual ReaderInitializeType
	CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
	              const vector<MultiFileColumnDefinition> &global_columns, const vector<ColumnIndex> &global_column_ids,
	              optional_ptr<TableFilterSet> filters, const OpenFileInfo &initial_file,
	              const MultiFileReaderBindData &bind_data, const virtual_column_map_t &virtual_columns);

	//! Finalize the reading of a chunk - applying any constants that are required
	DUCKDB_API virtual void FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data,
	                                      BaseFileReader &reader, const MultiFileReaderData &reader_data,
	                                      DataChunk &input_chunk, DataChunk &output_chunk, ExpressionExecutor &executor,
	                                      optional_ptr<MultiFileReaderGlobalState> global_state);

	//! Fetch the partition data for the current chunk
	DUCKDB_API virtual void GetPartitionData(ClientContext &context, const MultiFileReaderBindData &bind_data,
	                                         const MultiFileReaderData &reader_data,
	                                         optional_ptr<MultiFileReaderGlobalState> global_state,
	                                         const OperatorPartitionInfo &partition_info,
	                                         OperatorPartitionData &partition_data);

	DUCKDB_API static void GetVirtualColumns(ClientContext &context, MultiFileReaderBindData &bind_data,
	                                         virtual_column_map_t &result);

	template <class OP, class OPTIONS_TYPE>
	MultiFileReaderBindData BindUnionReader(ClientContext &context, vector<LogicalType> &return_types,
	                                        vector<string> &names, MultiFileList &files, MultiFileBindData &result,
	                                        OPTIONS_TYPE &options, MultiFileOptions &file_options) {
		D_ASSERT(file_options.union_by_name);
		vector<string> union_col_names;
		vector<LogicalType> union_col_types;

		// obtain the set of union column names + types by unifying the types of all of the files
		// note that this requires opening readers for each file and reading the metadata of each file
		// note also that it requires fully expanding the MultiFileList
		auto materialized_file_list = files.GetAllFiles();
		auto union_readers = UnionByName::UnionCols<OP>(context, materialized_file_list, union_col_types,
		                                                union_col_names, options, file_options);

		std::move(union_readers.begin(), union_readers.end(), std::back_inserter(result.union_readers));
		// perform the binding on the obtained set of names + types
		MultiFileReaderBindData bind_data;
		BindOptions(file_options, files, union_col_types, union_col_names, bind_data);
		names = union_col_names;
		return_types = union_col_types;
		result.Initialize(context, *result.union_readers[0]);
		D_ASSERT(names.size() == return_types.size());
		return bind_data;
	}

	template <class OP, class OPTIONS_TYPE>
	MultiFileReaderBindData BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                                   MultiFileList &files, MultiFileBindData &result, OPTIONS_TYPE &options,
	                                   MultiFileOptions &file_options) {
		if (file_options.union_by_name) {
			return BindUnionReader<OP>(context, return_types, names, files, result, options, file_options);
		} else {
			shared_ptr<BaseFileReader> reader;
			reader = OP::CreateReader(context, files.GetFirstFile(), options, file_options);
			auto &columns = reader->GetColumns();
			for (auto &column : columns) {
				return_types.emplace_back(column.type);
				names.emplace_back(column.name);
			}
			result.Initialize(std::move(reader));
			MultiFileReaderBindData bind_data;
			BindOptions(file_options, files, return_types, names, bind_data);
			return bind_data;
		}
	}

	virtual ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                              const vector<MultiFileColumnDefinition> &global_columns,
	                                              const vector<ColumnIndex> &global_column_ids,
	                                              optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                              optional_ptr<MultiFileReaderGlobalState> global_state) {
		FinalizeBind(reader_data, bind_data.file_options, bind_data.reader_bind, global_columns, global_column_ids,
		             context, global_state);
		return CreateMapping(context, reader_data, global_columns, global_column_ids, table_filters,
		                     bind_data.file_list->GetFirstFile(), bind_data.reader_bind, bind_data.virtual_columns);
	}

	template <class BIND_DATA>
	static void PruneReaders(BIND_DATA &data, MultiFileList &file_list) {
		unordered_set<string> file_set;

		// Avoid materializing the file list if there's nothing to prune
		if (!data.initial_reader && data.union_readers.empty()) {
			return;
		}

		for (const auto &file : file_list.Files()) {
			file_set.insert(file.path);
		}

		if (data.initial_reader) {
			// check if the initial reader should still be read
			auto entry = file_set.find(data.initial_reader->GetFileName());
			if (entry == file_set.end()) {
				data.initial_reader.reset();
			}
		}
		for (idx_t r = 0; r < data.union_readers.size(); r++) {
			if (!data.union_readers[r]) {
				data.union_readers.erase_at(r);
				r--;
				continue;
			}
			// check if the union reader should still be read or not
			auto entry = file_set.find(data.union_readers[r]->GetFileName());
			if (entry == file_set.end()) {
				data.union_readers.erase_at(r);
				r--;
				continue;
			}
		}
	}

	//! Get partition info
	DUCKDB_API virtual TablePartitionInfo GetPartitionInfo(ClientContext &context,
	                                                       const MultiFileReaderBindData &bind_data,
	                                                       TableFunctionPartitionInput &input);

protected:
	//! Used in errors to report which function is using this MultiFileReader
	string function_name;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
