//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"
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
	static constexpr column_t COLUMN_IDENTIFIER_FILE_INDEX = UINT64_C(9223372036854775810);
	// Reserved field id used for the "_file" field according to the iceberg spec (used for file_row_number)
	static constexpr int32_t ORDINAL_FIELD_ID = 2147483645;
	// Reserved field id used for the "_pos" field according to the iceberg spec (used for file_row_number)
	static constexpr int32_t FILENAME_FIELD_ID = 2147483646;
	// Reserved field id used for the "file_path" field for iceberg positional deletes
	static constexpr int32_t DELETE_FILE_PATH_FIELD_ID = 2147483546;
	// Reserved field id used for the "pos" field for iceberg positional deletes
	static constexpr int32_t DELETE_POS_FIELD_ID = 2147483545;
	// Reserved field id used for the "_row_id" field according to the iceberg spec
	static constexpr int32_t ROW_ID_FIELD_ID = 2147483540;
	// Reserved field id used for the "_last_updated_sequence_number" field according to the iceberg spec
	static constexpr int32_t LAST_UPDATED_SEQUENCE_NUMBER_ID = 2147483539;

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
	               const FileGlobInput &glob_input = FileGlobOptions::DISALLOW_EMPTY);
	//! Shorthand for ParsePaths + CreateFileList
	DUCKDB_API shared_ptr<MultiFileList>
	CreateFileList(ClientContext &context, const Value &input,
	               const FileGlobInput &glob_input = FileGlobOptions::DISALLOW_EMPTY);

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
	              optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
	              const MultiFileReaderBindData &bind_data, const virtual_column_map_t &virtual_columns,
	              MultiFileColumnMappingMode mapping_mode);

	DUCKDB_API virtual ReaderInitializeType
	CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
	              const vector<MultiFileColumnDefinition> &global_columns, const vector<ColumnIndex> &global_column_ids,
	              optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
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

	MultiFileReaderBindData BindUnionReader(ClientContext &context, vector<LogicalType> &return_types,
	                                        vector<string> &names, MultiFileList &files, MultiFileBindData &result,
	                                        BaseFileReaderOptions &options, MultiFileOptions &file_options);

	MultiFileReaderBindData BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                                   MultiFileList &files, MultiFileBindData &result, BaseFileReaderOptions &options,
	                                   MultiFileOptions &file_options);

	DUCKDB_API virtual ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data,
	                                                         const MultiFileBindData &bind_data,
	                                                         const vector<MultiFileColumnDefinition> &global_columns,
	                                                         const vector<ColumnIndex> &global_column_ids,
	                                                         optional_ptr<TableFilterSet> table_filters,
	                                                         ClientContext &context, MultiFileGlobalState &gstate);

	static void PruneReaders(MultiFileBindData &data, MultiFileList &file_list);

	DUCKDB_API virtual shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                                           BaseUnionData &union_data,
	                                                           const MultiFileBindData &bind_data);
	DUCKDB_API virtual shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                                           const OpenFileInfo &file, idx_t file_idx,
	                                                           const MultiFileBindData &bind_data);
	DUCKDB_API virtual shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                                           BaseFileReaderOptions &options,
	                                                           const MultiFileOptions &file_options,
	                                                           MultiFileReaderInterface &interface);

	//! Get partition info
	DUCKDB_API virtual TablePartitionInfo GetPartitionInfo(ClientContext &context,
	                                                       const MultiFileReaderBindData &bind_data,
	                                                       TableFunctionPartitionInput &input);

	//! Get a constant expression for the given virtual column, if this can be handled by the MultiFileReader
	//! This expression is constant for the entire file
	DUCKDB_API virtual unique_ptr<Expression> GetConstantVirtualColumn(MultiFileReaderData &reader_data,
	                                                                   idx_t column_id, const LogicalType &type);

	//! Gets an expression to evaluate the given virtual column
	DUCKDB_API virtual unique_ptr<Expression>
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, idx_t &column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_index,
	                           optional_ptr<MultiFileColumnDefinition> &global_column_reference);

	DUCKDB_API virtual unique_ptr<MultiFileReader> Copy() const;

	DUCKDB_API virtual FileGlobInput GetGlobInput(MultiFileReaderInterface &interface);

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
