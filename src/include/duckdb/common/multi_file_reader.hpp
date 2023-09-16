//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/multi_file_reader_options.hpp"
#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class TableFunction;
class TableFunctionSet;
class TableFilterSet;
class LogicalGet;
class Expression;
class ClientContext;
class DataChunk;

struct HivePartitioningIndex {
	HivePartitioningIndex(string value, idx_t index);

	string value;
	idx_t index;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static HivePartitioningIndex Deserialize(Deserializer &deserializer);
};

//! The bind data for the multi-file reader, obtained through MultiFileReader::BindReader
struct MultiFileReaderBindData {
	//! The index of the filename column (if any)
	idx_t filename_idx = DConstants::INVALID_INDEX;
	//! The set of hive partitioning indexes (if any)
	vector<HivePartitioningIndex> hive_partitioning_indexes;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static MultiFileReaderBindData Deserialize(Deserializer &deserializer);
};

struct MultiFileFilterEntry {
	idx_t index = DConstants::INVALID_INDEX;
	bool is_constant = false;
};

struct MultiFileConstantEntry {
	MultiFileConstantEntry(idx_t column_id, Value value_p) : column_id(column_id), value(std::move(value_p)) {
	}

	//! The column id to apply the constant value to
	idx_t column_id;
	//! The constant value
	Value value;
};

struct MultiFileReaderData {
	//! The column ids to read from the file
	vector<idx_t> column_ids;
	//! The mapping of column id -> result column id
	//! The result chunk will be filled as follows: chunk.data[column_mapping[i]] = ReadColumn(column_ids[i]);
	vector<idx_t> column_mapping;
	//! Whether or not there are no columns to read. This can happen when a file only consists of constants
	bool empty_columns = false;
	//! Filters can point to either (1) local columns in the file, or (2) constant values in the `constant_map`
	//! This map specifies where the to-be-filtered value can be found
	vector<MultiFileFilterEntry> filter_map;
	//! The set of table filters
	optional_ptr<TableFilterSet> filters;
	//! The constants that should be applied at the various positions
	vector<MultiFileConstantEntry> constant_map;
	//! Map of column_id -> cast, used when reading multiple files when files have diverging types
	//! for the same column
	unordered_map<column_t, LogicalType> cast_map;
};

struct MultiFileReader {
	//! Add the parameters for multi-file readers (e.g. union_by_name, filename) to a table function
	DUCKDB_API static void AddParameters(TableFunction &table_function);
	//! Performs any globbing for the multi-file reader and returns a list of files to be read
	DUCKDB_API static vector<string> GetFileList(ClientContext &context, const Value &input, const string &name,
	                                             FileGlobOptions options = FileGlobOptions::DISALLOW_EMPTY);
	//! Parse the named parameters of a multi-file reader
	DUCKDB_API static bool ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options,
	                                   ClientContext &context);
	//! Perform complex filter pushdown into the multi-file reader, potentially filtering out files that should be read
	//! If "true" the first file has been eliminated
	DUCKDB_API static bool ComplexFilterPushdown(ClientContext &context, vector<string> &files,
	                                             const MultiFileReaderOptions &options, LogicalGet &get,
	                                             vector<unique_ptr<Expression>> &filters);
	//! Bind the options of the multi-file reader, potentially emitting any extra columns that are required
	DUCKDB_API static MultiFileReaderBindData BindOptions(MultiFileReaderOptions &options, const vector<string> &files,
	                                                      vector<LogicalType> &return_types, vector<string> &names);
	//! Finalize the bind phase of the multi-file reader after we know (1) the required (output) columns, and (2) the
	//! pushed down table filters
	DUCKDB_API static void FinalizeBind(const MultiFileReaderOptions &file_options,
	                                    const MultiFileReaderBindData &options, const string &filename,
	                                    const vector<string> &local_names, const vector<LogicalType> &global_types,
	                                    const vector<string> &global_names, const vector<column_t> &global_column_ids,
	                                    MultiFileReaderData &reader_data, ClientContext &context);
	//! Create all required mappings from the global types/names to the file-local types/names
	DUCKDB_API static void CreateMapping(const string &file_name, const vector<LogicalType> &local_types,
	                                     const vector<string> &local_names, const vector<LogicalType> &global_types,
	                                     const vector<string> &global_names, const vector<column_t> &global_column_ids,
	                                     optional_ptr<TableFilterSet> filters, MultiFileReaderData &reader_data,
	                                     const string &initial_file);
	//! Finalize the reading of a chunk - applying any constants that are required
	DUCKDB_API static void FinalizeChunk(const MultiFileReaderBindData &bind_data,
	                                     const MultiFileReaderData &reader_data, DataChunk &chunk);
	//! Creates a table function set from a single reader function (including e.g. list parameters, etc)
	DUCKDB_API static TableFunctionSet CreateFunctionSet(TableFunction table_function);

	template <class READER_CLASS, class RESULT_CLASS, class OPTIONS_CLASS>
	static MultiFileReaderBindData BindUnionReader(ClientContext &context, vector<LogicalType> &return_types,
	                                               vector<string> &names, RESULT_CLASS &result,
	                                               OPTIONS_CLASS &options) {
		D_ASSERT(options.file_options.union_by_name);
		vector<string> union_col_names;
		vector<LogicalType> union_col_types;
		// obtain the set of union column names + types by unifying the types of all of the files
		// note that this requires opening readers for each file and reading the metadata of each file
		auto union_readers =
		    UnionByName::UnionCols<READER_CLASS>(context, result.files, union_col_types, union_col_names, options);

		std::move(union_readers.begin(), union_readers.end(), std::back_inserter(result.union_readers));
		// perform the binding on the obtained set of names + types
		auto bind_data =
		    MultiFileReader::BindOptions(options.file_options, result.files, union_col_types, union_col_names);
		names = union_col_names;
		return_types = union_col_types;
		result.Initialize(result.union_readers[0]);
		D_ASSERT(names.size() == return_types.size());
		return bind_data;
	}

	template <class READER_CLASS, class RESULT_CLASS, class OPTIONS_CLASS>
	static MultiFileReaderBindData BindReader(ClientContext &context, vector<LogicalType> &return_types,
	                                          vector<string> &names, RESULT_CLASS &result, OPTIONS_CLASS &options) {
		if (options.file_options.union_by_name) {
			return BindUnionReader<READER_CLASS>(context, return_types, names, result, options);
		} else {
			shared_ptr<READER_CLASS> reader;
			reader = make_shared<READER_CLASS>(context, result.files[0], options);
			return_types = reader->return_types;
			names = reader->names;
			result.Initialize(std::move(reader));
			return MultiFileReader::BindOptions(options.file_options, result.files, return_types, names);
		}
	}

	template <class READER_CLASS>
	static void InitializeReader(READER_CLASS &reader, const MultiFileReaderOptions &options,
	                             const MultiFileReaderBindData &bind_data, const vector<LogicalType> &global_types,
	                             const vector<string> &global_names, const vector<column_t> &global_column_ids,
	                             optional_ptr<TableFilterSet> table_filters, const string &initial_file,
	                             ClientContext &context) {
		FinalizeBind(options, bind_data, reader.GetFileName(), reader.GetNames(), global_types, global_names,
		             global_column_ids, reader.reader_data, context);
		CreateMapping(reader.GetFileName(), reader.GetTypes(), reader.GetNames(), global_types, global_names,
		              global_column_ids, table_filters, reader.reader_data, initial_file);
		reader.reader_data.filters = table_filters;
	}

	template <class BIND_DATA>
	static void PruneReaders(BIND_DATA &data) {
		unordered_set<string> file_set;
		for (auto &file : data.files) {
			file_set.insert(file);
		}

		if (data.initial_reader) {
			// check if the initial reader should still be read
			auto entry = file_set.find(data.initial_reader->GetFileName());
			if (entry == file_set.end()) {
				data.initial_reader.reset();
			}
		}
		for (idx_t r = 0; r < data.union_readers.size(); r++) {
			// check if the union reader should still be read or not
			auto entry = file_set.find(data.union_readers[r]->GetFileName());
			if (entry == file_set.end()) {
				data.union_readers.erase(data.union_readers.begin() + r);
				r--;
				continue;
			}
		}
	}

private:
	static void CreateNameMapping(const string &file_name, const vector<LogicalType> &local_types,
	                              const vector<string> &local_names, const vector<LogicalType> &global_types,
	                              const vector<string> &global_names, const vector<column_t> &global_column_ids,
	                              MultiFileReaderData &reader_data, const string &initial_file);
};

} // namespace duckdb
