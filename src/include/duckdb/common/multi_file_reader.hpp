//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/multi_file_reader_options.hpp"
#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class TableFunction;
class ClientContext;
class Value;

//! The bind data for the multi-file reader, obtained through MultiFileReader::BindOptions
struct MultiFileReaderBindData {
	//! The index of the filename column (if any)
	idx_t filename_idx = DConstants::INVALID_INDEX;
	//! The set of hive partitioning indexes (if any)
	vector<pair<string, idx_t>> hive_partitioning_indexes;
};

struct MultiFileReaderData {
	//! when reading multiple parquet files (with union by name option)
	//! TableFunction might return more cols than any single parquet file. Even all parquet files have same
	//! cols, those files might have cols at different positions and with different logical type.
	//! e.g. p1.parquet (a INT , b VARCHAR) p2.parquet (c VARCHAR, a VARCHAR)
	vector<idx_t> union_idx_map;
	//! If the parquet file dont have union_cols5  union_null_cols[5] will be true.
	//! some parquet files may not have all union cols.
	vector<bool> union_null_cols;
	//! All union cols will cast to same type.
	vector<LogicalType> union_col_types;
	//! The column ids to read from the file
	vector<idx_t> column_ids;
	//! The mapping of column id -> result column id
	//! The result chunk will be filled as follows: chunk.data[column_mapping[i]] = ReadColumn(column_ids[i]);
	vector<idx_t> column_mapping;
	//! Reverse column mapping holds the mapping of global id -> i
	//! So essentially reverse_column_mapping[column_mapping[i]] == i
	vector<idx_t> reverse_column_mapping;
	//! The set of table filters
	optional_ptr<TableFilterSet> filters;
	//! The constants that should be applied at the various positions
	vector<pair<idx_t, Value>> constant_map;
	//! Map of column_id -> cast, used when reading multiple files when files have diverging types
	//! for the same column
	unordered_map<column_t, LogicalType> cast_map;
};

struct MultiFileReader {
	//! Add the parameters for multi-file readers (e.g. union_by_name, filename) to a table function
	DUCKDB_API static void AddParameters(TableFunction &table_function);
	//! Performs any globbing for the multi-file reader and returns a list of files to be read
	DUCKDB_API static vector<string> GetFileList(ClientContext &context, const Value &input, const string &name, FileGlobOptions options = FileGlobOptions::DISALLOW_EMPTY);
	//! Parse the named parameters of a multi-file reader for a COPY statement
	DUCKDB_API static bool ParseCopyOption(const string &key, const vector<Value> &values, MultiFileReaderOptions &options);
	//! Parse the named parameters of a multi-file reader
	DUCKDB_API static bool ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options);
	//! Perform complex filter pushdown into the multi-file reader, potentially filtering out files that should be read
	//! If "true" the first file has been eliminated
	DUCKDB_API static bool ComplexFilterPushdown(ClientContext &context, vector<string> &files, const MultiFileReaderOptions &options, LogicalGet &get, vector<unique_ptr<Expression>> &filters);
	//! Bind the options of the multi-file reader, potentially emitting any extra columns that are required
	DUCKDB_API static MultiFileReaderBindData BindOptions(MultiFileReaderOptions &options, const vector<string> &files, vector<LogicalType> &return_types, vector<string> &names);
	//! Finalize the bind phase of the multi-file reader after we know (1) the required (output) columns, and (2) the pushed down table filters
	DUCKDB_API static void FinalizeBind(const MultiFileReaderBindData &options, const string &filename, const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data);
	//! Create all required mappings from the global types/names to the file-local types/names
	DUCKDB_API static void CreateMapping(const string &file_name, const vector<LogicalType> &file_types, const vector<string> &file_names, const vector<LogicalType> &global_types, const vector<string> &global_names, const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data);
	//! Finalize the reading of a chunk - applying any constants that are required
	DUCKDB_API static void FinalizeChunk(const MultiFileReaderBindData &bind_data, const MultiFileReaderData &reader_data, DataChunk &chunk);

	template<class READER_CLASS, class RESULT_CLASS, class OPTIONS_CLASS>
	static void BindUnionReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names, RESULT_CLASS &result, OPTIONS_CLASS &options) {
		D_ASSERT(options.file_options.union_by_name);
		case_insensitive_map_t<idx_t> union_names_map;
		vector<string> union_col_names;
		vector<LogicalType> union_col_types;
		auto dummy_readers = UnionByName::UnionCols<READER_CLASS>(
		    context, result.files, union_col_types, union_col_names, union_names_map, options);

		dummy_readers = UnionByName::CreateUnionMap<READER_CLASS>(
		    std::move(dummy_readers), union_col_types, union_col_names, union_names_map);

		std::move(dummy_readers.begin(), dummy_readers.end(), std::back_inserter(result.union_readers));
		names = union_col_names;
		return_types = union_col_types;
		result.Initialize(result.union_readers[0]);
		D_ASSERT(names.size() == return_types.size());
	}

	template<class READER_CLASS, class RESULT_CLASS, class OPTIONS_CLASS>
	static void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names, RESULT_CLASS &result, OPTIONS_CLASS &options) {
		if (options.file_options.union_by_name) {
			BindUnionReader<READER_CLASS>(context, return_types, names, result, options);
			return;
		}
		shared_ptr<READER_CLASS> reader;
		reader = make_shared<READER_CLASS>(context, result.files[0], options);
		return_types = reader->return_types;
		names = reader->names;
		result.Initialize(std::move(reader));
	}

	template<class READER_CLASS>
	static void InitializeReader(READER_CLASS &reader, const MultiFileReaderBindData &bind_data, const vector<LogicalType> &global_types, const vector<string> &global_names, const vector<column_t> &global_column_ids, optional_ptr<TableFilterSet> table_filters) {
		FinalizeBind(bind_data, reader.file_name, global_column_ids, reader.reader_data);
		CreateMapping(reader.file_name, reader.return_types, reader.names, global_types, global_names, global_column_ids, reader.reader_data);
		reader.reader_data.filters = table_filters;
	}


};

} // namespace duckdb
