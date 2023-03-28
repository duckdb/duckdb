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

namespace duckdb {
class TableFunction;
class ClientContext;
class Value;

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

	// 		return MultiFileReader::BindReader<ParquetReader>(context, files, return_types, names, *result, parquet_options);
	template<class READER_CLASS, class RESULT_CLASS, class OPTIONS_CLASS>
	static void BindReader(ClientContext &context, vector<string> files, vector<LogicalType> &return_types, vector<string> &names, RESULT_CLASS &result, OPTIONS_CLASS &options) {
		result.files = std::move(files);
		if (!options.file_options.union_by_name) {
			shared_ptr<READER_CLASS> reader;
			if (return_types.empty()) {
				reader = make_shared<READER_CLASS>(context, result.files[0], options);
				return_types = reader->return_types;
				names = reader->names;
			} else {
				reader = make_shared<READER_CLASS>(context, result.files[0], return_types, options);
			}
			result.SetInitialReader(std::move(reader));
			return;
		}
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
		result.SetInitialReader(result.union_readers[0]);
		D_ASSERT(names.size() == return_types.size());
		result.types = union_col_types;
	}
};

} // namespace duckdb
