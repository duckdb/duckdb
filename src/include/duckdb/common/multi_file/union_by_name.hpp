//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/union_by_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"

namespace duckdb {
struct MultiFileReader;
struct MultiFileReaderInterface;

class UnionByName {
public:
	static void CombineUnionTypes(const vector<string> &new_names, const vector<LogicalType> &new_types,
	                              vector<LogicalType> &union_col_types, vector<string> &union_col_names,
	                              case_insensitive_map_t<idx_t> &union_names_map);

	//! Union all files(readers) by their col names
	static vector<shared_ptr<BaseUnionData>>
	UnionCols(ClientContext &context, const vector<OpenFileInfo> &files, vector<LogicalType> &union_col_types,
	          vector<string> &union_col_names, BaseFileReaderOptions &options, MultiFileOptions &file_options,
	          MultiFileReader &multi_file_reader, MultiFileReaderInterface &interface);
};

} // namespace duckdb
