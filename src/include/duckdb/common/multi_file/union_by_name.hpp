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
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"

namespace duckdb {
struct MultiFileReader;
struct MultiFileReaderInterface;

class UnionByName {
public:
	//! Combines the columns of all schemas by name - the types of the columns do not depend on the order of the schemas
	static void CombineUnionTypes(ClientContext &context, const vector<vector<string>> &names,
	                              const vector<vector<LogicalType>> &types, vector<LogicalType> &union_col_types,
	                              vector<Identifier> &union_col_names);
	//! Adds the columns of a schema to the combined columns - uses only the built-in type combination rules
	static void CombineUnionTypes(const vector<string> &new_names, const vector<LogicalType> &new_types,
	                              vector<LogicalType> &union_col_types, vector<Identifier> &union_col_names,
	                              identifier_map_t<idx_t> &union_names_map);

	//! Union all files(readers) by their col names
	static vector<shared_ptr<BaseUnionData>>
	UnionCols(ClientContext &context, const vector<OpenFileInfo> &files, vector<LogicalType> &union_col_types,
	          vector<Identifier> &union_col_names, BaseFileReaderOptions &options, MultiFileOptions &file_options,
	          MultiFileReader &multi_file_reader, MultiFileReaderInterface &interface);
};

} // namespace duckdb
