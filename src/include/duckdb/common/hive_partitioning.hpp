//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/hive_partitioning.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "re2/re2.h"

namespace duckdb {

class HivePartitioning {
public:
	//! Parse a filename that follows the hive partitioning scheme
	DUCKDB_API static std::map<string, string> Parse(string &filename);
	DUCKDB_API static std::map<string, string> Parse(string &filename, duckdb_re2::RE2 &regex);
	//! Prunes a list of filenames based on a set of filters, can be used by TableFunctions in the
	//! pushdown_complex_filter function to skip files with filename-based filters. Also removes the filters that always
	//! evaluate to true.
	DUCKDB_API static void ApplyFiltersToFileList(vector<string> &files, vector<unique_ptr<Expression>> &filters,
	                                              unordered_map<string, column_t> &column_map, idx_t table_index,
	                                              bool hive_enabled, bool filename_enabled);

	//! Returns the compiled regex pattern to match hive partitions
	DUCKDB_API static const string REGEX_STRING;
};

} // namespace duckdb
