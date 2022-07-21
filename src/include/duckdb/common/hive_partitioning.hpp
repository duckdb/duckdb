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
	static std::map<string, string> Parse(string &filename);
	//! Prunes a list of filenames based on a set of filters, can be used by TableFunctions in the pushdown_complex_filter
	//! function to skip files with filename-based filters.
	static void PruneFilesList(vector<string> &files,  vector<unique_ptr<Expression>> &filters, bool hive_enabled, bool filename_enabled, bool preserve_first = false);
};

} // namespace duckdb
