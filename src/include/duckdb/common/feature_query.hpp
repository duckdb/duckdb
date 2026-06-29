//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/feature_query.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/interval.hpp"

namespace duckdb {

class SelectNode;

struct FeaturePITQueryParameters {
	string source_table;
	string timestamp_column;
	vector<string> entity_columns;
	interval_t window_interval;
	string spine_filter;
	bool order_result = false;
};

bool FeatureColumnListContains(const vector<string> &columns, const string &column_name);
string BuildFeaturePITQuerySQL(const SelectNode &select_node, const FeaturePITQueryParameters &parameters);

} // namespace duckdb
