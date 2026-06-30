//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/feature_query.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class SelectNode;
class SelectStatement;

struct FeaturePITQueryParameters {
	string source_table;
	string timestamp_column;
	vector<string> entity_columns;
	interval_t window_interval;
	string spine_filter;
	unique_ptr<ParsedExpression> anchor_filter;
	bool order_result = false;
};

bool FeatureColumnListContains(const vector<string> &columns, const string &column_name);
unique_ptr<SelectStatement> BuildFeaturePITQuery(const SelectNode &select_node,
                                                 const FeaturePITQueryParameters &parameters);
string BuildFeaturePITQuerySQL(const SelectNode &select_node, const FeaturePITQueryParameters &parameters);

} // namespace duckdb
