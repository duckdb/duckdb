//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/feature_serve.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;
class SelectStatement;

struct FeatureServeEntityMapping {
	string feature_column;
	string spine_column;
};

struct ServeFeatureRequest {
	string feature_name;
	vector<FeatureServeEntityMapping> entity_mappings;
};

unique_ptr<SelectStatement> BuildServeFeatureSelect(ClientContext &context, const vector<ServeFeatureRequest> &features,
                                                    const string &spine_table, const string &spine_entity_override,
                                                    const string &spine_asof_column);

} // namespace duckdb
