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

string BuildServeFeatureSQL(ClientContext &context, const vector<string> &feature_list, const string &spine_table,
                            const string &entity_override, const string &as_of_override);

} // namespace duckdb