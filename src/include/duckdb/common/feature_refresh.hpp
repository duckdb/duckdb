//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/feature_refresh.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;

struct FeatureRefreshResult {
	idx_t rows_affected = 0;
};

FeatureRefreshResult RefreshFeature(ClientContext &context, const string &feature_name);

} // namespace duckdb
