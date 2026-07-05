//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/feature_refresh.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

class FeatureCatalogEntry;
class SelectStatement;

//! Builds the SELECT statement whose result is the full contents of the next feature version: one
//! snapshot row per entity in the entity table, aggregating the events in [feature_ts - window,
//! feature_ts). Entities with no events in the window keep NULL feature values, except SUM/COUNT which
//! default to 0.
unique_ptr<SelectStatement> BuildFeatureRefreshQuery(const FeatureCatalogEntry &feat, timestamp_t feature_ts);

} // namespace duckdb
