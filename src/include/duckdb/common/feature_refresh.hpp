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

//! Builds the SELECT statement whose result is the full contents of the next denormalized store table
//! (feature_name__v<new_version>): the new snapshot (one row per entity at feature_ts, tagged with
//! new_version and feature_ts) UNION ALL the still-retained rows carried forward from the previous store
//! table. Entities with no events in the window keep NULL feature values, except SUM/COUNT which default
//! to 0.
//! `entity_key_is_unique` drops the entity spine's DISTINCT when the caller has verified the entity key columns
//! are already unique (a PRIMARY KEY covers them), turning a full hash-aggregate over the entity table into a
//! plain scan. Pass false when uniqueness is unknown.
unique_ptr<SelectStatement> BuildFeatureRefreshQuery(const FeatureCatalogEntry &feat, timestamp_t feature_ts,
                                                     int64_t new_version, bool entity_key_is_unique);

} // namespace duckdb
