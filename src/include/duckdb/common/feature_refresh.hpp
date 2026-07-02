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

class FeatureCatalogEntry;
class SelectStatement;

//! Builds the SELECT statement whose result is the full contents of the next feature version.
//! FULL:        recompute every anchor from the source table.
//! INCREMENTAL: copy the unaffected rows of the current version forward, UNION ALL the recomputed
//!              tail. The recompute boundary is expressed as an in-query scalar subquery
//!              (max(feature_timestamp) - watermark), so the statement is control-flow free and can
//!              be executed as a single plan inside the caller's transaction.
//! Every branch projects a trailing boolean marker column (TRUE for recomputed rows, FALSE for rows
//! copied forward). The refresh operator sums it to report rows_affected and strips it before
//! appending, so the marker never becomes part of the feature schema.
unique_ptr<SelectStatement> BuildFeatureRefreshQuery(const FeatureCatalogEntry &feat);

} // namespace duckdb
