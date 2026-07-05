//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/feature_query.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class SelectNode;
class SelectStatement;

//! Internal columns appended to every denormalized store row: which refresh produced the row and when.
//! They are excluded from user-facing output (the resolver view, SERVE, feature_at_version).
static constexpr const char *FEATURE_VERSION_COLUMN = "__feature_version";
static constexpr const char *FEATURE_TIMESTAMP_COLUMN = "__feature_timestamp";

struct FeatureSnapshotParameters {
	//! The entity table (LEFT JOIN anchor).
	string entity_table;
	//! The source/event table the feature query reads from.
	string source_table;
	//! Entity columns (event-side names); empty for global features.
	vector<string> entity_columns;
	//! Entity table key columns referenced by the foreign key, aligned to entity_columns.
	vector<string> entity_key_columns;
	//! The event timestamp column used for the window filter.
	string timestamp_column;
	//! The lookback window interval.
	interval_t window_interval;
	//! The snapshot timestamp: events in [feature_ts - window, feature_ts) are aggregated.
	timestamp_t feature_ts;
};

bool FeatureColumnListContains(const vector<string> &columns, const string &column_name);

//! Builds the SELECT statement that produces one snapshot row per entity at feature_ts:
//!   SELECT e.<key> AS <entity_col>, <coalesced feature exprs>
//!   FROM <entity_table> e
//!   LEFT JOIN (<user feature query> WHERE ts >= feature_ts - window AND ts < feature_ts) agg
//!     ON e.<key> = agg.<entity_col> ...
//! Entities with no events in the window keep NULL feature values, except SUM/COUNT aggregates which
//! default to 0. For a global feature (no entity columns) the result is a single aggregate row.
unique_ptr<SelectStatement> BuildFeatureSnapshotQuery(const SelectNode &select_node,
                                                      const FeatureSnapshotParameters &parameters);

} // namespace duckdb
