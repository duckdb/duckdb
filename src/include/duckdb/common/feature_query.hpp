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
class SubqueryRef;

//! Internal columns appended to every denormalized store row: which refresh produced the row and when.
//! They are excluded from user-facing output (the resolver view, SERVE, feature_at_version).
static constexpr const char *FEATURE_VERSION_COLUMN = "__feature_version";
static constexpr const char *FEATURE_TIMESTAMP_COLUMN = "__feature_timestamp";

//! The single denormalized store table backing a feature. Every refresh appends its snapshot here and
//! evicts versions that have fallen outside the retain window; the table itself persists across refreshes.
inline string FeatureStoreTableName(const string &feature_name) {
	return feature_name + "__store";
}

struct FeatureSnapshotParameters {
	//! The entity table the snapshot is built over (one output row per entity).
	string entity_table;
	//! Entity columns (entity table primary key names); empty for global features.
	vector<string> entity_columns;
	//! Entity table key columns referenced by the foreign key, aligned to entity_columns.
	vector<string> entity_key_columns;
	//! The event timestamp column used for the window filter.
	string timestamp_column;
	//! Optional table qualifier for the timestamp column (empty if the reference is unqualified).
	string timestamp_table;
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

//! One row per entity out of the denormalized store at a specific version, with the internal bookkeeping
//! columns (__feature_version, __feature_timestamp) hidden. Built as an AST subquery and bound like any
//! other subquery — no shadow view, table function, or side connection is involved. Used both for
//! "FROM my_feature" (current version) and "feature_at_version(name, version)" (an explicit version).
unique_ptr<SubqueryRef> BuildFeatureVersionRef(const string &catalog_name, const string &schema_name,
                                               const string &feature_name, int64_t version, const string &alias,
                                               const vector<string> &column_name_alias);

} // namespace duckdb
