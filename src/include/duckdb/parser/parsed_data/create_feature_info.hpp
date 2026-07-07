//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_feature_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/types/interval.hpp"

namespace duckdb {

struct CreateFeatureInfo : public CreateInfo {
	CreateFeatureInfo();

	//! Feature name
	string feature_name;
	//! Entity table name (one snapshot row per entity); declared via the ENTITY clause
	string entity_table;
	//! Entity columns (the entity table's primary key names, projected by the query); empty for global features
	vector<string> entity_columns;
	//! Entity table key columns, aligned to entity_columns
	vector<string> entity_key_columns;
	//! Timestamp column (temporal ordering)
	string timestamp_column;
	//! Optional table qualifier for the timestamp column (from "TIMESTAMP tbl.col"); empty if unqualified
	string timestamp_table;
	//! Lookback window interval
	interval_t window_interval;
	//! TTL / serving staleness bound (declared via the TTL clause). At SERVE time, if the entity's matched
	//! snapshot is older than this relative to the request timestamp, its feature columns resolve to NULL.
	//! A zero interval disables the bound.
	interval_t ttl_interval;
	//! Number of versions to retain
	int64_t retain_versions;
	//! Current version number (incremented on each REFRESH); persisted so it survives restarts
	int64_t current_version;
	//! Whether an automatic refresh schedule is attached to this feature
	bool has_schedule;
	//! The interval between automatic refreshes (valid when has_schedule is true)
	interval_t schedule_interval;
	//! Whether the schedule is currently active; can be toggled without dropping the interval
	bool schedule_enabled;
	//! The SELECT query that defines the feature
	unique_ptr<SelectStatement> query;
	//! Column names of the materialized result (set during binding)
	vector<string> result_names;
	//! Column types of the materialized result (set during binding)
	vector<LogicalType> result_types;

public:
	unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;
	void FinalizeDeserialization();

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
