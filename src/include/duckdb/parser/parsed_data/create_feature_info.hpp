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

namespace duckdb {

enum class FeatureGranularity : uint8_t { DAY = 0, HOUR = 1, MINUTE = 2 };

enum class FeatureRefreshMode : uint8_t { FULL = 0, INCREMENTAL = 1 };

struct CreateFeatureInfo : public CreateInfo {
	CreateFeatureInfo();

	//! Feature name
	string feature_name;
	//! Source table name
	string source_table;
	//! Entity column (the GROUP BY key)
	string entity_column;
	//! Timestamp column (temporal ordering)
	string timestamp_column;
	//! Time granularity for bucketing
	FeatureGranularity granularity;
	//! Lookback window size (in units of granularity)
	int64_t window_size;
	//! Refresh mode
	FeatureRefreshMode refresh_mode;
	//! Number of versions to retain
	int64_t retain_versions;
	//! Current version number (incremented on each REFRESH); persisted so it survives restarts
	int64_t current_version;
	//! The SELECT query that defines the feature
	unique_ptr<SelectStatement> query;
	//! Column names of the materialized result (set during binding)
	vector<string> result_names;
	//! Column types of the materialized result (set during binding)
	vector<LogicalType> result_types;

public:
	unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
