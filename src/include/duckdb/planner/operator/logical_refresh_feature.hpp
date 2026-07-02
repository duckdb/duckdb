//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_refresh_feature.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalRefreshFeature represents a REFRESH FEATURE command. Its single child produces the full
//! contents of the next feature version; the operator materializes that into a new version table.
class LogicalRefreshFeature : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_REFRESH_FEATURE;

public:
	LogicalRefreshFeature();
	explicit LogicalRefreshFeature(string feature_name);

	string feature_name;
	//! Column names/types of the new version table (schema of the child query result)
	vector<string> result_names;
	vector<LogicalType> result_types;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BIGINT);
	}
};

} // namespace duckdb
