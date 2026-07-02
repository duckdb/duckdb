#include "duckdb/planner/operator/logical_refresh_feature.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

LogicalRefreshFeature::LogicalRefreshFeature() : LogicalOperator(LogicalOperatorType::LOGICAL_REFRESH_FEATURE) {
}

LogicalRefreshFeature::LogicalRefreshFeature(string feature_name_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_REFRESH_FEATURE), feature_name(std::move(feature_name_p)) {
}

void LogicalRefreshFeature::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty(200, "feature_name", feature_name);
	serializer.WriteProperty(201, "result_names", result_names);
	serializer.WriteProperty(202, "result_types", result_types);
}

unique_ptr<LogicalOperator> LogicalRefreshFeature::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<LogicalRefreshFeature>();
	result->feature_name = deserializer.ReadProperty<string>(200, "feature_name");
	result->result_names = deserializer.ReadProperty<vector<string>>(201, "result_names");
	result->result_types = deserializer.ReadProperty<vector<LogicalType>>(202, "result_types");
	return std::move(result);
}

idx_t LogicalRefreshFeature::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
