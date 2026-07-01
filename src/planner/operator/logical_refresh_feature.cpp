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
}

unique_ptr<LogicalOperator> LogicalRefreshFeature::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<LogicalRefreshFeature>();
	result->feature_name = deserializer.ReadProperty<string>(200, "feature_name");
	return std::move(result);
}

idx_t LogicalRefreshFeature::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
