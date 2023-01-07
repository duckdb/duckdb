#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"
#include <cmath>

namespace duckdb {

void LogicalLimitPercent::Serialize(FieldWriter &writer) const {
	writer.WriteField(limit_percent);
	writer.WriteField(offset_val);
	writer.WriteOptional(limit);
	writer.WriteOptional(offset);
}

unique_ptr<LogicalOperator> LogicalLimitPercent::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto limit_percent = reader.ReadRequired<double>();
	auto offset_val = reader.ReadRequired<int64_t>();
	auto limit = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto offset = reader.ReadOptional<Expression>(nullptr, state.gstate);
	return make_unique<LogicalLimitPercent>(limit_percent, offset_val, move(limit), move(offset));
}

idx_t LogicalLimitPercent::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = LogicalOperator::EstimateCardinality(context);
	if ((limit_percent < 0 || limit_percent > 100) || std::isnan(limit_percent)) {
		return child_cardinality;
	}
	return idx_t(child_cardinality * (limit_percent / 100.0));
}

} // namespace duckdb
