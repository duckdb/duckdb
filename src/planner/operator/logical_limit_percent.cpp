#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"

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
} // namespace duckdb
