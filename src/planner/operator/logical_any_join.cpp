#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

LogicalAnyJoin::LogicalAnyJoin(JoinType type) : LogicalJoin(type, LogicalOperatorType::LOGICAL_ANY_JOIN) {
}

string LogicalAnyJoin::ParamsToString() const {
	return condition->ToString();
}

void LogicalAnyJoin::Serialize(FieldWriter &writer) const {
	writer.WriteField(join_type);
	writer.WriteOptional(condition);
}

unique_ptr<LogicalOperator> LogicalAnyJoin::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto join_type = reader.ReadRequired<JoinType>();
	auto condition = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto result = make_unique<LogicalAnyJoin>(join_type);
	result->condition = std::move(condition);
	return std::move(result);
}

} // namespace duckdb
