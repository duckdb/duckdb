#include "duckdb/planner/operator/logical_any_join.hpp"

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

unique_ptr<LogicalOperator> LogicalAnyJoin::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                        FieldReader &reader) {
	auto join_type = reader.ReadRequired<JoinType>();
	unique_ptr<Expression> condition;
	condition = reader.ReadOptional<Expression>(move(condition), context);
	auto result = make_unique<LogicalAnyJoin>(join_type);
	result->condition = move(condition);
	return result;
}

} // namespace duckdb
