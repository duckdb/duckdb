#include "duckdb/planner/operator/logical_positional_join.hpp"

namespace duckdb {

LogicalPositionalJoin::LogicalPositionalJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_POSITIONAL_JOIN, std::move(left), std::move(right)) {
}

unique_ptr<LogicalOperator> LogicalPositionalJoin::Create(unique_ptr<LogicalOperator> left,
                                                          unique_ptr<LogicalOperator> right) {
	if (left->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return right;
	}
	if (right->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return left;
	}
	return make_unique<LogicalPositionalJoin>(std::move(left), std::move(right));
}

void LogicalPositionalJoin::Serialize(FieldWriter &writer) const {
}

unique_ptr<LogicalOperator> LogicalPositionalJoin::Deserialize(LogicalDeserializationState &state,
                                                               FieldReader &reader) {
	// TODO(stephwang): review if unique_ptr<LogicalOperator> plan is needed
	auto result = unique_ptr<LogicalPositionalJoin>(new LogicalPositionalJoin());
	return std::move(result);
}

} // namespace duckdb
