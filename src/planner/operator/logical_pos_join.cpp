#include "duckdb/planner/operator/logical_pos_join.hpp"

namespace duckdb {

LogicalPositionalJoin::LogicalPositionalJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : LogicalOperator(LogicalOperatorType::LOGICAL_POSITIONAL_JOIN) {
	D_ASSERT(left);
	D_ASSERT(right);
	children.push_back(move(left));
	children.push_back(move(right));
}

vector<ColumnBinding> LogicalPositionalJoin::GetColumnBindings() {
	auto left_bindings = children[0]->GetColumnBindings();
	auto right_bindings = children[1]->GetColumnBindings();
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

void LogicalPositionalJoin::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
}

unique_ptr<LogicalOperator> LogicalPositionalJoin::Create(unique_ptr<LogicalOperator> left,
                                                          unique_ptr<LogicalOperator> right) {
	if (left->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return right;
	}
	if (right->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return left;
	}
	return make_unique<LogicalPositionalJoin>(move(left), move(right));
}

void LogicalPositionalJoin::Serialize(FieldWriter &writer) const {
}

unique_ptr<LogicalOperator> LogicalPositionalJoin::Deserialize(LogicalDeserializationState &state,
                                                               FieldReader &reader) {
	// TODO(stephwang): review if unique_ptr<LogicalOperator> plan is needed
	auto result = unique_ptr<LogicalPositionalJoin>(new LogicalPositionalJoin());
	return move(result);
}

} // namespace duckdb
