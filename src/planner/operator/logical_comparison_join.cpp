#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/enum_util.hpp"
namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = EnumUtil::ToChars(join_type);
	for (auto &condition : conditions) {
		result += "\n";
		auto expr =
		    make_uniq<BoundComparisonExpression>(condition.comparison, condition.left->Copy(), condition.right->Copy());
		result += expr->ToString();
	}

	return result;
}

void LogicalComparisonJoin::Serialize(FieldWriter &writer) const {
	LogicalJoin::Serialize(writer);
	writer.WriteRegularSerializableList(mark_types);
	writer.WriteRegularSerializableList(conditions);
}

void LogicalComparisonJoin::Deserialize(LogicalComparisonJoin &comparison_join, LogicalDeserializationState &state,
                                        FieldReader &reader) {
	LogicalJoin::Deserialize(comparison_join, state, reader);
	comparison_join.mark_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	comparison_join.conditions = reader.ReadRequiredSerializableList<JoinCondition, JoinCondition>(state.gstate);
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::Deserialize(LogicalDeserializationState &state,
                                                               FieldReader &reader) {
	auto result = make_uniq<LogicalComparisonJoin>(JoinType::INVALID, state.type);
	LogicalComparisonJoin::Deserialize(*result, state, reader);
	return std::move(result);
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::FromDelimJoin(LogicalDelimJoin &delim_join) {
	auto new_join = make_uniq<LogicalComparisonJoin>(delim_join.join_type);
	new_join->children = std::move(delim_join.children);
	new_join->conditions = std::move(delim_join.conditions);
	new_join->types = std::move(delim_join.types);
	new_join->mark_types = std::move(delim_join.mark_types);
	new_join->mark_index = delim_join.mark_index;
	new_join->left_projection_map = std::move(delim_join.left_projection_map);
	new_join->right_projection_map = std::move(delim_join.right_projection_map);
	new_join->join_stats = std::move(delim_join.join_stats);
	return std::move(new_join);
}

} // namespace duckdb
