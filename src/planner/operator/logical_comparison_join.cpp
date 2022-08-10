#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = JoinTypeToString(join_type);
	for (auto &condition : conditions) {
		result += "\n";
		auto expr = make_unique<BoundComparisonExpression>(condition.comparison, condition.left->Copy(),
		                                                   condition.right->Copy());
		result += expr->ToString();
	}

	return result;
}

void LogicalComparisonJoin::Serialize(FieldWriter &writer) const {
	LogicalJoin::Serialize(writer);
	writer.WriteRegularSerializableList(conditions);
	writer.WriteRegularSerializableList(delim_types);
}

void LogicalComparisonJoin::Deserialize(LogicalComparisonJoin &comparison_join, ClientContext &context,
                                        LogicalOperatorType type, FieldReader &reader) {
	LogicalJoin::Deserialize(comparison_join, context, type, reader);
	comparison_join.conditions =
	    reader.ReadRequiredSerializableList<JoinCondition, JoinCondition, ClientContext &>(context);
	comparison_join.delim_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
}
unique_ptr<LogicalOperator> LogicalComparisonJoin::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                               FieldReader &reader) {
	auto result = make_unique<LogicalComparisonJoin>(JoinType::INVALID, type);
	LogicalComparisonJoin::Deserialize(*result, context, type, reader);
	return result;
}

} // namespace duckdb
