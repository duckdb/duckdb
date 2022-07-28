#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/string_util.hpp"
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
	writer.WriteField(join_type);
	writer.WriteRegularSerializableList(conditions);
	writer.WriteList<LogicalType>(delim_types);
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                               FieldReader &reader) {
	auto join_type = reader.ReadRequired<JoinType>();
	auto conditions = reader.ReadRequiredList<JoinCondition>();
	auto delim_types = reader.ReadRequiredList<LogicalType>();
	auto result = make_unique<LogicalComparisonJoin>(join_type, type);
	result->conditions = move(conditions);
	result->delim_types = delim_types;
	return result;
}

} // namespace duckdb
