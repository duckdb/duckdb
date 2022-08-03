#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"

namespace duckdb {

void LogicalDelimJoin::Serialize(FieldWriter &writer) const {
	if (type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) { // DeliminatorPlanUpdater::VisitOperator can turn a
		                                                        // delim join into a comparison join
		return LogicalComparisonJoin::Serialize(writer);
	}
	writer.WriteField(join_type);
	writer.WriteSerializableList(duplicate_eliminated_columns);
}

unique_ptr<LogicalOperator> LogicalDelimJoin::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                          FieldReader &reader) {
	auto join_type = reader.ReadRequired<JoinType>();
	auto duplicate_eliminated_columns = reader.ReadRequiredSerializableList<Expression>(context);
	auto result = make_unique<LogicalDelimJoin>(join_type);
	result->duplicate_eliminated_columns = move(duplicate_eliminated_columns);
	return result;
}

} // namespace duckdb
