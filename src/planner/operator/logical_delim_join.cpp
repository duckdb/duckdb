#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"

namespace duckdb {

LogicalDelimJoin::LogicalDelimJoin(JoinType type)
    : LogicalComparisonJoin(type, LogicalOperatorType::LOGICAL_DELIM_JOIN) {
}

void LogicalDelimJoin::Serialize(FieldWriter &writer) const {
	LogicalComparisonJoin::Serialize(writer);
	if (type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		throw InternalException("LogicalDelimJoin needs to have type LOGICAL_DELIM_JOIN");
	}
	writer.WriteSerializableList(duplicate_eliminated_columns);
}

unique_ptr<LogicalOperator> LogicalDelimJoin::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto result = make_uniq<LogicalDelimJoin>(JoinType::INVALID);
	LogicalComparisonJoin::Deserialize(*result, state, reader);
	result->duplicate_eliminated_columns = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	return std::move(result);
}

} // namespace duckdb
