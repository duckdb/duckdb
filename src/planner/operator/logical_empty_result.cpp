#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {

	this->bindings = op->GetColumnBindings();

	op->ResolveOperatorTypes();
	this->return_types = op->types;
}

LogicalEmptyResult::LogicalEmptyResult() : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
}

void LogicalEmptyResult::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(return_types);
	writer.WriteList<ColumnBinding>(bindings);
}

unique_ptr<LogicalOperator> LogicalEmptyResult::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto return_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto bindings = reader.ReadRequiredList<ColumnBinding>();
	auto result = unique_ptr<LogicalEmptyResult>(new LogicalEmptyResult());
	result->return_types = return_types;
	result->bindings = bindings;
	return std::move(result);
}

} // namespace duckdb
