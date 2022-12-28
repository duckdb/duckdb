#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"

namespace duckdb {

void LogicalExpressionGet::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteRegularSerializableList(expr_types);

	writer.WriteField<idx_t>(expressions.size());
	for (auto &entry : expressions) {
		writer.WriteSerializableList(entry);
	}
}

unique_ptr<LogicalOperator> LogicalExpressionGet::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto expr_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();

	auto expressions_size = reader.ReadRequired<idx_t>();
	vector<vector<unique_ptr<Expression>>> expressions;
	for (idx_t i = 0; i < expressions_size; i++) {
		expressions.push_back(reader.ReadRequiredSerializableList<Expression>(state.gstate));
	}

	return make_unique<LogicalExpressionGet>(table_index, expr_types, move(expressions));
}

vector<idx_t> LogicalExpressionGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
