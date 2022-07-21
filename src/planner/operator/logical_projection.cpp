#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

LogicalProjection::LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION, move(select_list)), table_index(table_index) {
}

vector<ColumnBinding> LogicalProjection::GetColumnBindings() {
	return GenerateColumnBindings(table_index, expressions.size());
}

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

void LogicalProjection::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList<Expression>(expressions);
	writer.WriteField(table_index);
}

unique_ptr<LogicalOperator> LogicalProjection::Deserialize(FieldReader &source) {
	auto expressions = source.ReadRequiredSerializableList<Expression>();
	auto table_index = source.ReadRequired<idx_t>();
	return make_unique<LogicalProjection>(table_index, move(expressions));
}

} // namespace duckdb
