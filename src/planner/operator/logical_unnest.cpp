#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"

namespace duckdb {

vector<ColumnBinding> LogicalUnnest::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	for (idx_t i = 0; i < expressions.size(); i++) {
		child_bindings.emplace_back(unnest_index, i);
	}
	return child_bindings;
}

void LogicalUnnest::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

void LogicalUnnest::Serialize(FieldWriter &writer) const {
	writer.WriteField(unnest_index);
	writer.WriteSerializableList<Expression>(expressions);
}

unique_ptr<LogicalOperator> LogicalUnnest::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto unnest_index = reader.ReadRequired<idx_t>();
	auto expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	auto result = make_unique<LogicalUnnest>(unnest_index);
	result->expressions = std::move(expressions);
	return std::move(result);
}

vector<idx_t> LogicalUnnest::GetTableIndex() const {
	return vector<idx_t> {unnest_index};
}

} // namespace duckdb
