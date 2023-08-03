#include "duckdb/planner/operator/logical_pivot.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalPivot::LogicalPivot() : LogicalOperator(LogicalOperatorType::LOGICAL_PIVOT) {
}

LogicalPivot::LogicalPivot(idx_t pivot_idx, unique_ptr<LogicalOperator> plan, BoundPivotInfo info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PIVOT), pivot_index(pivot_idx), bound_pivot(std::move(info_p)) {
	D_ASSERT(plan);
	children.push_back(std::move(plan));
}

vector<ColumnBinding> LogicalPivot::GetColumnBindings() {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < bound_pivot.types.size(); i++) {
		result.emplace_back(pivot_index, i);
	}
	return result;
}

void LogicalPivot::Serialize(FieldWriter &writer) const {
	writer.WriteField(pivot_index);
	writer.WriteOptional<LogicalOperator>(children.back());
	writer.WriteField(bound_pivot.group_count);
	writer.WriteRegularSerializableList<LogicalType>(bound_pivot.types);
	writer.WriteList<string>(bound_pivot.pivot_values);
	writer.WriteSerializableList<Expression>(bound_pivot.aggregates);
}

unique_ptr<LogicalOperator> LogicalPivot::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto pivot_index = reader.ReadRequired<idx_t>();
	auto plan = reader.ReadOptional<LogicalOperator>(nullptr, state.gstate);
	BoundPivotInfo info;
	info.group_count = reader.ReadRequired<idx_t>();
	info.types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	info.pivot_values = reader.ReadRequiredList<string>();
	info.aggregates = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	return make_uniq<LogicalPivot>(pivot_index, std::move(plan), std::move(info));
}

vector<idx_t> LogicalPivot::GetTableIndex() const {
	return vector<idx_t> {pivot_index};
}

void LogicalPivot::ResolveTypes() {
	this->types = bound_pivot.types;
}

string LogicalPivot::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", pivot_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
