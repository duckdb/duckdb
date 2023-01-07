#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

LogicalAggregate::LogicalAggregate(idx_t group_index, idx_t aggregate_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY, move(select_list)), group_index(group_index),
      aggregate_index(aggregate_index), groupings_index(DConstants::INVALID_INDEX) {
}

void LogicalAggregate::ResolveTypes() {
	D_ASSERT(groupings_index != DConstants::INVALID_INDEX || grouping_functions.empty());
	for (auto &expr : groups) {
		types.push_back(expr->return_type);
	}
	// get the chunk types from the projection list
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
	for (idx_t i = 0; i < grouping_functions.size(); i++) {
		types.emplace_back(LogicalType::BIGINT);
	}
}

vector<ColumnBinding> LogicalAggregate::GetColumnBindings() {
	D_ASSERT(groupings_index != DConstants::INVALID_INDEX || grouping_functions.empty());
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < groups.size(); i++) {
		result.emplace_back(group_index, i);
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		result.emplace_back(aggregate_index, i);
	}
	for (idx_t i = 0; i < grouping_functions.size(); i++) {
		result.emplace_back(groupings_index, i);
	}
	return result;
}

string LogicalAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += expressions[i]->GetName();
	}
	return result;
}

void LogicalAggregate::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(expressions);

	writer.WriteField(group_index);
	writer.WriteField(aggregate_index);
	writer.WriteField(groupings_index);
	writer.WriteSerializableList(groups);
	writer.WriteField<idx_t>(grouping_sets.size());
	for (auto &entry : grouping_sets) {
		writer.WriteList<idx_t>(entry);
	}
	writer.WriteField<idx_t>(grouping_functions.size());
	for (auto &entry : grouping_functions) {
		writer.WriteList<idx_t>(entry);
	}

	// TODO statistics
}

unique_ptr<LogicalOperator> LogicalAggregate::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);

	auto group_index = reader.ReadRequired<idx_t>();
	auto aggregate_index = reader.ReadRequired<idx_t>();
	auto groupings_index = reader.ReadRequired<idx_t>();
	auto groups = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	auto grouping_sets_size = reader.ReadRequired<idx_t>();
	vector<GroupingSet> grouping_sets;
	for (idx_t i = 0; i < grouping_sets_size; i++) {
		grouping_sets.push_back(reader.ReadRequiredSet<idx_t>());
	}
	vector<vector<idx_t>> grouping_functions;
	auto grouping_functions_size = reader.ReadRequired<idx_t>();
	for (idx_t i = 0; i < grouping_functions_size; i++) {
		grouping_functions.push_back(reader.ReadRequiredList<idx_t>());
	}
	auto result = make_unique<LogicalAggregate>(group_index, aggregate_index, move(expressions));
	result->groupings_index = groupings_index;
	result->groups = move(groups);
	result->grouping_functions = move(grouping_functions);
	result->grouping_sets = move(grouping_sets);

	return move(result);
}

idx_t LogicalAggregate::EstimateCardinality(ClientContext &context) {
	if (groups.empty()) {
		// ungrouped aggregate
		return 1;
	}
	return LogicalOperator::EstimateCardinality(context);
}

vector<idx_t> LogicalAggregate::GetTableIndex() const {
	vector<idx_t> result {group_index, aggregate_index};
	if (groupings_index != DConstants::INVALID_INDEX) {
		result.push_back(groupings_index);
	}
	return result;
}

} // namespace duckdb
