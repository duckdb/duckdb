#include "duckdb/planner/operator/logical_aggregate.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalAggregate::LogicalAggregate(TableIndex group_index, TableIndex aggregate_index,
                                   vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY, std::move(select_list)),
      group_index(group_index), aggregate_index(aggregate_index),
      distinct_validity(TupleDataValidityType::CAN_HAVE_NULL_VALUES) {
}

bool LogicalAggregate::KeysAreUnique(LogicalOperator &op, vector<ColumnBinding> keys) {
	reference<LogicalOperator> current = op;
	while (current.get().type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &node = current.get();
		switch (node.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			// every key has to be a column the projection forwards
			auto outputs = node.GetColumnBindings();
			for (auto &key : keys) {
				auto entry = std::find(outputs.begin(), outputs.end(), key);
				if (entry == outputs.end()) {
					return false;
				}
				auto &expression = *node.expressions[NumericCast<idx_t>(entry - outputs.begin())];
				if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
				    expression.Cast<BoundColumnRefExpression>().Depth() != 0) {
					return false;
				}
				key = expression.Cast<BoundColumnRefExpression>().Binding();
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
			// removes rows, keeps the bindings
			break;
		default:
			return false;
		}
		current = *node.children[0];
	}
	auto &aggregate = current.get().Cast<LogicalAggregate>();
	if (!aggregate.grouping_functions.empty() || aggregate.grouping_sets.size() > 1) {
		// several grouping sets emit one row per set, so the groups do not determine a unique row
		return false;
	}
	if (aggregate.grouping_sets.size() == 1 && aggregate.grouping_sets[0].size() != aggregate.groups.size()) {
		// a partial grouping set: rows are unique in its groups only once the others are known to be NULL
		return false;
	}
	for (idx_t group = 0; group < aggregate.groups.size(); group++) {
		ColumnBinding binding(aggregate.group_index, ProjectionIndex(group));
		if (std::find(keys.begin(), keys.end(), binding) == keys.end()) {
			return false;
		}
	}
	return true;
}

const Expression &LogicalAggregate::GetExpression(ColumnBinding binding) const {
	if (binding.table_index == group_index) {
		return *groups[binding.column_index];
	}
	if (binding.table_index == aggregate_index) {
		return *expressions[binding.column_index];
	}
	if (binding.table_index == groupings_index) {
		throw InternalException("Groupings function does not have an expression defined");
	}
	throw InternalException("LogicalAggregate::GetExpression - incorrect table index");
}

const Expression &LogicalAggregate::GetGroupExpression(ProjectionIndex group_col_idx) const {
	return GetExpression(ColumnBinding(group_index, group_col_idx));
}

void LogicalAggregate::ResolveTypes() {
	D_ASSERT(groupings_index.IsValid() || grouping_functions.empty());
	for (auto &expr : groups) {
		types.push_back(expr->GetReturnType());
	}
	// get the chunk types from the projection list
	for (auto &expr : expressions) {
		types.push_back(expr->GetReturnType());
	}
	for (idx_t i = 0; i < grouping_functions.size(); i++) {
		types.emplace_back(LogicalType::BIGINT);
	}
}

vector<ColumnBinding> LogicalAggregate::GetColumnBindings() {
	D_ASSERT(groupings_index.IsValid() || grouping_functions.empty());
	vector<ColumnBinding> result;
	result.reserve(groups.size() + expressions.size() + grouping_functions.size());
	for (auto group_col_idx : ProjectionIndex::GetIndexes(groups.size())) {
		result.emplace_back(group_index, group_col_idx);
	}
	for (auto aggr_col_idx : ProjectionIndex::GetIndexes(expressions.size())) {
		result.emplace_back(aggregate_index, aggr_col_idx);
	}
	for (auto grouping_col_idx : ProjectionIndex::GetIndexes(grouping_functions.size())) {
		result.emplace_back(groupings_index, grouping_col_idx);
	}
	return result;
}

InsertionOrderPreservingMap<string> LogicalAggregate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string groups_info;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			groups_info += "\n";
		}
		groups_info += groups[i]->GetName();
	}
	result["Groups"] = groups_info;

	string expressions_info;
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (i > 0) {
			expressions_info += "\n";
		}
		expressions_info += expressions[i]->GetName();
	}
	result["Expressions"] = expressions_info;
	SetParamsEstimatedCardinality(result);
	return result;
}

idx_t LogicalAggregate::EstimateCardinality(ClientContext &context) {
	if (groups.empty()) {
		// ungrouped aggregate
		return 1;
	}
	return LogicalOperator::EstimateCardinality(context);
}

vector<TableIndex> LogicalAggregate::GetTableIndex() const {
	vector<TableIndex> result {group_index, aggregate_index};
	if (groupings_index.IsValid()) {
		result.push_back(groupings_index);
	}
	return result;
}

string LogicalAggregate::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu, #%llu, #%llu", group_index.index,
		                                                       aggregate_index.index, groupings_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
