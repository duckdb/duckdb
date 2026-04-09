#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/main/config.hpp"

#include <unordered_set>
#include <utility>
#include <vector>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

LogicalRecursiveCTE::LogicalRecursiveCTE() : LogicalCTE(LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
}

LogicalRecursiveCTE::LogicalRecursiveCTE(string ctename_p, TableIndex table_index, idx_t column_count, bool union_all,
                                         vector<unique_ptr<Expression>> key_targets, unique_ptr<LogicalOperator> top,
                                         unique_ptr<LogicalOperator> bottom)
    : LogicalCTE(std::move(ctename_p), table_index, column_count, std::move(top), std::move(bottom),
                 LogicalOperatorType::LOGICAL_RECURSIVE_CTE),
      union_all(union_all), key_targets(std::move(key_targets)) {
}

InsertionOrderPreservingMap<string> LogicalRecursiveCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename;
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	SetParamsEstimatedCardinality(result);
	return result;
}

vector<TableIndex> LogicalRecursiveCTE::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalRecursiveCTE::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

void LogicalRecursiveCTE::ResolveTypes() {
	types = children[0]->types;

	if (payload_aggregates.empty()) {
		return;
	}

	unordered_set<ProjectionIndex> key_idx;
	for (auto &key_target : key_targets) {
		D_ASSERT(key_target->type == ExpressionType::BOUND_COLUMN_REF);
		auto &bound_ref = key_target->Cast<BoundColumnRefExpression>();
		key_idx.insert(bound_ref.binding.column_index);
	}

	idx_t pay_idx = 0;
	for (idx_t i = 0; i < types.size(); ++i) {
		if (key_idx.find(ProjectionIndex(i)) != key_idx.end()) {
			continue;
		}
		types[i] = payload_aggregates[pay_idx++]->return_type;
	}
}

} // namespace duckdb
