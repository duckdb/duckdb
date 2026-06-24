#include "duckdb/optimizer/materialized_cte_optimizer.hpp"

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

static idx_t CountMaterializedCTEReferences(const LogicalOperator &op, TableIndex cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte = op.Cast<LogicalCTERef>();
		if (cte.cte_index == cte_index) {
			return 1;
		}
	}
	idx_t number_of_references = 0;
	for (auto &child : op.children) {
		number_of_references += CountMaterializedCTEReferences(*child, cte_index);
	}

	return number_of_references;
}

static void GatherCTERefBindings(const LogicalOperator &op, TableIndex cte_index, unordered_set<TableIndex> &bindings) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte = op.Cast<LogicalCTERef>();
		if (cte.cte_index == cte_index) {
			bindings.insert(cte.table_index);
		}
	}
	for (auto &child : op.children) {
		GatherCTERefBindings(*child, cte_index, bindings);
	}
}

static bool CTEExpressionsUseOnlyKeys(const LogicalOperator &op, const unordered_set<TableIndex> &cte_bindings,
                                      const unordered_set<ProjectionIndex> &key_columns) {
	for (auto &expr : op.expressions) {
		bool valid = true;
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *expr, [&](const BoundColumnRefExpression &colref) {
			    if (cte_bindings.find(colref.binding.table_index) != cte_bindings.end() &&
			        key_columns.find(colref.binding.column_index) == key_columns.end()) {
				    valid = false;
			    }
		    });
		if (!valid) {
			return false;
		}
	}
	for (auto &child : op.children) {
		if (!CTEExpressionsUseOnlyKeys(*child, cte_bindings, key_columns)) {
			return false;
		}
	}
	return true;
}

static bool JoinRHSIsDuplicateInsensitive(JoinType join_type) {
	return join_type == JoinType::MARK || join_type == JoinType::SEMI || join_type == JoinType::ANTI;
}

static bool JoinLHSIsDuplicateInsensitive(JoinType join_type) {
	return join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI;
}

static bool IsLogicalJoin(const LogicalOperator &op) {
	return op.type == LogicalOperatorType::LOGICAL_JOIN || op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	       op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
	       op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN || op.type == LogicalOperatorType::LOGICAL_ANY_JOIN;
}

static bool CTERefsAreDuplicateInsensitive(const LogicalOperator &op, TableIndex cte_index,
                                           bool duplicate_insensitive) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte = op.Cast<LogicalCTERef>();
		return cte.cte_index != cte_index || duplicate_insensitive;
	}

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
		duplicate_insensitive = false;
		break;
	default:
		break;
	}

	if (IsLogicalJoin(op)) {
		auto &join = op.Cast<LogicalJoin>();
		D_ASSERT(op.children.size() == 2);
		auto lhs_duplicate_insensitive = JoinLHSIsDuplicateInsensitive(join.join_type);
		auto rhs_duplicate_insensitive = JoinRHSIsDuplicateInsensitive(join.join_type);
		if (join.join_type == JoinType::INNER) {
			lhs_duplicate_insensitive = duplicate_insensitive;
			rhs_duplicate_insensitive = duplicate_insensitive;
		}
		return CTERefsAreDuplicateInsensitive(*op.children[0], cte_index, lhs_duplicate_insensitive) &&
		       CTERefsAreDuplicateInsensitive(*op.children[1], cte_index, rhs_duplicate_insensitive);
	}

	for (auto &child : op.children) {
		if (!CTERefsAreDuplicateInsensitive(*child, cte_index, duplicate_insensitive)) {
			return false;
		}
	}
	return true;
}

static bool CTEIsOnlyUsedForKeyExistence(const LogicalMaterializedCTE &cte,
                                         const unordered_set<ProjectionIndex> &key_columns) {
	unordered_set<TableIndex> cte_bindings;
	GatherCTERefBindings(*cte.children[1], cte.table_index, cte_bindings);
	if (cte_bindings.empty()) {
		return false;
	}
	return CTEExpressionsUseOnlyKeys(*cte.children[1], cte_bindings, key_columns) &&
	       CTERefsAreDuplicateInsensitive(*cte.children[1], cte.table_index, false);
}

static bool SameTableScan(const LogicalGet &lhs, const LogicalGet &rhs) {
	auto left_table = lhs.GetTable();
	auto right_table = rhs.GetTable();
	if (left_table || right_table) {
		return left_table && right_table && left_table.get() == right_table.get();
	}
	if (lhs.function.name != rhs.function.name || lhs.returned_types != rhs.returned_types || lhs.names != rhs.names ||
	    lhs.parameters.size() != rhs.parameters.size() || lhs.named_parameters.size() != rhs.named_parameters.size()) {
		return false;
	}
	for (idx_t i = 0; i < lhs.parameters.size(); i++) {
		if (lhs.parameters[i] != rhs.parameters[i]) {
			return false;
		}
	}
	for (auto &entry : lhs.named_parameters) {
		auto rhs_entry = rhs.named_parameters.find(entry.first);
		if (rhs_entry == rhs.named_parameters.end() || entry.second != rhs_entry->second) {
			return false;
		}
	}
	return true;
}

static optional_ptr<const BoundColumnRefExpression> GetColumnRef(const Expression &expr);

static optional_ptr<const LogicalGet> GetUnderlyingGet(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return &op.Cast<LogicalGet>();
	}
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION || op.children.size() != 1) {
		return nullptr;
	}
	return GetUnderlyingGet(*op.children[0]);
}

static bool SameProjectionShape(const LogicalProjection &lhs, const LogicalProjection &rhs) {
	if (lhs.expressions.size() != rhs.expressions.size()) {
		return false;
	}
	for (idx_t i = 0; i < lhs.expressions.size(); i++) {
		auto lhs_colref = GetColumnRef(*lhs.expressions[i]);
		auto rhs_colref = GetColumnRef(*rhs.expressions[i]);
		if (!lhs_colref || !rhs_colref || lhs_colref->binding.column_index != rhs_colref->binding.column_index) {
			return false;
		}
	}
	return true;
}

static bool SameSourceOperator(const LogicalOperator &lhs, const LogicalOperator &rhs) {
	auto lhs_get = GetUnderlyingGet(lhs);
	auto rhs_get = GetUnderlyingGet(rhs);
	if (!lhs_get || !rhs_get || !SameTableScan(*lhs_get, *rhs_get)) {
		return false;
	}
	if (lhs.type == LogicalOperatorType::LOGICAL_GET || rhs.type == LogicalOperatorType::LOGICAL_GET) {
		return lhs.type == rhs.type;
	}
	if (lhs.type != rhs.type || lhs.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	return SameProjectionShape(lhs.Cast<LogicalProjection>(), rhs.Cast<LogicalProjection>());
}

static optional_ptr<const BoundColumnRefExpression> GetColumnRef(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	return &expr.Cast<BoundColumnRefExpression>();
}

struct ColumnComparison {
	ExpressionType comparison_type;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
	LogicalType left_type;
	LogicalType right_type;
};

enum class KeyVariationProjectionType : uint8_t { KEY, MIN_VALUE, MAX_VALUE };

struct KeyVariationProjectionColumn {
	KeyVariationProjectionType type;
	idx_t key_index = DConstants::INVALID_INDEX;
};

struct KeyVariationCTEInfo {
	vector<ColumnBinding> key_bindings;
	vector<ColumnBinding> right_key_bindings;
	vector<LogicalType> key_types;
	vector<KeyVariationProjectionColumn> projection_columns;
	unordered_set<ProjectionIndex> key_columns;
	ColumnBinding value_binding;
	LogicalType value_type;
	optional_ptr<unique_ptr<LogicalOperator>> scan_ref;
};

static bool GetComparisonBindings(const Expression &expr, ColumnComparison &result) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left_expr = BoundComparisonExpression::Left(comparison);
	auto &right_expr = BoundComparisonExpression::Right(comparison);
	auto left = GetColumnRef(left_expr);
	auto right = GetColumnRef(right_expr);
	if (!left || !right) {
		return false;
	}
	result.comparison_type = comparison.GetExpressionType();
	result.left_binding = left->binding;
	result.right_binding = right->binding;
	result.left_type = left_expr.GetReturnType();
	result.right_type = right_expr.GetReturnType();
	return true;
}

static bool GetComparisonBindings(const JoinCondition &condition, ColumnComparison &result) {
	if (!condition.IsComparison()) {
		return false;
	}
	auto left = GetColumnRef(condition.GetLHS());
	auto right = GetColumnRef(condition.GetRHS());
	if (!left || !right) {
		return false;
	}
	result.comparison_type = condition.GetComparisonType();
	result.left_binding = left->binding;
	result.right_binding = right->binding;
	result.left_type = condition.GetLHS().GetReturnType();
	result.right_type = condition.GetRHS().GetReturnType();
	return true;
}

static bool AddFilterComparisons(LogicalFilter &filter, vector<ColumnComparison> &comparisons) {
	for (auto &expr : filter.expressions) {
		ColumnComparison comparison;
		if (!GetComparisonBindings(*expr, comparison)) {
			return false;
		}
		comparisons.push_back(std::move(comparison));
	}
	return true;
}

static bool AddJoinComparisons(const LogicalComparisonJoin &join, vector<ColumnComparison> &comparisons) {
	for (auto &condition : join.conditions) {
		ColumnComparison comparison;
		if (!GetComparisonBindings(condition, comparison)) {
			return false;
		}
		comparisons.push_back(std::move(comparison));
	}
	return true;
}

static bool ExtractSelfJoinInput(unique_ptr<LogicalOperator> &input, vector<ColumnComparison> &comparisons,
                                 optional_ptr<unique_ptr<LogicalOperator>> &left_ref,
                                 optional_ptr<unique_ptr<LogicalOperator>> &right_ref) {
	if (input->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = input->Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::INNER || join.children.size() != 2 || !AddJoinComparisons(join, comparisons)) {
			return false;
		}
		left_ref = &join.children[0];
		right_ref = &join.children[1];
		return true;
	}

	if (input->type != LogicalOperatorType::LOGICAL_FILTER) {
		return false;
	}
	auto &filter = input->Cast<LogicalFilter>();
	if (!AddFilterComparisons(filter, comparisons) || filter.children.size() != 1) {
		return false;
	}

	auto &child = filter.children[0];
	if (child->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		auto &cross_product = child->Cast<LogicalCrossProduct>();
		if (cross_product.children.size() != 2) {
			return false;
		}
		left_ref = &cross_product.children[0];
		right_ref = &cross_product.children[1];
		return true;
	}
	if (child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = child->Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::INNER || join.children.size() != 2 || !AddJoinComparisons(join, comparisons)) {
			return false;
		}
		left_ref = &join.children[0];
		right_ref = &join.children[1];
		return true;
	}
	return false;
}

static bool OperatorOutputsBinding(LogicalOperator &op, ColumnBinding binding) {
	for (auto &op_binding : op.GetColumnBindings()) {
		if (op_binding == binding) {
			return true;
		}
	}
	return false;
}

static bool GetChildBindingPair(LogicalOperator &left_op, LogicalOperator &right_op, const ColumnComparison &comparison,
                                ColumnBinding &left_binding, ColumnBinding &right_binding, LogicalType &left_type,
                                LogicalType &right_type) {
	auto comparison_left_is_left = OperatorOutputsBinding(left_op, comparison.left_binding);
	auto comparison_left_is_right = OperatorOutputsBinding(right_op, comparison.left_binding);
	auto comparison_right_is_left = OperatorOutputsBinding(left_op, comparison.right_binding);
	auto comparison_right_is_right = OperatorOutputsBinding(right_op, comparison.right_binding);

	if (comparison_left_is_left && comparison_right_is_right && !comparison_left_is_right &&
	    !comparison_right_is_left) {
		left_binding = comparison.left_binding;
		right_binding = comparison.right_binding;
		left_type = comparison.left_type;
		right_type = comparison.right_type;
		return true;
	}
	if (comparison_left_is_right && comparison_right_is_left && !comparison_left_is_left &&
	    !comparison_right_is_right) {
		left_binding = comparison.right_binding;
		right_binding = comparison.left_binding;
		left_type = comparison.right_type;
		right_type = comparison.left_type;
		return true;
	}
	return false;
}

static bool KeyAlreadyMatched(const vector<ColumnBinding> &key_bindings, ColumnBinding binding) {
	for (auto &key_binding : key_bindings) {
		if (key_binding == binding) {
			return true;
		}
	}
	return false;
}

static bool FindKeyBinding(const KeyVariationCTEInfo &info, ColumnBinding binding, idx_t &key_index) {
	for (idx_t i = 0; i < info.key_bindings.size(); i++) {
		if (info.key_bindings[i] == binding || info.right_key_bindings[i] == binding) {
			key_index = i;
			return true;
		}
	}
	return false;
}

static bool MatchKeyVariationSelfJoinCTE(unique_ptr<LogicalOperator> &definition, KeyVariationCTEInfo &result) {
	if (definition->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	auto &projection = definition->Cast<LogicalProjection>();
	if (projection.children.size() != 1) {
		return false;
	}

	vector<ColumnComparison> comparisons;
	optional_ptr<unique_ptr<LogicalOperator>> left_ref;
	optional_ptr<unique_ptr<LogicalOperator>> right_ref;
	if (!ExtractSelfJoinInput(projection.children[0], comparisons, left_ref, right_ref)) {
		return false;
	}
	if (!left_ref || !right_ref || !SameSourceOperator(**left_ref, **right_ref)) {
		return false;
	}

	bool found_value_inequality = false;
	ColumnBinding left_value_binding;
	ColumnBinding right_value_binding;
	LogicalType left_value_type;
	for (auto &comparison : comparisons) {
		if (comparison.comparison_type != ExpressionType::COMPARE_NOTEQUAL) {
			continue;
		}
		ColumnBinding left_binding;
		ColumnBinding right_binding;
		LogicalType left_type;
		LogicalType right_type;
		if (!GetChildBindingPair(**left_ref, **right_ref, comparison, left_binding, right_binding, left_type,
		                         right_type) ||
		    left_binding.column_index != right_binding.column_index || left_type != right_type ||
		    found_value_inequality) {
			return false;
		}
		found_value_inequality = true;
		left_value_binding = left_binding;
		right_value_binding = right_binding;
		left_value_type = left_type;
	}
	if (!found_value_inequality) {
		return false;
	}

	for (auto &comparison : comparisons) {
		if (comparison.comparison_type == ExpressionType::COMPARE_NOTEQUAL) {
			continue;
		}
		if (comparison.comparison_type != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		ColumnBinding left_binding;
		ColumnBinding right_binding;
		LogicalType left_type;
		LogicalType right_type;
		if (!GetChildBindingPair(**left_ref, **right_ref, comparison, left_binding, right_binding, left_type,
		                         right_type) ||
		    left_binding.column_index != right_binding.column_index || left_type != right_type) {
			return false;
		}
		if (left_binding == left_value_binding || KeyAlreadyMatched(result.key_bindings, left_binding)) {
			continue;
		}
		result.key_bindings.push_back(left_binding);
		result.right_key_bindings.push_back(right_binding);
		result.key_types.push_back(left_type);
	}
	if (result.key_bindings.empty()) {
		return false;
	}

	vector<bool> projected_keys(result.key_bindings.size(), false);
	result.projection_columns.reserve(projection.expressions.size());
	for (idx_t projection_idx = 0; projection_idx < projection.expressions.size(); projection_idx++) {
		auto colref = GetColumnRef(*projection.expressions[projection_idx]);
		if (!colref) {
			return false;
		}

		KeyVariationProjectionColumn projection_column;
		if (colref->binding == left_value_binding) {
			projection_column.type = KeyVariationProjectionType::MIN_VALUE;
		} else if (colref->binding == right_value_binding) {
			projection_column.type = KeyVariationProjectionType::MAX_VALUE;
		} else {
			idx_t key_index;
			if (!FindKeyBinding(result, colref->binding, key_index)) {
				return false;
			}
			projection_column.type = KeyVariationProjectionType::KEY;
			projection_column.key_index = key_index;
			projected_keys[key_index] = true;
			result.key_columns.insert(ProjectionIndex(projection_idx));
		}
		result.projection_columns.push_back(projection_column);
	}
	for (auto projected : projected_keys) {
		if (!projected) {
			return false;
		}
	}
	result.value_binding = left_value_binding;
	result.value_type = left_value_type;
	result.scan_ref = left_ref;
	return true;
}

static unique_ptr<LogicalOperator> CreateGroupedKeyVariationCTE(Optimizer &optimizer, KeyVariationCTEInfo &info,
                                                                TableIndex projection_index) {
	auto value_expr = make_uniq<BoundColumnRefExpression>(info.value_type, info.value_binding);

	vector<unique_ptr<Expression>> aggregate_expressions;
	FunctionBinder function_binder(optimizer.GetContext());
	vector<unique_ptr<Expression>> min_children;
	min_children.push_back(value_expr->Copy());
	aggregate_expressions.push_back(function_binder.BindAggregateFunction(
	    MinFunction::GetFunction(), std::move(min_children), nullptr, AggregateType::NON_DISTINCT));
	vector<unique_ptr<Expression>> max_children;
	max_children.push_back(std::move(value_expr));
	aggregate_expressions.push_back(function_binder.BindAggregateFunction(
	    MaxFunction::GetFunction(), std::move(max_children), nullptr, AggregateType::NON_DISTINCT));

	auto group_index = optimizer.binder.GenerateTableIndex();
	auto aggregate_index = optimizer.binder.GenerateTableIndex();
	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregate_expressions));
	for (idx_t i = 0; i < info.key_bindings.size(); i++) {
		aggregate->groups.push_back(make_uniq<BoundColumnRefExpression>(info.key_types[i], info.key_bindings[i]));
	}
	D_ASSERT(info.scan_ref);
	auto input = std::move(*info.scan_ref);
	auto key_filter = make_uniq<LogicalFilter>();
	for (idx_t i = 0; i < info.key_bindings.size(); i++) {
		auto key_not_null =
		    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		key_not_null->children.push_back(make_uniq<BoundColumnRefExpression>(info.key_types[i], info.key_bindings[i]));
		key_filter->expressions.push_back(std::move(key_not_null));
	}
	key_filter->children.push_back(std::move(input));
	aggregate->children.push_back(std::move(key_filter));

	auto min_binding = ColumnBinding(aggregate_index, ProjectionIndex(0));
	auto max_binding = ColumnBinding(aggregate_index, ProjectionIndex(1));
	auto filter = make_uniq<LogicalFilter>(BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_NOTEQUAL, make_uniq<BoundColumnRefExpression>(info.value_type, min_binding),
	    make_uniq<BoundColumnRefExpression>(info.value_type, max_binding)));
	filter->children.push_back(std::move(aggregate));

	vector<unique_ptr<Expression>> projection_expressions;
	for (auto &projection_column : info.projection_columns) {
		switch (projection_column.type) {
		case KeyVariationProjectionType::KEY:
			projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    info.key_types[projection_column.key_index],
			    ColumnBinding(group_index, ProjectionIndex(projection_column.key_index))));
			break;
		case KeyVariationProjectionType::MIN_VALUE:
			projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(info.value_type, min_binding));
			break;
		case KeyVariationProjectionType::MAX_VALUE:
			projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(info.value_type, max_binding));
			break;
		default:
			throw InternalException("Unsupported key variation projection type");
		}
	}
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projection_expressions));
	projection->children.push_back(std::move(filter));
	return std::move(projection);
}

static bool TryRewriteKeyExistenceSelfJoin(Optimizer &optimizer, LogicalMaterializedCTE &cte) {
	if (cte.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	auto projection_index = cte.children[0]->Cast<LogicalProjection>().table_index;
	KeyVariationCTEInfo info;
	if (!MatchKeyVariationSelfJoinCTE(cte.children[0], info)) {
		return false;
	}
	if (!CTEIsOnlyUsedForKeyExistence(cte, info.key_columns)) {
		return false;
	}
	cte.children[0] = CreateGroupedKeyVariationCTE(optimizer, info, projection_index);
	return true;
}

} // namespace

MaterializedCTEOptimizer::MaterializedCTEOptimizer(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> MaterializedCTEOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	VisitOperator(*op);
	return op;
}

void MaterializedCTEOptimizer::VisitOperator(LogicalOperator &op) {
	for (auto &child : op.children) {
		VisitOperator(*child);
	}
	if (op.type != LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return;
	}
	auto &cte = op.Cast<LogicalMaterializedCTE>();
	if (cte.children[0]->HasSideEffects() || CountMaterializedCTEReferences(op, cte.table_index) == 0) {
		return;
	}
	TryRewriteKeyExistenceSelfJoin(optimizer, cte);
}

} // namespace duckdb
