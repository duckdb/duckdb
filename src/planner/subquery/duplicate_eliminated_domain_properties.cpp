//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/duplicate_eliminated_domain_properties.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/subquery/duplicate_eliminated_domain_properties.hpp"

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

static optional_idx FindPropertyBindingIndex(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
	auto entry = std::find(bindings.begin(), bindings.end(), binding);
	if (entry == bindings.end()) {
		return optional_idx();
	}
	return NumericCast<idx_t>(entry - bindings.begin());
}

static bool IsEqualityPropertyJoinCondition(const JoinCondition &condition) {
	if (!condition.IsComparison()) {
		return false;
	}
	switch (condition.GetComparisonType()) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

static bool IsColumnEqualityPredicate(Expression &expr) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		break;
	default:
		return false;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &lhs = BoundComparisonExpression::Left(comparison);
	auto &rhs = BoundComparisonExpression::Right(comparison);
	if (lhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
	    rhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	return lhs.Cast<BoundColumnRefExpression>().Depth() == 0 && rhs.Cast<BoundColumnRefExpression>().Depth() == 0;
}

static bool IsNonSelectiveJoinPredicate(Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		bool all_children_non_selective = true;
		ExpressionIterator::EnumerateChildren(
		    expr, [&](Expression &child) { all_children_non_selective &= IsNonSelectiveJoinPredicate(child); });
		return all_children_non_selective;
	}
	return IsColumnEqualityPredicate(expr);
}

bool DuplicateEliminatedDomainProperties::HasNonJoinSelection(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		for (const auto &entry : get.table_filters) {
			auto &filter = ExpressionFilter::GetExpressionFilter(
			    entry.Filter(), "DuplicateEliminatedDomainProperties::HasNonJoinSelection");
			auto &expr = *filter.expr;
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR ||
			    expr.GetExpressionType() != ExpressionType::OPERATOR_IS_NOT_NULL) {
				return true;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			if (!IsNonSelectiveJoinPredicate(*expr)) {
				return true;
			}
		}
		break;
	}
	default:
		break;
	}

	for (auto &child : op.children) {
		if (HasNonJoinSelection(*child)) {
			return true;
		}
	}
	return false;
}

class DuplicateFreeBindingAnalyzer {
public:
	explicit DuplicateFreeBindingAnalyzer(LogicalOperator &root) {
		CollectCTEs(root);
	}

	bool IsDuplicateFree(LogicalOperator &op, const vector<ColumnBinding> &bindings) {
		unordered_set<TableIndex> visiting_ctes;
		return IsDuplicateFree(op, bindings, visiting_ctes);
	}

private:
	void CollectCTEs(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && op.children.size() == 2) {
			auto &cte = op.Cast<LogicalMaterializedCTE>();
			cte_definitions.emplace(cte.table_index, *op.children[0]);
		}
		for (auto &child : op.children) {
			CollectCTEs(*child);
		}
	}

	bool IsDuplicateFree(LogicalOperator &op, const vector<ColumnBinding> &bindings,
	                     unordered_set<TableIndex> &visiting_ctes) {
		if (bindings.empty()) {
			return false;
		}
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			return AggregateIsDuplicateFree(op.Cast<LogicalAggregate>(), bindings);
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_ORDER_BY:
			return op.children.size() == 1 && IsDuplicateFree(*op.children[0], bindings, visiting_ctes);
		case LogicalOperatorType::LOGICAL_PROJECTION:
			return ProjectionIsDuplicateFree(op.Cast<LogicalProjection>(), bindings, visiting_ctes);
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			return JoinIsDuplicateFree(op.Cast<LogicalComparisonJoin>(), bindings, visiting_ctes);
		case LogicalOperatorType::LOGICAL_CTE_REF:
			return CTERefIsDuplicateFree(op.Cast<LogicalCTERef>(), bindings, visiting_ctes);
		default:
			return false;
		}
	}

	static bool AggregateIsDuplicateFree(LogicalAggregate &aggregate, const vector<ColumnBinding> &bindings) {
		if (!aggregate.grouping_functions.empty() || aggregate.grouping_sets.size() > 1) {
			return false;
		}
		if (aggregate.grouping_sets.size() == 1 && aggregate.grouping_sets[0].size() != aggregate.groups.size()) {
			return false;
		}
		for (idx_t group_idx = 0; group_idx < aggregate.groups.size(); group_idx++) {
			if (!aggregate.grouping_sets.empty() &&
			    aggregate.grouping_sets[0].find(ProjectionIndex(group_idx)) == aggregate.grouping_sets[0].end()) {
				return false;
			}
			if (!FindPropertyBindingIndex(bindings, ColumnBinding(aggregate.group_index, ProjectionIndex(group_idx)))
			         .IsValid()) {
				return false;
			}
		}
		return true;
	}

	bool ProjectionIsDuplicateFree(LogicalProjection &projection, const vector<ColumnBinding> &bindings,
	                               unordered_set<TableIndex> &visiting_ctes) {
		if (projection.children.size() != 1) {
			return false;
		}
		auto output_bindings = projection.GetColumnBindings();
		vector<ColumnBinding> child_bindings;
		child_bindings.reserve(bindings.size());
		for (auto &binding : bindings) {
			auto binding_idx = FindPropertyBindingIndex(output_bindings, binding);
			if (!binding_idx.IsValid()) {
				return false;
			}
			auto &expression = *projection.expressions[binding_idx.GetIndex()];
			if (expression.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				return false;
			}
			auto &column = expression.Cast<BoundColumnRefExpression>();
			if (column.Depth() != 0) {
				return false;
			}
			child_bindings.push_back(column.Binding());
		}
		return IsDuplicateFree(*projection.children[0], child_bindings, visiting_ctes);
	}

	bool JoinIsDuplicateFree(LogicalComparisonJoin &join, const vector<ColumnBinding> &bindings,
	                         unordered_set<TableIndex> &visiting_ctes) {
		if (join.children.size() != 2 || join.HasProjectionMap()) {
			return false;
		}
		auto left_bindings = join.children[0]->GetColumnBindings();
		auto right_bindings = join.children[1]->GetColumnBindings();
		bool bindings_from_left = true;
		bool bindings_from_right = true;
		for (auto &binding : bindings) {
			bindings_from_left &= FindPropertyBindingIndex(left_bindings, binding).IsValid();
			bindings_from_right &= FindPropertyBindingIndex(right_bindings, binding).IsValid();
		}
		if (bindings_from_left == bindings_from_right) {
			return false;
		}

		auto output_side = bindings_from_left ? 0 : 1;
		switch (join.join_type) {
		case JoinType::SEMI:
		case JoinType::ANTI:
			return output_side == 0 && IsDuplicateFree(*join.children[0], bindings, visiting_ctes);
		case JoinType::RIGHT_SEMI:
		case JoinType::RIGHT_ANTI:
			return output_side == 1 && IsDuplicateFree(*join.children[1], bindings, visiting_ctes);
		case JoinType::SINGLE:
			return output_side == 0 && IsDuplicateFree(*join.children[0], bindings, visiting_ctes);
		case JoinType::INNER:
			break;
		case JoinType::LEFT:
			if (output_side != 0) {
				return false;
			}
			break;
		case JoinType::RIGHT:
			if (output_side != 1) {
				return false;
			}
			break;
		default:
			return false;
		}

		vector<ColumnBinding> opposite_join_bindings;
		for (auto &condition : join.conditions) {
			if (!IsEqualityPropertyJoinCondition(condition)) {
				return false;
			}
			auto &opposite_expression = output_side == 0 ? condition.GetRHS() : condition.GetLHS();
			if (opposite_expression.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				return false;
			}
			auto &opposite_column = opposite_expression.Cast<BoundColumnRefExpression>();
			if (opposite_column.Depth() != 0) {
				return false;
			}
			opposite_join_bindings.push_back(opposite_column.Binding());
		}
		return IsDuplicateFree(*join.children[output_side], bindings, visiting_ctes) &&
		       IsDuplicateFree(*join.children[1 - output_side], opposite_join_bindings, visiting_ctes);
	}

	bool CTERefIsDuplicateFree(LogicalCTERef &cte_ref, const vector<ColumnBinding> &bindings,
	                           unordered_set<TableIndex> &visiting_ctes) {
		if (cte_ref.is_recurring || !visiting_ctes.insert(cte_ref.cte_index).second) {
			return false;
		}
		auto definition_entry = cte_definitions.find(cte_ref.cte_index);
		if (definition_entry == cte_definitions.end()) {
			visiting_ctes.erase(cte_ref.cte_index);
			return false;
		}
		auto ref_bindings = cte_ref.GetColumnBindings();
		auto definition_bindings = definition_entry->second.get().GetColumnBindings();
		vector<ColumnBinding> mapped_bindings;
		mapped_bindings.reserve(bindings.size());
		for (auto &binding : bindings) {
			auto binding_idx = FindPropertyBindingIndex(ref_bindings, binding);
			if (!binding_idx.IsValid() || binding_idx.GetIndex() >= definition_bindings.size()) {
				visiting_ctes.erase(cte_ref.cte_index);
				return false;
			}
			mapped_bindings.push_back(definition_bindings[binding_idx.GetIndex()]);
		}
		auto result = IsDuplicateFree(definition_entry->second.get(), mapped_bindings, visiting_ctes);
		visiting_ctes.erase(cte_ref.cte_index);
		return result;
	}

private:
	unordered_map<TableIndex, reference<LogicalOperator>> cte_definitions;
};

bool DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(LogicalComparisonJoin &join,
                                                                      LogicalOperator &root) {
	if (join.join_type != JoinType::SINGLE) {
		return false;
	}
	vector<ColumnBinding> join_bindings;
	for (auto &condition : join.conditions) {
		if (!IsEqualityPropertyJoinCondition(condition) ||
		    condition.GetRHS().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &column = condition.GetRHS().Cast<BoundColumnRefExpression>();
		if (column.Depth() != 0) {
			return false;
		}
		join_bindings.emplace_back(column.Binding());
	}
	DuplicateFreeBindingAnalyzer analyzer(root);
	return analyzer.IsDuplicateFree(*join.children[1], join_bindings);
}

} // namespace duckdb
