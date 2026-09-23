#include "logical_plan_sql_exporter_internal.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"

namespace duckdb {
namespace logical_plan_sql_export {

string LogicalPlanSQLExportState::MarkConditionUnsupportedReason(const LogicalComparisonJoin &join) {
	bool comparisons_only = !join.conditions.empty();
	bool all_equal = true;
	bool all_null_safe = true;
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			comparisons_only = false;
			continue;
		}
		all_equal &= condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL;
		all_null_safe &= condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	}
	const bool has_supported_conjunction = join.conditions.size() == 1 || all_equal || all_null_safe;
	if (!comparisons_only || !has_supported_conjunction) {
		return "The MARK condition requires conjunction execution semantics";
	}
	for (auto &condition : join.conditions) {
		switch (condition.GetComparisonType()) {
		case ExpressionType::COMPARE_DISTINCT_FROM:
			return "MARK DISTINCT FROM comparisons cannot preserve NULL semantics";
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			if (condition.GetLHS().GetReturnType().IsNested() || condition.GetRHS().GetReturnType().IsNested()) {
				return "MARK ordering comparisons on nested types cannot preserve NULL semantics";
			}
			break;
		default:
			break;
		}
	}
	return string();
}

bool LogicalPlanSQLExportState::RequiresMarkGroupMetadata(const LogicalComparisonJoin &join) {
	if (join.join_type != JoinType::MARK || join.mark_types.empty()) {
		return false;
	}
	idx_t comparison_count = 0;
	bool tuple_comparison = false;
	bool all_equal = true;
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			continue;
		}
		comparison_count++;
		tuple_comparison |= condition.GetLHS().GetReturnType().id() == LogicalTypeId::TUPLE;
		all_equal &= condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL;
	}
	if (comparison_count == join.mark_types.size() + 1) {
		return true;
	}
	// Retained types also disable uncorrelated row-equality NULL handling.
	return (comparison_count > 1 || tuple_comparison) && all_equal;
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportJoin(LogicalOperator &op,
                                                                 const LogicalPlanVerificationPath &path) {
	D_ASSERT(op.children.size() == 2);
	auto fields = CreateFields(op, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	auto left = ExportChild(*op.children[0], PlanChildPath(path, 0));
	if (left.HasError()) {
		return LogicalPlanSQLExportResult::Failure(left.GetIssues());
	}
	auto right = ExportChild(*op.children[1], PlanChildPath(path, 1));
	if (right.HasError()) {
		return LogicalPlanSQLExportResult::Failure(right.GetIssues());
	}
	vector<reference<const LogicalPlanSQLExportedChild>> children {left.GetValue(), right.GetValue()};
	PropagateSemanticTypes(fields.GetValue(), children);
	auto left_plain = PlainScope(*left.GetValue().relation.query);
	auto right_plain = PlainScope(*right.GetValue().relation.query);
	if (left_plain && left_plain->where_clause) {
		left_plain = nullptr;
	}
	if (right_plain && right_plain->where_clause) {
		right_plain = nullptr;
	}
	identifier_set_t left_aliases, right_aliases;
	if (left_plain) {
		CollectScopeAliases(*left_plain->from_table, left_aliases);
		if (left_aliases.count(right.GetValue().relation_alias)) {
			left_plain = nullptr;
		}
	}
	if (right_plain) {
		CollectScopeAliases(*right_plain->from_table, right_aliases);
		if (right_aliases.count(left.GetValue().relation_alias)) {
			right_plain = nullptr;
		}
	}
	if (left_plain && right_plain) {
		for (auto &alias : left_aliases) {
			if (right_aliases.count(alias)) {
				right_plain = nullptr;
				break;
			}
		}
	}
	auto expression_context = CreateBindingContext(context, children, {left_plain, right_plain});
	auto join = make_uniq<JoinRef>();
	optional_ptr<LogicalJoin> logical_join;
	if (op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		join->ref_type = JoinRefType::CROSS;
	} else if (op.type == LogicalOperatorType::LOGICAL_POSITIONAL_JOIN) {
		join->ref_type = JoinRefType::POSITIONAL;
	} else {
		logical_join = op.Cast<LogicalJoin>();
		join->type = logical_join->join_type;
		auto expressions = CollectExpressions(op);
		if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			if (join->type == JoinType::MARK) {
				return PlanFailure(PlanUnsupportedFeature(
				    path, "mark_condition_semantics", "The MARK condition requires conjunction execution semantics"));
			}
			auto predicate = ExportExpression(op, expressions, 0, expression_context, path);
			if (predicate.HasError()) {
				return LogicalPlanSQLExportResult::Failure(predicate.GetIssues());
			}
			join->condition = std::move(predicate.GetValue());
		} else {
			auto &comparison = op.Cast<LogicalComparisonJoin>();
			if (!comparison.duplicate_eliminated_columns.empty()) {
				return PlanFailure(PlanUnsupportedFeature(path, "join_delim_state",
				                                          "The join requires a duplicate-eliminated input scope"));
			}
			if (comparison.join_type == JoinType::MARK) {
				auto reason = MarkConditionUnsupportedReason(comparison);
				if (!reason.empty()) {
					return PlanFailure(PlanUnsupportedFeature(path, "mark_condition_semantics", reason));
				}
			}
			if (RequiresMarkGroupMetadata(comparison)) {
				return PlanFailure(PlanUnsupportedFeature(path, "mark_group_null_semantics",
				                                          "The MARK join requires its group-specific NULL semantics"));
			}
			if (op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
				join->ref_type = JoinRefType::ASOF;
			}
			idx_t ordinal = 0;
			for (auto &condition : comparison.conditions) {
				auto lhs = ExportExpression(op, expressions, ordinal++, expression_context, path);
				if (lhs.HasError()) {
					return LogicalPlanSQLExportResult::Failure(lhs.GetIssues());
				}
				auto predicate = std::move(lhs.GetValue());
				if (condition.IsComparison()) {
					auto rhs = ExportExpression(op, expressions, ordinal++, expression_context, path);
					if (rhs.HasError()) {
						return LogicalPlanSQLExportResult::Failure(rhs.GetIssues());
					}
					predicate = make_uniq<ComparisonExpression>(condition.GetComparisonType(), std::move(predicate),
					                                            std::move(rhs.GetValue()));
				}
				if (join->condition) {
					join->condition = make_uniq<ConjunctionExpression>(
					    ExpressionType::CONJUNCTION_AND, std::move(join->condition), std::move(predicate));
				} else {
					join->condition = std::move(predicate);
				}
			}
			if (!join->condition) {
				join->condition = ConstantExpression::FromValue(Value::BOOLEAN(true));
			}
		}
	}
	auto select = make_uniq<SelectNode>();
	for (auto &field : fields.GetValue()) {
		unique_ptr<ParsedExpression> expression;
		if (logical_join && logical_join->join_type == JoinType::MARK &&
		    field.source_binding.table_index == logical_join->mark_index) {
			expression = make_uniq<ColumnRefExpression>(Identifier("__mark_join_marker"));
		} else {
			auto resolved = expression_context.resolve_binding(field.source_binding);
			D_ASSERT(resolved && resolved->type == field.type);
			expression = make_uniq<ColumnRefExpression>(resolved->names);
		}
		expression->SetAlias(FieldIdentifier(select->select_list.size()));
		select->select_list.push_back(std::move(expression));
	}
	join->left = left_plain ? std::move(left.GetValue().relation.query->Cast<SelectNode>().from_table)
	                        : CreateSubquery(std::move(left.GetValue()));
	join->right = right_plain ? std::move(right.GetValue().relation.query->Cast<SelectNode>().from_table)
	                          : CreateSubquery(std::move(right.GetValue()));
	select->from_table = std::move(join);
	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
}

} // namespace logical_plan_sql_export
} // namespace duckdb
