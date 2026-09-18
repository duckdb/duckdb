#include "duckdb/planner/sql_export/logical_plan_sql_exporter_internal.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"

namespace duckdb {
namespace logical_plan_sql_export {

LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
LogicalPlanSQLExportState::ExportPivotDefault(const BoundAggregateExpression &aggregate,
                                              const LogicalPlanVerificationPath &path) {
	if (aggregate.Function().GetStability() == FunctionStability::VOLATILE ||
	    aggregate.Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
		return LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>::Failure(
		    {PlanUnsupportedFeature(path, "pivot_empty_aggregate",
		                            "A volatile or fallible PIVOT default must be evaluated before query execution")});
	}
	if (aggregate.StateExportMode() != AggregateStateExportMode::NONE) {
		return LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>::Failure({PlanUnsupportedFeature(
		    path, "pivot_empty_aggregate", "The PIVOT default requires an ordinary aggregate invocation")});
	}
	auto copy = aggregate.Copy();
	bool outer_reference = false;
	std::function<void(unique_ptr<Expression> &)> replace = [&](unique_ptr<Expression> &expression) {
		if (expression->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			if (expression->Cast<BoundColumnRefExpression>().Depth() != 0) {
				outer_reference = true;
				return;
			}
			expression = make_uniq<BoundConstantExpression>(Value(expression->GetReturnType()));
			return;
		}
		ExpressionIterator::EnumerateChildren(*expression, replace);
	};
	replace(copy);
	if (outer_reference) {
		return LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>::Failure({PlanUnsupportedFeature(
		    path, "pivot_empty_aggregate", "The PIVOT default contains an unresolved outer reference")});
	}
	auto result = BoundExpressionSQLExporter::ExportAggregateCallAtPath(copy->Cast<BoundAggregateExpression>(),
	                                                                    CreateBindingContext(context, {}), path);
	if (result.HasError()) {
		return LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>::Failure(result.GetIssues());
	}
	return LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>::Success(std::move(result.GetValue()));
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportPivot(LogicalPivot &pivot,
                                                                  const LogicalPlanVerificationPath &path) {
	D_ASSERT(pivot.children.size() == 1);
	auto fields = CreateFields(pivot, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	auto &info = pivot.bound_pivot;
	auto aggregate_count = info.aggregates.size();
	if (aggregate_count == 0 || info.group_count > fields.GetValue().size() ||
	    (fields.GetValue().size() - info.group_count) % aggregate_count != 0 ||
	    info.types.size() != fields.GetValue().size() ||
	    info.pivot_values.size() != fields.GetValue().size() - info.group_count) {
		return PlanFailure(PlanUnsupportedFeature(path, "pivot_layout", "The PIVOT output layout is incomplete"));
	}
	auto target_count = (fields.GetValue().size() - info.group_count) / aggregate_count;
	if (target_count == 0) {
		return PlanFailure(PlanUnsupportedFeature(path, "pivot_layout", "The PIVOT has no output targets"));
	}

	auto defaults = make_uniq<SelectNode>();
	defaults->from_table = make_uniq<EmptyTableRef>();
	defaults->where_clause = ConstantExpression::FromValue(Value::BOOLEAN(false));
	auto defaults_name = NextRelationAlias();
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregate_count; aggregate_idx++) {
		auto &expression = info.aggregates[aggregate_idx];
		if (!expression || expression->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return PlanFailure(
			    PlanUnsupportedFeature(path, "pivot_layout", "The PIVOT aggregate metadata is incomplete"));
		}
		auto &aggregate = expression->Cast<BoundAggregateExpression>();
		if (!aggregate.GetReturnType().EqualsIncludingCollation(
		        fields.GetValue()[info.group_count + aggregate_idx].type)) {
			return PlanFailure(PlanUnsupportedFeature(path, "pivot_layout",
			                                          "The PIVOT aggregate metadata does not match its output types"));
		}
		auto value = ExportPivotDefault(aggregate, PlanExpressionPath(path, aggregate_idx));
		if (value.HasError()) {
			return LogicalPlanSQLExportResult::Failure(value.GetIssues());
		}
		value.GetValue()->SetAlias(FieldIdentifier(aggregate_idx));
		defaults->select_list.push_back(std::move(value.GetValue()));
	}
	for (idx_t target_idx = 0; target_idx < target_count; target_idx++) {
		for (idx_t aggregate_idx = 0; aggregate_idx < aggregate_count; aggregate_idx++) {
			auto output_idx = info.group_count + target_idx * aggregate_count + aggregate_idx;
			if (info.pivot_values[target_idx * aggregate_count + aggregate_idx] !=
			        info.pivot_values[target_idx * aggregate_count] ||
			    !info.aggregates[aggregate_idx]->GetReturnType().EqualsIncludingCollation(
			        fields.GetValue()[output_idx].type) ||
			    !info.types[output_idx].EqualsIncludingCollation(fields.GetValue()[output_idx].type)) {
				return PlanFailure(PlanUnsupportedFeature(
				    path, "pivot_layout", "The PIVOT target blocks do not match the retained aggregate layout"));
			}
		}
	}

	auto child = ExportChild(*pivot.children[0], PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child.GetIssues());
	}
	if (child.GetValue().relation.fields.size() != info.group_count + aggregate_count + 1) {
		return PlanFailure(
		    PlanUnsupportedFeature(path, "pivot_layout", "The PIVOT child does not contain aligned lists"));
	}
	for (idx_t group_idx = 0; group_idx < info.group_count; group_idx++) {
		if (!info.types[group_idx].EqualsIncludingCollation(fields.GetValue()[group_idx].type) ||
		    !child.GetValue().relation.fields[group_idx].type.EqualsIncludingCollation(
		        fields.GetValue()[group_idx].type)) {
			return PlanFailure(
			    PlanUnsupportedFeature(path, "pivot_layout", "The PIVOT group types do not match its child"));
		}
	}
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregate_count; aggregate_idx++) {
		auto &list_type = child.GetValue().relation.fields[info.group_count + aggregate_idx].type;
		if (list_type.id() != LogicalTypeId::LIST || !ListType::GetChildType(list_type).EqualsIncludingCollation(
		                                                 info.aggregates[aggregate_idx]->GetReturnType())) {
			return PlanFailure(PlanUnsupportedFeature(path, "pivot_layout",
			                                          "The PIVOT aggregate list type does not match its output"));
		}
	}
	auto &key_list_type = child.GetValue().relation.fields.back().type;
	if (key_list_type.id() != LogicalTypeId::LIST ||
	    ListType::GetChildType(key_list_type).id() != LogicalTypeId::VARCHAR) {
		return PlanFailure(PlanUnsupportedFeature(path, "pivot_layout", "The PIVOT key list is not textual"));
	}

	auto call = [](const char *name, unique_ptr<ParsedExpression> first,
	               unique_ptr<ParsedExpression> second = nullptr) -> unique_ptr<ParsedExpression> {
		vector<unique_ptr<ParsedExpression>> arguments;
		arguments.push_back(std::move(first));
		if (second) {
			arguments.push_back(std::move(second));
		}
		return make_uniq<FunctionExpression>(QualifiedName("system", "main", name), std::move(arguments));
	};
	auto key_name = NextRelationAlias();
	auto encoded_keys = call("list_transform", ChildColumn(child.GetValue(), info.group_count + aggregate_count),
	                         make_uniq<LambdaExpression>(vector<string> {key_name.GetIdentifierName()},
	                                                     call("encode", make_uniq<ColumnRefExpression>(key_name))));
	auto reversed_keys = call("list_reverse", std::move(encoded_keys));
	auto length = call("len", ChildColumn(child.GetValue(), info.group_count + aggregate_count));
	auto select = make_uniq<SelectNode>();
	for (idx_t group_idx = 0; group_idx < info.group_count; group_idx++) {
		auto expression = ChildColumn(child.GetValue(), group_idx);
		expression->SetAlias(FieldIdentifier(group_idx));
		select->select_list.push_back(std::move(expression));
	}
	unordered_set<string> seen_keys;
	for (idx_t target_idx = 0; target_idx < target_count; target_idx++) {
		auto &key = info.pivot_values[target_idx * aggregate_count];
		unique_ptr<ParsedExpression> position = ConstantExpression::FromValue(Value(LogicalType::BIGINT));
		auto first_target = seen_keys.insert(key).second;
		if (first_target) {
			position =
			    call("list_position", reversed_keys->Copy(), call("encode", ConstantExpression::FromValue(Value(key))));
		}
		auto index =
		    call("-", call("+", length->Copy(), ConstantExpression::FromValue(Value::BIGINT(1))), position->Copy());
		for (idx_t aggregate_idx = 0; aggregate_idx < aggregate_count; aggregate_idx++) {
			auto expression =
			    call("list_extract", ChildColumn(child.GetValue(), info.group_count + aggregate_idx), index->Copy());
			auto fallback = make_uniq<ColumnRefExpression>(FieldIdentifier(aggregate_idx), defaults_name);
			if (!first_target) {
				expression = std::move(fallback);
			} else {
				auto result = make_uniq<CaseExpression>();
				CaseCheck check;
				check.when_expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, position->Copy());
				check.then_expr = std::move(fallback);
				result->CaseChecksMutable().push_back(std::move(check));
				result->ElseMutable() = std::move(expression);
				expression = std::move(result);
			}
			expression->SetAlias(FieldIdentifier(select->select_list.size()));
			select->select_list.push_back(std::move(expression));
		}
	}
	auto source = make_uniq<BaseTableRef>();
	source->SetTable(defaults_name);
	source->alias = defaults_name;
	auto join = make_uniq<JoinRef>(JoinRefType::CROSS);
	join->left = CreateSubquery(std::move(child.GetValue()));
	join->right = std::move(source);
	select->from_table = std::move(join);
	auto default_info = make_uniq<CommonTableExpressionInfo>();
	default_info->query_node = std::move(defaults);
	default_info->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
	select->cte_map.map.insert(defaults_name, std::move(default_info));
	vector<FunctionArgument> row;
	for (idx_t i = 0; i < select->select_list.size(); i++) {
		row.emplace_back(FieldIdentifier(i), std::move(select->select_list[i]));
	}
	auto packed_row = make_uniq<FunctionExpression>(QualifiedName("system", "main", "struct_pack"), std::move(row));
	select->select_list.clear();
	return ExportRow(std::move(select), std::move(packed_row), std::move(fields.GetValue()));
}

} // namespace logical_plan_sql_export
} // namespace duckdb
