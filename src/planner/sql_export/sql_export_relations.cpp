#include "duckdb/planner/sql_export/logical_plan_sql_exporter_internal.hpp"
#include "duckdb/function/scalar/compressed_materialization_utils.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

namespace duckdb {
namespace logical_plan_sql_export {

static LogicalType SemanticExpressionType(const Expression &expression,
                                          const BoundExpressionSQLExportContext &context) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF && context.resolve_binding) {
		auto &column = expression.Cast<BoundColumnRefExpression>();
		auto resolved = context.resolve_binding(column.Binding());
		if (resolved && (resolved->type == column.GetReturnType() ||
		                 (resolved->optimizer_type && *resolved->optimizer_type == column.GetReturnType()))) {
			return resolved->type;
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expression.Cast<BoundFunctionExpression>();
		if (context.discard_optimizer_metadata && CMUtils::GetExpressionType(function) != CMExpressionType::NONE &&
		    !function.GetChildren().empty()) {
			return SemanticExpressionType(*function.GetChildren()[0], context);
		}
	}
	return expression.GetReturnType();
}

static void ApplySemanticType(LogicalPlanSQLExportField &field, const Expression &expression,
                              const BoundExpressionSQLExportContext &context) {
	auto semantic_type = SemanticExpressionType(expression, context);
	if (semantic_type == field.type) {
		return;
	}
	field.optimizer_type = field.type;
	field.type = std::move(semantic_type);
}

static bool HasSafePredicates(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		return !HasEffectfulExpressions(op) &&
		       (op.children[0]->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN || HasSafePredicates(*op.children[0]));
	}
	if (op.type != LogicalOperatorType::LOGICAL_FILTER && op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	for (auto &expression : op.expressions) {
		if (expression->IsVolatile() || expression->CanThrow()) {
			return false;
		}
	}
	return HasSafePredicates(*op.children[0]);
}

LogicalPlanSQLExportResult
LogicalPlanSQLExportState::ExportContextExpressions(LogicalOperator &op, const LogicalPlanVerificationPath &path) {
	D_ASSERT(op.children.size() == 1);
	auto fields = CreateFields(op, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	if (op.type == LogicalOperatorType::LOGICAL_WINDOW && op.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.children[0]->Cast<LogicalGet>();
		if (get.source_ordinality == OrdinalityType::WITH_ORDINALITY && !get.ordinality_idx.IsValid()) {
			bool supported =
			    op.expressions.size() == 1 && !get.table_filters.HasFilters() && !get.extra_info.sample_options;
			if (supported) {
				auto &window = op.expressions[0]->Cast<BoundWindowExpression>();
				supported = window.GetExpressionType() == ExpressionType::WINDOW_ROW_NUMBER &&
				            window.Partitions().empty() && window.OrderBy().empty() && window.GetChildren().empty();
			}
			if (!supported) {
				return PlanFailure(PlanUnsupportedFeature(
				    path, "ordinality_window", "The source ordinality cannot be reconstructed through this window"));
			}
			return ExportGet(get, PlanChildPath(path, 0), fields.GetValue().back());
		}
	}
	auto child = ExportChild(*op.children[0], PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child.GetIssues());
	}
	PropagateSemanticTypes(fields.GetValue(), {child.GetValue()});
	auto select = make_uniq<SelectNode>();
	for (idx_t i = 0; i < child.GetValue().relation.fields.size(); i++) {
		select->select_list.push_back(ChildColumn(child.GetValue(), i));
	}
	auto expression_context = CreateBindingContext(context, {child.GetValue()});
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		auto expression_path = PlanExpressionPath(path, i);
		auto expression =
		    op.type == LogicalOperatorType::LOGICAL_WINDOW
		        ? BoundExpressionSQLExporter::ExportWindowAtPath(op.expressions[i]->Cast<BoundWindowExpression>(),
		                                                         expression_context, expression_path)
		        : BoundExpressionSQLExporter::ExportUnnestAtPath(op.expressions[i]->Cast<BoundUnnestExpression>(),
		                                                         expression_context, expression_path);
		if (expression.HasError()) {
			return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
		}
		expression.GetValue()->SetAlias(FieldIdentifier(select->select_list.size()));
		select->select_list.push_back(std::move(expression.GetValue()));
	}
	select->from_table = CreateSubquery(std::move(child.GetValue()));
	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportFilter(LogicalFilter &filter,
                                                                   const LogicalPlanVerificationPath &path) {
	D_ASSERT(filter.children.size() == 1);
	for (auto &expression : filter.expressions) {
		D_ASSERT(expression && expression->GetReturnType() == LogicalType::BOOLEAN);
	}
	auto fields = CreateFields(filter, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	auto child = ExportChild(*filter.children[0], PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child.GetIssues());
	}
	PropagateSemanticTypes(fields.GetValue(), {child.GetValue()});
	auto plain = PlainScope(*child.GetValue().relation.query);
	if (plain && plain->where_clause && !HasSafePredicates(*filter.children[0])) {
		plain = nullptr;
	}
	if (plain && plain->where_clause) {
		for (auto &predicate : filter.expressions) {
			if (predicate->IsVolatile() || predicate->CanThrow()) {
				plain = nullptr;
				break;
			}
		}
	}
	vector<reference<const LogicalPlanSQLExportedChild>> child_references {child.GetValue()};
	auto expression_context = CreateBindingContext(context, child_references, {plain});
	auto expressions = CollectExpressions(filter);
	vector<unique_ptr<ParsedExpression>> predicates;
	for (idx_t expression_index = 0; expression_index < filter.expressions.size(); expression_index++) {
		auto predicate = ExportExpression(filter, expressions, expression_index, expression_context, path);
		if (predicate.HasError()) {
			return LogicalPlanSQLExportResult::Failure(predicate.GetIssues());
		}
		predicates.push_back(std::move(predicate.GetValue()));
	}
	auto select = make_uniq<SelectNode>();
	for (idx_t field_index = 0; field_index < fields.GetValue().size(); field_index++) {
		auto child_field_index =
		    filter.projection_map.empty() ? field_index : filter.projection_map[field_index].GetIndexUnsafe();
		auto expression = ChildColumn(child.GetValue(), child_field_index, plain);
		expression->SetAlias(FieldIdentifier(field_index));
		select->select_list.push_back(std::move(expression));
	}
	if (predicates.size() == 1) {
		select->where_clause = std::move(predicates[0]);
	} else if (!predicates.empty()) {
		select->where_clause = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(predicates));
	}
	SetChildScope(*select, std::move(child.GetValue()), plain);
	LogicalPlanSQLExportRelation relation {std::move(select), std::move(fields.GetValue())};
	return LogicalPlanSQLExportResult::Success(std::move(relation));
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportProjection(LogicalProjection &projection,
                                                                       const LogicalPlanVerificationPath &path) {
	D_ASSERT(projection.children.size() == 1 && !projection.expressions.empty());
	auto fields = CreateFields(projection, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	D_ASSERT(projection.expressions.size() == fields.GetValue().size());
	bool fromless = projection.children[0]->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN;
	auto child = ExportChild(*projection.children[0], PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child.GetIssues());
	}
	if (IsIdentityProjection(projection, child.GetValue().relation.fields)) {
		return LogicalPlanSQLExportResult::Success(
		    {std::move(child.GetValue().relation.query), std::move(fields.GetValue())});
	}
	auto plain = PlainScope(*child.GetValue().relation.query);
	vector<reference<const LogicalPlanSQLExportedChild>> child_references {child.GetValue()};
	auto expression_context = CreateBindingContext(context, child_references, {plain});
	auto expressions = CollectExpressions(projection);
	auto select = make_uniq<SelectNode>();
	for (idx_t expression_index = 0; expression_index < projection.expressions.size(); expression_index++) {
		fromless = fromless && projection.expressions[expression_index]->IsScalar();
		ApplySemanticType(fields.GetValue()[expression_index], *projection.expressions[expression_index],
		                  expression_context);
		auto expression = ExportExpression(projection, expressions, expression_index, expression_context, path);
		if (expression.HasError()) {
			return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
		}
		expression.GetValue()->SetAlias(FieldIdentifier(expression_index));
		select->select_list.push_back(std::move(expression.GetValue()));
	}
	if (fromless) {
		select->from_table = make_uniq<EmptyTableRef>();
	} else {
		SetChildScope(*select, std::move(child.GetValue()), plain);
	}
	LogicalPlanSQLExportRelation relation {std::move(select), std::move(fields.GetValue())};
	return LogicalPlanSQLExportResult::Success(std::move(relation));
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportModifier(LogicalOperator &op,
                                                                     const LogicalPlanVerificationPath &path) {
	D_ASSERT(op.children.size() == 1);
	auto fields = CreateFields(op, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	auto child = ExportChild(*op.children[0], PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child.GetIssues());
	}
	PropagateSemanticTypes(fields.GetValue(), {child.GetValue()});
	auto expression_context = CreateBindingContext(context, {child.GetValue()});
	auto expressions = CollectExpressions(op);
	auto select = ForwardFields(child.GetValue(), fields.GetValue());
	idx_t expression_ordinal = 0;
	optional_ptr<const vector<BoundOrderByNode>> orders;
	if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
		auto &distinct = op.Cast<LogicalDistinct>();
		auto modifier = make_uniq<DistinctModifier>();
		// DISTINCT targets describe the grouping keys, including optimizer-pruned full-row DISTINCT.
		for (auto &target : distinct.distinct_targets) {
			(void)target;
			auto expression = ExportExpression(op, expressions, expression_ordinal++, expression_context, path);
			if (expression.HasError()) {
				return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
			}
			modifier->distinct_on_targets.push_back(std::move(expression.GetValue()));
		}
		select->modifiers.push_back(std::move(modifier));
		if (distinct.order_by) {
			orders = distinct.order_by->orders;
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		orders = op.Cast<LogicalOrder>().orders;
	} else {
		orders = op.Cast<LogicalTopN>().orders;
	}
	if (orders) {
		auto modifier = make_uniq<OrderModifier>();
		for (auto &order : *orders) {
			auto expression = ExportExpression(op, expressions, expression_ordinal++, expression_context, path);
			if (expression.HasError()) {
				return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
			}
			modifier->orders.emplace_back(order.type, order.null_order, std::move(expression.GetValue()));
		}
		select->modifiers.push_back(std::move(modifier));
	}
	if (op.type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &top_n = op.Cast<LogicalTopN>();
		auto modifier = make_uniq<LimitModifier>();
		modifier->limit = ConstantExpression::FromValue(Value::BIGINT(NumericCast<int64_t>(top_n.limit)));
		auto offset = top_n.unpruned_offset.IsValid() ? top_n.unpruned_offset.GetIndex() : top_n.offset;
		modifier->offset = ConstantExpression::FromValue(Value::BIGINT(NumericCast<int64_t>(offset)));
		select->modifiers.push_back(std::move(modifier));
	}
	select->from_table = CreateSubquery(std::move(child.GetValue()));
	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportSample(LogicalSample &sample,
                                                                   const LogicalPlanVerificationPath &path) {
	D_ASSERT(sample.children.size() == 1 && sample.sample_options);
	auto &sampling = *sample.sample_options;
	if (sampling.seed.IsValid() != sampling.repeatable) {
		return PlanFailure(
		    PlanUnsupportedFeature(path, "sample_repeatability", "SQL sampling seeds imply repeatable sampling"));
	}
	if (sampling.seed.IsValid() && sampling.seed.GetIndex() > idx_t(NumericLimits<int64_t>::Maximum())) {
		return PlanFailure(PlanUnsupportedFeature(path, "sample_seed", "The sampling seed has no SQL spelling"));
	}
	auto fields = CreateFields(sample, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	auto child = ExportChild(*sample.children[0], PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child.GetIssues());
	}
	auto select = ForwardFields(child.GetValue(), fields.GetValue());
	select->sample = sampling.Copy();
	select->from_table = CreateSubquery(std::move(child.GetValue()));
	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportSetOperation(LogicalSetOperation &op,
                                                                         const LogicalPlanVerificationPath &path) {
	D_ASSERT(!op.children.empty());
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_UNION || op.children.size() == 2);
	auto fields = CreateFields(op, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	auto query = make_uniq<SetOperationNode>();
	query->setop_all = op.setop_all;
	query->setop_type = op.type == LogicalOperatorType::LOGICAL_UNION    ? SetOperationType::UNION
	                    : op.type == LogicalOperatorType::LOGICAL_EXCEPT ? SetOperationType::EXCEPT
	                                                                     : SetOperationType::INTERSECT;
	for (idx_t i = 0; i < op.children.size(); i++) {
		auto child = Export(*op.children[i], PlanChildPath(path, i));
		if (child.HasError()) {
			return LogicalPlanSQLExportResult::Failure(child.GetIssues());
		}
		D_ASSERT(child.GetValue().fields.size() == fields.GetValue().size());
		query->children.push_back(std::move(child.GetValue().query));
	}
	if (op.children.size() == 1) {
		// Retain the UNION boundary when optimization removed every other arm.
		LogicalEmptyResult empty(op.types, op.GetColumnBindings());
		empty.ResolveOperatorTypes();
		auto exported = ExportConstantSource(empty, path);
		if (exported.HasError()) {
			return exported;
		}
		query->children.push_back(std::move(exported.GetValue().query));
	}
	return LogicalPlanSQLExportResult::Success({std::move(query), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportAggregate(LogicalAggregate &aggregate,
                                                                      const LogicalPlanVerificationPath &path) {
	D_ASSERT(aggregate.children.size() == 1);
	for (auto &expression : aggregate.expressions) {
		D_ASSERT(expression && expression->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
	}
	auto fields = CreateFields(aggregate, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	D_ASSERT(aggregate.groups.size() + aggregate.expressions.size() + aggregate.grouping_functions.size() ==
	         fields.GetValue().size());
	auto child = ExportChild(*aggregate.children[0], PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child.GetIssues());
	}
	auto plain = PlainScope(*child.GetValue().relation.query);
	vector<reference<const LogicalPlanSQLExportedChild>> child_references {child.GetValue()};
	auto expression_context = CreateBindingContext(context, child_references, {plain});
	auto expressions = CollectExpressions(aggregate);
	for (idx_t group_index = 0; group_index < aggregate.groups.size(); group_index++) {
		ApplySemanticType(fields.GetValue()[group_index], *aggregate.groups[group_index], expression_context);
	}
	auto input_field_count = child.GetValue().relation.fields.size();
	if (!aggregate.groups.empty()) {
		// Equal group expressions can still occupy distinct grouping-set positions.
		auto input = make_uniq<SelectNode>();
		auto input_fields = child.GetValue().relation.fields;
		for (idx_t i = 0; i < input_field_count; i++) {
			auto expression = ChildColumn(child.GetValue(), i, plain);
			expression->SetAlias(FieldIdentifier(i));
			input->select_list.push_back(std::move(expression));
		}
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			auto expression = ExportExpression(aggregate, expressions, i, expression_context, path);
			if (expression.HasError()) {
				return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
			}
			expression.GetValue()->SetAlias(FieldIdentifier(input_fields.size()));
			input->select_list.push_back(std::move(expression.GetValue()));
			input_fields.push_back(fields.GetValue()[i]);
		}
		SetChildScope(*input, std::move(child.GetValue()), plain);
		child.GetValue() = {{std::move(input), std::move(input_fields)}, NextRelationAlias()};
		plain = nullptr;
		expression_context = CreateBindingContext(context, {child.GetValue()});
	}
	auto select = make_uniq<SelectNode>();
	for (idx_t expression_index = 0; expression_index < expressions.size(); expression_index++) {
		unique_ptr<ParsedExpression> result;
		if (expression_index < aggregate.groups.size()) {
			result = ChildColumn(child.GetValue(), input_field_count + expression_index);
			select->groups.group_expressions.push_back(result->Copy());
		} else {
			auto expression = ExportExpression(aggregate, expressions, expression_index, expression_context, path);
			if (expression.HasError()) {
				return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
			}
			result = std::move(expression.GetValue());
		}
		result->SetAlias(FieldIdentifier(expression_index));
		select->select_list.push_back(std::move(result));
	}
	select->groups.grouping_sets = aggregate.grouping_sets;
	if (select->groups.grouping_sets.empty() && !aggregate.groups.empty()) {
		GroupingSet all_groups;
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			all_groups.insert(ProjectionIndex(i));
		}
		select->groups.grouping_sets.push_back(std::move(all_groups));
	}
	for (auto &grouping : aggregate.grouping_functions) {
		vector<unique_ptr<ParsedExpression>> arguments;
		for (auto index : grouping) {
			arguments.push_back(select->groups.group_expressions[index]->Copy());
		}
		auto expression = make_uniq<OperatorExpression>(ExpressionType::GROUPING_FUNCTION, std::move(arguments));
		expression->SetAlias(FieldIdentifier(select->select_list.size()));
		select->select_list.push_back(std::move(expression));
	}
	SetChildScope(*select, std::move(child.GetValue()), plain);
	LogicalPlanSQLExportRelation relation {std::move(select), std::move(fields.GetValue())};
	return LogicalPlanSQLExportResult::Success(std::move(relation));
}

} // namespace logical_plan_sql_export
} // namespace duckdb
