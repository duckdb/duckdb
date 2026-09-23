#include "duckdb/planner/sql_export/logical_plan_sql_exporter_internal.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {
using namespace logical_plan_sql_export;

LogicalPlanSQLExportResult LogicalMaterializedCTE::ToSQL(LogicalPlanSQLExportContext &export_context,
                                                         const LogicalPlanVerificationPath &path) {
	auto &cte = *this;
	D_ASSERT(cte.children.size() == 2);
	auto fields = CreateFields(cte, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	auto name = export_context.NextRelationAlias(cte.ctename);
	auto producer = export_context.ExportNamedProducer(*cte.children[0], PlanChildPath(path, 0), name);
	if (producer.HasError()) {
		return LogicalPlanSQLExportResult::Failure(producer.GetIssues());
	}
	export_context.named_relations.push_back({cte.table_index, name, false, 0});
	auto consumer = export_context.ExportChild(*cte.children[1], PlanChildPath(path, 1));
	auto references = export_context.named_relations.back().references;
	export_context.named_relations.pop_back();
	if (consumer.HasError()) {
		return LogicalPlanSQLExportResult::Failure(consumer.GetIssues());
	}
	if (references == 0) {
		return PlanFailure(PlanUnsupportedFeature(path, "cte_unreferenced_evaluation",
		                                          "SQL would discard the unreferenced CTE producer"));
	}
	auto info = make_uniq<CommonTableExpressionInfo>();
	for (idx_t i = 0; i < producer.GetValue().relation.fields.size(); i++) {
		info->aliases.push_back(FieldIdentifier(i));
	}
	if (producer.GetValue().relation.query->type == QueryNodeType::RECURSIVE_CTE_NODE) {
		for (auto &key : producer.GetValue().relation.query->Cast<RecursiveCTENode>().key_targets) {
			info->key_targets.push_back(key->Copy());
		}
	}
	info->query_node = std::move(producer.GetValue().relation.query);
	info->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
	auto select = export_context.ForwardFields(consumer.GetValue(), fields.GetValue());
	select->from_table = CreateSubquery(std::move(consumer.GetValue()));
	select->cte_map.map.insert(name, std::move(info));
	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalCTERef::ToSQL(LogicalPlanSQLExportContext &export_context,
                                                const LogicalPlanVerificationPath &path) {
	auto &ref = *this;
	D_ASSERT(ref.children.empty());
	optional<Identifier> name;
	for (idx_t i = export_context.named_relations.size(); i > 0; i--) {
		auto &relation = export_context.named_relations[i - 1];
		if (relation.index == ref.cte_index && relation.is_recurring == ref.is_recurring) {
			relation.references++;
			name = relation.name;
			break;
		}
	}
	if (!name) {
		return PlanFailure(PlanUnsupportedFeature(path, "cte_reference_scope",
		                                          "The referenced CTE is not in the exported relation scope"));
	}
	auto fields = CreateFields(ref, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	return LogicalPlanSQLExportResult::Success(
	    {export_context.CreateNamedSource(*name, fields.GetValue(), ref.is_recurring), std::move(fields.GetValue())});
}

unique_ptr<SelectNode> logical_plan_sql_export::LogicalPlanSQLExportContext::CreateNamedSource(
    const Identifier &name, const vector<LogicalPlanSQLExportField> &fields, bool recurring) {
	auto table = make_uniq<BaseTableRef>();
	table->SetTable(name);
	if (recurring) {
		table->SetQualifiedName(Identifier(), Identifier("recurring"), name);
	}
	table->alias = NextRelationAlias();
	auto select = make_uniq<SelectNode>();
	for (idx_t i = 0; i < fields.size(); i++) {
		table->column_name_alias.push_back(FieldIdentifier(i));
		select->select_list.push_back(make_uniq<ColumnRefExpression>(FieldIdentifier(i), table->alias));
	}
	select->from_table = std::move(table);
	return select;
}

LogicalPlanSQLExportResult LogicalRecursiveCTE::ExportSQLDefinition(LogicalPlanSQLExportContext &export_context,
                                                                    const LogicalPlanVerificationPath &path,
                                                                    const Identifier &name) {
	auto &cte = *this;
	D_ASSERT(cte.children.size() == 2);
	auto fields = CreateFields(cte, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	auto seed = export_context.Export(*cte.children[0], PlanChildPath(path, 0));
	if (seed.HasError()) {
		return LogicalPlanSQLExportResult::Failure(seed.GetIssues());
	}
	export_context.named_relations.push_back({cte.table_index, name, false, 0});
	export_context.named_relations.push_back({cte.table_index, name, true, 0});
	auto step = export_context.Export(*cte.children[1], PlanChildPath(path, 1));
	auto recurring_references = export_context.named_relations.back().references;
	export_context.named_relations.pop_back();
	auto references = export_context.named_relations.back().references;
	export_context.named_relations.pop_back();
	if (step.HasError()) {
		return LogicalPlanSQLExportResult::Failure(step.GetIssues());
	}
	if ((references == 0 && recurring_references == 0) || (cte.ref_recurring && recurring_references == 0)) {
		// Binding still needs a self reference when optimization removed the recursive scan.
		auto empty = export_context.CreateNamedSource(name, fields.GetValue(), cte.ref_recurring);
		empty->select_list.clear();
		for (auto &type : cte.internal_types) {
			auto value = ExportTypedNull(type, path);
			if (value.HasError()) {
				return LogicalPlanSQLExportResult::Failure(value.GetIssues());
			}
			empty->select_list.push_back(std::move(value.GetValue()));
		}
		empty->where_clause = ConstantExpression::FromValue(Value::BOOLEAN(false));
		auto recursive_step = make_uniq<SetOperationNode>();
		recursive_step->setop_type = SetOperationType::UNION;
		recursive_step->setop_all = true;
		recursive_step->children.push_back(std::move(step.GetValue().query));
		recursive_step->children.push_back(std::move(empty));
		step.GetValue().query = std::move(recursive_step);
	}
	auto query = make_uniq<RecursiveCTENode>();
	query->ctename = name;
	query->union_all = cte.union_all;
	for (idx_t i = 0; i < fields.GetValue().size(); i++) {
		query->aliases.push_back(FieldIdentifier(i));
	}
	BoundExpressionSQLExportContext key_context;
	key_context.client_context = &export_context.context;
	key_context.resolve_binding = [&](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		if (binding.table_index != cte.table_index || binding.column_index.GetIndex() >= cte.internal_types.size()) {
			return {};
		}
		return ResolvedSQLColumnReference {{FieldIdentifier(binding.column_index.GetIndex())},
		                                   cte.internal_types[binding.column_index.GetIndex()]};
	};
	auto expressions = CollectExpressions(cte);
	idx_t expression_ordinal = 0;
	unordered_set<ProjectionIndex> key_columns;
	for (auto &key : cte.key_targets) {
		auto exported = export_context.ExportExpression(cte, expressions, expression_ordinal++, key_context, path);
		if (exported.HasError()) {
			return LogicalPlanSQLExportResult::Failure(exported.GetIssues());
		}
		key_columns.insert(key->Cast<BoundColumnRefExpression>().Binding().column_index);
		query->key_targets.push_back(std::move(exported.GetValue()));
	}
	if (!cte.key_targets.empty()) {
		for (idx_t column = 0; column < cte.internal_types.size(); column++) {
			if (key_columns.count(ProjectionIndex(column))) {
				continue;
			}
			auto ordinal = expression_ordinal++;
			auto &aggregate = expressions[ordinal].get().Cast<BoundAggregateExpression>();
			const bool has_order = aggregate.GetOrderBys() && !aggregate.GetOrderBys()->orders.empty();
			const bool has_modifiers = aggregate.IsDistinct() || aggregate.GetFilter() || has_order;
			if (has_modifiers || aggregate.StateExportMode() != AggregateStateExportMode::NONE) {
				return PlanFailure(
				    PlanUnsupportedFeature(PlanExpressionPath(path, ordinal), "recursive_payload_modifiers",
				                           "The recursive payload clause cannot preserve these aggregate modifiers"));
			}
			auto exported = BoundExpressionSQLExporter::ExportAggregateCallAtPath(aggregate, key_context,
			                                                                      PlanExpressionPath(path, ordinal));
			if (exported.HasError()) {
				return LogicalPlanSQLExportResult::Failure(exported.GetIssues());
			}
			exported.GetValue()->SetAlias(FieldIdentifier(column));
			query->key_targets.push_back(std::move(exported.GetValue()));
		}
	}
	query->left = std::move(seed.GetValue().query);
	query->right = std::move(step.GetValue().query);
	return LogicalPlanSQLExportResult::Success({std::move(query), std::move(fields.GetValue())});
}

LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>
logical_plan_sql_export::LogicalPlanSQLExportContext::ExportNamedProducer(LogicalOperator &op,
                                                                          const LogicalPlanVerificationPath &path,
                                                                          const Identifier &name) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		D_ASSERT(op.children.size() == 1);
		auto fields = CreateFields(op, path);
		auto child_path = PlanChildPath(path, 0);
		auto child_fields = CreateFields(*op.children[0], child_path);
		if (fields.IsSuccess() && child_fields.IsSuccess() &&
		    IsIdentityProjection(op.Cast<LogicalProjection>(), child_fields.GetValue())) {
			ancestors.push_back(op);
			auto exported = ExportNamedProducer(*op.children[0], child_path, name);
			ancestors.pop_back();
			if (exported.IsSuccess()) {
				exported.GetValue().relation.fields = std::move(fields.GetValue());
			}
			return exported;
		}
	}
	if (op.type != LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return ExportChild(op, path);
	}
	ancestors.push_back(op);
	auto exported = op.Cast<LogicalRecursiveCTE>().ExportSQLDefinition(*this, path, name);
	ancestors.pop_back();
	if (exported.HasError()) {
		return LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>::Failure(exported.GetIssues());
	}
	return LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>::Success(
	    {std::move(exported.GetValue()), NextRelationAlias()});
}

LogicalPlanSQLExportResult LogicalRecursiveCTE::ToSQL(LogicalPlanSQLExportContext &export_context,
                                                      const LogicalPlanVerificationPath &path) {
	auto &cte = *this;
	if (Optimizer::OptimizerDisabled(export_context.context, OptimizerType::CTE_INLINING) ||
	    Settings::Get<DebugDisableOptimizerSetting>(export_context.context)) {
		return PlanFailure(
		    PlanUnsupportedFeature(path, "recursive_cte_materialization",
		                           "The SQL wrapper requires CTE inlining to preserve recursive evaluation"));
	}
	auto name = export_context.NextRelationAlias(cte.ctename);
	auto recursive = ExportSQLDefinition(export_context, path, name);
	if (recursive.HasError()) {
		return recursive;
	}
	auto info = make_uniq<CommonTableExpressionInfo>();
	info->aliases = recursive.GetValue().query->Cast<RecursiveCTENode>().aliases;
	for (auto &key : recursive.GetValue().query->Cast<RecursiveCTENode>().key_targets) {
		info->key_targets.push_back(key->Copy());
	}
	info->query_node = std::move(recursive.GetValue().query);
	info->materialized = CTEMaterialize::CTE_MATERIALIZE_NEVER;
	auto select = export_context.CreateNamedSource(name, recursive.GetValue().fields);
	select->cte_map.map.insert(name, std::move(info));
	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(recursive.GetValue().fields)});
}

} // namespace duckdb
