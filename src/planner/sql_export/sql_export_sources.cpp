#include "duckdb/planner/sql_export/logical_plan_sql_exporter_internal.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/operator_extension.hpp"

namespace duckdb {
namespace logical_plan_sql_export {

static LogicalPlanVerificationIssue ExtensionIssue(LogicalPlanVerificationIssueCode code,
                                                   const LogicalPlanVerificationPath &path, const string &identifier,
                                                   string message) {
	return SQLExportHelpers::MakeIssue(code, LogicalPlanVerificationPhase::PLAN_EXPORT, path,
	                                   LogicalPlanVerificationConstructIdentity::Extension(identifier),
	                                   std::move(message));
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportGet(LogicalGet &get,
                                                                const LogicalPlanVerificationPath &path,
                                                                optional<LogicalPlanSQLExportField> ordinality) {
	D_ASSERT(get.children.size() <= 1);
	if (get.has_pushed_projection) {
		return PlanFailure(UnsupportedSource(path, LogicalSourceIdentity(get), "pushed_projection"));
	}
	auto fields = CreateFields(get, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	if ((!get.extra_info.file_filters.empty() || get.extra_info.total_files.IsValid()) &&
	    !get.extra_info.file_filter_expressions) {
		return PlanFailure(PlanUnsupportedFeature(
		    path, "file_filter_residual", "The source does not retain the SQL predicate used for file pruning"));
	}
	if (get.row_group_order_options &&
	    (get.row_group_order_options->row_group_offset || get.row_group_order_options->leading_null_group_offset)) {
		bool has_unpruned_offset = false;
		for (idx_t i = ancestors.size(); i > 0; i--) {
			auto &ancestor = ancestors[i - 1].get();
			if (ancestor.type == LogicalOperatorType::LOGICAL_LIMIT) {
				has_unpruned_offset = ancestor.Cast<LogicalLimit>().unpruned_offset.IsValid();
				break;
			} else if (ancestor.type == LogicalOperatorType::LOGICAL_TOP_N) {
				has_unpruned_offset = ancestor.Cast<LogicalTopN>().unpruned_offset.IsValid();
				break;
			}
		}
		if (!has_unpruned_offset) {
			return PlanFailure(PlanUnsupportedFeature(path, "pruned_offset",
			                                          "The row group pruning does not retain its original SQL offset"));
		}
	}

	vector<LogicalPlanSQLExportField> scan_fields;
	for (idx_t i = 0; i < get.GetColumnIds().size(); i++) {
		auto &type = get.GetColumnType(get.GetColumnIds()[i]);
		if (!SQLExportHelpers::IsSQLValueType(type)) {
			return PlanFailure(PlanUnsupportedFeature(path, "scan_type", "Scan type cannot be represented in SQL"));
		}
		scan_fields.push_back({ColumnBinding(get.table_index, ProjectionIndex(i)), type});
	}
	unique_ptr<TableRef> input;
	if (!get.children.empty()) {
		auto child = ExportChild(*get.children[0], PlanChildPath(path, 0));
		if (child.HasError()) {
			return LogicalPlanSQLExportResult::Failure(child.GetIssues());
		}
		for (auto index : get.projected_input) {
			scan_fields.push_back(child.GetValue().relation.fields[index]);
		}
		input = CreateSubquery(std::move(child.GetValue()));
	}
	auto to_sql = get.function.to_sql;
	if (!to_sql) {
		return PlanFailure(UnsupportedSource(path, LogicalSourceIdentity(get), "to_sql_callback"));
	}
	if (ordinality) {
		fields.GetValue().push_back(*ordinality);
		scan_fields.push_back(*ordinality);
	}
	auto relation_alias = NextRelationAlias();
	auto source_sql =
	    to_sql(context, get,
	           {std::move(input), relation_alias, bool(ordinality),
	            get.extra_info.file_filter_expressions ? &*get.extra_info.file_filter_expressions : nullptr});
	if (!source_sql.query) {
		auto guard = source_sql.unsupported_reason.empty() ? "to_sql_callback_declined" : source_sql.unsupported_reason;
		return PlanFailure(UnsupportedSource(path, LogicalSourceIdentity(get), std::move(guard)));
	}
	auto query = std::move(source_sql.query);
	if (get.extra_info.sample_options) {
		auto sampling = get.extra_info.sample_options->Copy();
		if (!sampling->repeatable && sampling->seed.IsValid()) {
			sampling->seed = optional_idx::Invalid();
		}
		if (sampling->repeatable && !sampling->seed.IsValid()) {
			return PlanFailure(
			    PlanUnsupportedFeature(path, "sample_repeatability", "SQL sampling seeds imply repeatable sampling"));
		}
		if (sampling->seed.IsValid() && sampling->seed.GetIndex() > idx_t(NumericLimits<int64_t>::Maximum())) {
			return PlanFailure(PlanUnsupportedFeature(path, "sample_seed", "The sampling seed has no SQL spelling"));
		}
		sampling->sample_rate = -1.0;
		LogicalPlanSQLExportedChild unsampled {{std::move(query), scan_fields}, NextRelationAlias()};
		auto sampled = ForwardFields(unsampled, scan_fields);
		sampled->sample = std::move(sampling);
		sampled->from_table = CreateSubquery(std::move(unsampled));
		query = std::move(sampled);
	}
	LogicalPlanSQLExportedChild source {{std::move(query), std::move(scan_fields)}, std::move(relation_alias)};
	auto plain = PlainScope(*source.relation.query);
	if (plain && (plain->where_clause || plain->select_list.size() != source.relation.fields.size())) {
		plain = nullptr;
	}
	auto binding_context = CreateBindingContext(context, {source}, {plain});
	auto select = ForwardFields(source, fields.GetValue(), plain);
	vector<unique_ptr<Expression>> predicates;
	for (auto &entry : get.table_filters) {
		if (ExpressionFilter::IsOptionalFilter(entry.Filter())) {
			continue;
		}
		auto &field = source.relation.fields[entry.GetIndex().GetIndex()];
		BoundColumnRefExpression column(field.type, field.source_binding);
		predicates.push_back(entry.Filter().ToExpression(column));
	}
	auto conjunction = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	auto ordinal = CollectExpressions(get).size();
	for (auto &predicate : predicates) {
		auto exported =
		    BoundExpressionSQLExporter::ExportAtPath(*predicate, binding_context, PlanExpressionPath(path, ordinal++));
		if (exported.HasError()) {
			return LogicalPlanSQLExportResult::Failure(exported.GetIssues());
		}
		conjunction->AddExpression(std::move(exported.GetValue()));
	}
	if (conjunction->GetChildren().size() == 1) {
		select->where_clause = std::move(conjunction->GetChildrenMutable()[0]);
	} else if (!conjunction->GetChildren().empty()) {
		select->where_clause = std::move(conjunction);
	}
	SetChildScope(*select, std::move(source), plain);
	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportSecureView(LogicalSecureView &view,
                                                                       const LogicalPlanVerificationPath &path) {
	auto fields = CreateFields(view, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}
	if (!view.has_source || view.source_name.Path().empty() || view.source_types.empty()) {
		return PlanFailure(PlanUnsupportedFeature(path, "secure_view_source",
		                                          "The secure view does not retain its qualified source metadata"));
	}
	for (auto &component : view.source_name.Path()) {
		if (component.empty()) {
			return PlanFailure(PlanUnsupportedFeature(path, "secure_view_source",
			                                          "The secure view source name cannot be represented in SQL"));
		}
	}
	if (view.output_bindings.size() != fields.GetValue().size() ||
	    view.output_expressions.size() != fields.GetValue().size()) {
		return PlanFailure(
		    PlanUnsupportedFeature(path, "secure_view_output", "The secure view output mapping is incomplete"));
	}
	for (auto &type : view.source_types) {
		if (!SQLExportHelpers::IsSQLRepresentableType(type)) {
			return PlanFailure(PlanUnsupportedFeature(path, "secure_view_source",
			                                          "A secure view source type cannot be represented in SQL"));
		}
	}

	if (view.source_filters.size() != view.pushed_filters.size()) {
		return PlanFailure(PlanUnsupportedFeature(path, "secure_view_filter",
		                                          "The secure view does not retain every caller predicate"));
	}

	auto source_alias = NextRelationAlias();
	auto table = make_uniq<BaseTableRef>();
	table->SetQualifiedName(view.source_name);
	table->alias = source_alias;
	for (idx_t i = 0; i < view.source_types.size(); i++) {
		table->column_name_alias.push_back(FieldIdentifier(i));
	}
	if (view.has_at_clause) {
		table->at_clause = make_uniq<AtClause>(view.at_unit, ConstantExpression::FromValue(view.at_value));
	}

	BoundExpressionSQLExportContext expression_context;
	expression_context.client_context = &context;
	expression_context.resolve_binding = [source_alias, source_types = view.source_types](
	                                         const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		if (binding.table_index != TableIndex(0) || binding.column_index.GetIndex() >= source_types.size()) {
			return {};
		}
		auto index = binding.column_index.GetIndex();
		return ResolvedSQLColumnReference {{source_alias, FieldIdentifier(index)}, source_types[index]};
	};

	auto select = make_uniq<SelectNode>();
	select->from_table = std::move(table);
	for (idx_t i = 0; i < fields.GetValue().size(); i++) {
		if (view.output_bindings[i] != fields.GetValue()[i].source_binding ||
		    !view.output_expressions[i]->GetReturnType().EqualsIncludingCollation(fields.GetValue()[i].type)) {
			return PlanFailure(PlanUnsupportedFeature(
			    path, "secure_view_output", "The secure view output mapping does not match its current schema"));
		}
		auto expression = BoundExpressionSQLExporter::ExportAtPath(*view.output_expressions[i], expression_context,
		                                                           PlanExpressionPath(path, i));
		if (expression.HasError()) {
			return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
		}
		expression.GetValue()->SetAlias(FieldIdentifier(i));
		select->select_list.push_back(std::move(expression.GetValue()));
	}
	for (idx_t i = 0; i < view.source_filters.size(); i++) {
		if (!view.source_filters[i]) {
			return PlanFailure(PlanUnsupportedFeature(
			    path, "secure_view_filter", "The secure view caller predicate has no complete source mapping"));
		}
		auto predicate = BoundExpressionSQLExporter::ExportAtPath(
		    *view.source_filters[i], expression_context, PlanExpressionPath(path, fields.GetValue().size() + i));
		if (predicate.HasError()) {
			return LogicalPlanSQLExportResult::Failure(predicate.GetIssues());
		}
		if (select->where_clause) {
			select->where_clause = make_uniq<ConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(select->where_clause), std::move(predicate.GetValue()));
		} else {
			select->where_clause = std::move(predicate.GetValue());
		}
	}

	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalPlanSQLExportState::ExportExtension(LogicalExtensionOperator &extension,
                                                                      const LogicalPlanVerificationPath &path) {
	auto extension_identifier = extension.GetExtensionName();
	D_ASSERT(SQLExportHelpers::IsValidIdentifier(Identifier(extension_identifier)));
	auto fields = CreateFields(extension, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
	}

	vector<LogicalPlanSQLExportedChild> exported_children;
	exported_children.reserve(extension.children.size());
	for (idx_t child_index = 0; child_index < extension.children.size(); child_index++) {
		auto child = ExportChild(*extension.children[child_index], PlanChildPath(path, child_index));
		if (child.HasError()) {
			return LogicalPlanSQLExportResult::Failure(child.GetIssues());
		}
		exported_children.push_back(std::move(child.GetValue()));
	}
	vector<reference<const LogicalPlanSQLExportedChild>> child_references;
	for (auto &child : exported_children) {
		child_references.push_back(child);
	}
	auto expression_context = CreateBindingContext(context, child_references);
	vector<LogicalPlanSQLExportChild> child_views;
	for (auto &child : exported_children) {
		child_views.emplace_back(CreateSubquery(std::move(child)));
	}
	auto expressions = CollectExpressions(extension);
	auto expression_exporter = [&](idx_t expression_ordinal) {
		return ExportExpression(extension, expressions, expression_ordinal, expression_context, path);
	};
	LogicalPlanSQLExportExtensionInput input(extension, child_views, expressions.size(), std::move(expression_exporter),
	                                         expression_context);

	if (options.extension_resolver) {
		auto result = options.extension_resolver(input);
		auto handled = HandleExtensionResult(path, extension_identifier, std::move(result), fields.GetValue());
		if (handled) {
			return std::move(*handled);
		}
#ifdef D_ASSERT_IS_ENABLED
		for (auto &child : child_views) {
			D_ASSERT(child.table);
		}
#endif
	}
	for (auto &registered_extension : OperatorExtension::Iterate(context)) {
		if (registered_extension->GetName() != extension_identifier) {
			continue;
		}
		auto result = registered_extension->ExportLogicalPlanSQL(input);
		auto handled = HandleExtensionResult(path, extension_identifier, std::move(result), fields.GetValue());
		if (handled) {
			return std::move(*handled);
		}
#ifdef D_ASSERT_IS_ENABLED
		for (auto &child : child_views) {
			D_ASSERT(child.table);
		}
#endif
		break;
	}
	return PlanFailure(ExtensionIssue(LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION, path,
	                                  extension_identifier, "No SQL export handler accepted the extension operator"));
}

optional<LogicalPlanSQLExportResult> LogicalPlanSQLExportState::HandleExtensionResult(
    const LogicalPlanVerificationPath &path, const string &extension_identifier,
    LogicalPlanSQLExportExtensionResult result, const vector<LogicalPlanSQLExportField> &fields) {
	switch (result.type) {
	case LogicalPlanSQLExportExtensionResultType::NOT_HANDLED:
		D_ASSERT(!result.query && result.reason.empty());
		return {};
	case LogicalPlanSQLExportExtensionResultType::EXPORTED:
		D_ASSERT(result.query && result.reason.empty());
		return LogicalPlanSQLExportResult::Success({std::move(result.query), fields});
	case LogicalPlanSQLExportExtensionResultType::UNSUPPORTED:
		D_ASSERT(!result.query && !result.reason.empty());
		D_ASSERT(SQLExportHelpers::IsValidIdentifier(Identifier(result.reason)));
		return PlanFailure(ExtensionIssue(LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION, path,
		                                  extension_identifier, std::move(result.reason)));
	default:
		D_ASSERT(false);
		return {};
	}
}

} // namespace logical_plan_sql_export
} // namespace duckdb
