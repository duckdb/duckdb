#include "duckdb/planner/sql_export/logical_plan_sql_exporter_internal.hpp"
#include "duckdb/function/scalar/compressed_materialization_utils.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_plan_verifier.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"

namespace duckdb {
namespace logical_plan_sql_export {

static LogicalPlanVerificationIssue UnsupportedOperator(const LogicalPlanVerificationPath &path,
                                                        LogicalOperatorType type) {
	return SQLExportHelpers::MakeIssue(LogicalPlanVerificationIssueCode::UNSUPPORTED_OPERATOR,
	                                   LogicalPlanVerificationPhase::PLAN_EXPORT, path,
	                                   LogicalPlanVerificationConstructIdentity::LogicalOperator(type),
	                                   "The logical operator does not have a SQL AST representation in this exporter");
}

static optional<Value> ConstantSQLInput(const Expression &expression, LogicalOperator &input) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return expression.Cast<BoundConstantExpression>().GetValue();
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expression.Cast<BoundFunctionExpression>();
		if (!function.GetChildren().empty() && CMUtils::GetExpressionType(function) != CMExpressionType::NONE) {
			return ConstantSQLInput(*function.GetChildren()[0], input);
		}
	}
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF || input.children.size() != 1) {
		return {};
	}
	auto &column = expression.Cast<BoundColumnRefExpression>();
	if (column.Depth() != 0) {
		return {};
	}
	switch (input.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = input.Cast<LogicalProjection>();
		if (column.Binding().table_index != projection.table_index) {
			return {};
		}
		return ConstantSQLInput(*projection.expressions[column.Binding().column_index], *input.children[0]);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = input.Cast<LogicalAggregate>();
		auto index = column.Binding().column_index;
		if (column.Binding().table_index != aggregate.group_index) {
			return {};
		}
		for (auto &grouping_set : aggregate.grouping_sets) {
			if (!grouping_set.count(index)) {
				return {};
			}
		}
		return ConstantSQLInput(*aggregate.groups[index], *input.children[0]);
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return ConstantSQLInput(expression, *input.children[0]);
	default:
		return {};
	}
}

static LogicalPlanSQLExportResult ApplyOutputNames(LogicalPlanSQLExportResult result,
                                                   const vector<Identifier> &output_names) {
	if (result.HasError()) {
		return result;
	}
	if (output_names.size() != result.GetValue().fields.size()) {
		return LogicalPlanSQLExportResult::Failure({PlanUnsupportedFeature(
		    LogicalPlanVerificationPath(), "output_names", "Output name count does not match the exported plan")});
	}
	if (result.GetValue().query->type == QueryNodeType::SELECT_NODE) {
		auto &select = result.GetValue().query->Cast<SelectNode>();
		D_ASSERT(select.select_list.size() == output_names.size());
		for (idx_t i = 0; i < output_names.size(); i++) {
			select.select_list[i]->SetAlias(output_names[i]);
		}
		return result;
	}
	auto fields = result.GetValue().fields;
	LogicalPlanSQLExportedChild child {std::move(result.GetValue()), Identifier("exported_query")};
	auto select = make_uniq<SelectNode>();
	for (idx_t i = 0; i < output_names.size(); i++) {
		auto expression = make_uniq<ColumnRefExpression>(FieldIdentifier(i), child.relation_alias);
		expression->SetAlias(output_names[i]);
		select->select_list.push_back(std::move(expression));
	}
	select->from_table = CreateSubquery(std::move(child));
	return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields)});
}

struct LogicalPlanSQLExportContext::SourceScope {
	SourceScope(LogicalPlanSQLExportContext &context_p, const vector<LogicalPlanSQLExportSource> &sources_p)
	    : context(context_p), sources(sources_p), parent(context.source_scope) {
		context.source_scope = this;
	}
	~SourceScope() {
		context.source_scope = parent;
	}

	LogicalPlanSQLExportContext &context;
	const vector<LogicalPlanSQLExportSource> &sources;
	optional_ptr<const SourceScope> parent;
};

LogicalPlanSQLExportContext::LogicalPlanSQLExportContext(ClientContext &context_p) : context(context_p) {
}

LogicalPlanSQLExportResult LogicalPlanSQLExportContext::Export(LogicalOperator &op,
                                                               const LogicalPlanVerificationPath &path) {
	for (auto scope = source_scope; scope; scope = scope->parent) {
		for (auto &source : scope->sources) {
			if (&source.op.get() == &op) {
				return LogicalPlanSQLExportResult::Success(
				    {CreateNamedSource(source.name, source.relation.fields), source.relation.fields});
			}
		}
	}
	ancestors.push_back(op);
	auto result = op.ToSQL(*this, path);
	ancestors.pop_back();
	return result;
}

Identifier LogicalPlanSQLExportContext::NextRelationAlias(const Identifier &preferred) {
	if (!preferred.empty() && relation_aliases.insert(preferred).second) {
		return preferred;
	}
	while (true) {
		auto name = Identifier("r" + to_string(next_relation_ordinal++));
		if (relation_aliases.insert(name).second) {
			return name;
		}
	}
}

LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>
LogicalPlanSQLExportContext::ExportChild(LogicalOperator &child, const LogicalPlanVerificationPath &path) {
	auto exported = Export(child, path);
	if (exported.HasError()) {
		return LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>::Failure(exported.GetIssues());
	}
	LogicalPlanSQLExportedChild result {std::move(exported.GetValue()), NextRelationAlias()};
	return LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>::Success(std::move(result));
}

LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>
LogicalPlanSQLExportContext::ExportChild(LogicalOperator &child, const LogicalPlanVerificationPath &path,
                                         const vector<LogicalPlanSQLExportSource> &sources) {
	SourceScope scope(*this, sources);
	return ExportChild(child, path);
}

LogicalPlanVerificationResult<unique_ptr<ParsedExpression>> LogicalPlanSQLExportContext::ExportExpression(
    const LogicalOperator &op, const vector<reference<const Expression>> &expressions, idx_t expression_ordinal,
    const BoundExpressionSQLExportContext &expression_context, const LogicalPlanVerificationPath &path) {
	D_ASSERT(expression_ordinal < expressions.size());
	auto &expression = expressions[expression_ordinal].get();
	unique_ptr<Expression> restored;
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE && op.children.size() == 1) {
		auto &arguments = expression.Cast<BoundAggregateExpression>().GetChildren();
		for (idx_t i = 0; i < arguments.size(); i++) {
			if (arguments[i]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				continue;
			}
			auto value = ConstantSQLInput(*arguments[i], *op.children[0]);
			if (value && value->type().EqualsIncludingCollation(arguments[i]->GetReturnType())) {
				if (!restored) {
					restored = expression.Copy();
				}
				restored->Cast<BoundAggregateExpression>().GetChildrenMutable()[i] =
				    make_uniq<BoundConstantExpression>(*value);
			}
		}
	}
	return BoundExpressionSQLExporter::ExportAtPath(restored ? *restored : expression, expression_context,
	                                                PlanExpressionPath(path, expression_ordinal));
}

unique_ptr<SelectNode> LogicalPlanSQLExportContext::ForwardFields(const LogicalPlanSQLExportedChild &child,
                                                                  const vector<LogicalPlanSQLExportField> &fields,
                                                                  optional_ptr<const SelectNode> plain) {
	auto select = make_uniq<SelectNode>();
	for (auto &field : fields) {
		bool found = false;
		for (idx_t i = 0; i < child.relation.fields.size(); i++) {
			if (field.source_binding != child.relation.fields[i].source_binding) {
				continue;
			}
			auto expression = plain ? plain->select_list[i]->Copy() : ChildColumn(child, i);
			expression->SetAlias(FieldIdentifier(select->select_list.size()));
			select->select_list.push_back(std::move(expression));
			found = true;
			break;
		}
		D_ASSERT(found);
		(void)found;
	}
	return select;
}

} // namespace logical_plan_sql_export

LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>
LogicalOperator::ToSQL(LogicalPlanSQLExportContext &, const LogicalPlanVerificationPath &path) {
	using namespace logical_plan_sql_export;
	if (type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return LogicalPlanSQLExportResult::Failure({UnsupportedSource(path, LogicalSourceIdentity(), "delim_get")});
	}
	D_ASSERT(type != LogicalOperatorType::LOGICAL_INVALID);
	return LogicalPlanSQLExportResult::Failure({UnsupportedOperator(path, type)});
}

LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>
LogicalPlanSQLExporter::Export(ClientContext &context, LogicalOperator &root,
                               const LogicalPlanSQLExportOptions &options) {
	auto verification = LogicalPlanVerifier::VerifyAlways(root);
	if (verification.HasError()) {
		return LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>::Failure(verification.GetIssues());
	}
	logical_plan_sql_export::LogicalPlanSQLExportContext state(context);
	auto result = state.Export(root, LogicalPlanVerificationPath());
	if (!options.output_names) {
		return result;
	}
	return logical_plan_sql_export::ApplyOutputNames(std::move(result), *options.output_names);
}

} // namespace duckdb
