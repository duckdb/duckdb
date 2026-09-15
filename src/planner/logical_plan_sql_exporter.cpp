#include "duckdb/function/scalar/compressed_materialization_utils.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_repeatability.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/logical_plan_verifier.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/operator_extension.hpp"

#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

namespace duckdb {

LogicalPlanSQLExportExtensionResult LogicalPlanSQLExportExtensionResult::NotHandled() {
	return LogicalPlanSQLExportExtensionResult();
}

LogicalPlanSQLExportExtensionResult LogicalPlanSQLExportExtensionResult::Exported(unique_ptr<QueryNode> query_p) {
	LogicalPlanSQLExportExtensionResult result;
	result.type = LogicalPlanSQLExportExtensionResultType::EXPORTED;
	result.query = std::move(query_p);
	return result;
}

LogicalPlanSQLExportExtensionResult LogicalPlanSQLExportExtensionResult::Unsupported(string reason_p) {
	LogicalPlanSQLExportExtensionResult result;
	result.type = LogicalPlanSQLExportExtensionResultType::UNSUPPORTED;
	result.reason = std::move(reason_p);
	return result;
}

namespace logical_plan_sql_export {

using LogicalPlanSQLExportResult = LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>;
using LogicalPlanSQLFieldResult = LogicalPlanVerificationResult<vector<LogicalPlanSQLExportField>>;

static LogicalPlanVerificationPath PlanChildPath(const LogicalPlanVerificationPath &path, idx_t ordinal) {
	return SQLExportHelpers::ChildPath(path, ordinal, LogicalPlanVerificationPathComponentType::OPERATOR_CHILD);
}
static LogicalPlanVerificationPath PlanExpressionPath(const LogicalPlanVerificationPath &path, idx_t ordinal) {
	return SQLExportHelpers::ChildPath(path, ordinal, LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION);
}

static LogicalPlanSQLExportResult PlanFailure(LogicalPlanVerificationIssue issue) {
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(std::move(issue));
	return LogicalPlanSQLExportResult::Failure(std::move(issues));
}

static LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
ExportTypedNull(const LogicalType &type, const LogicalPlanVerificationPath &path) {
	auto result = BoundExpressionSQLExporter::Export(BoundConstantExpression(Value(type)), {});
	if (!result.HasError()) {
		return result;
	}
	auto issues = result.GetIssues();
	for (auto &issue : issues) {
		issue.path = path;
		issue.phase = LogicalPlanVerificationPhase::PLAN_EXPORT;
	}
	return LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>::Failure(std::move(issues));
}

static LogicalPlanSQLFieldResult FieldFailure(LogicalPlanVerificationIssue issue) {
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(std::move(issue));
	return LogicalPlanSQLFieldResult::Failure(std::move(issues));
}

static bool IsValidText(const string &value) {
	return SQLExportHelpers::IsValidIdentifier(Identifier(value));
}

static LogicalPlanVerificationIssue ExtensionIssue(LogicalPlanVerificationIssueCode code,
                                                   const LogicalPlanVerificationPath &path, const string &identifier,
                                                   string message) {
	return SQLExportHelpers::MakeIssue(code, LogicalPlanVerificationPhase::PLAN_EXPORT, path,
	                                   LogicalPlanVerificationConstructIdentity::Extension(identifier),
	                                   std::move(message));
}

static LogicalPlanVerificationIssue PlanUnsupportedFeature(const LogicalPlanVerificationPath &path, string feature,
                                                           string message) {
	return SQLExportHelpers::MakeIssue(
	    LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, LogicalPlanVerificationPhase::PLAN_EXPORT, path,
	    LogicalPlanVerificationConstructIdentity::ExportFeature(std::move(feature)), std::move(message));
}

static LogicalPlanVerificationIssue UnsupportedOperator(const LogicalPlanVerificationPath &path,
                                                        LogicalOperatorType type) {
	return SQLExportHelpers::MakeIssue(LogicalPlanVerificationIssueCode::UNSUPPORTED_OPERATOR,
	                                   LogicalPlanVerificationPhase::PLAN_EXPORT, path,
	                                   LogicalPlanVerificationConstructIdentity::LogicalOperator(type),
	                                   "The logical operator does not have a SQL AST representation in this exporter");
}

static LogicalPlanVerificationFunctionIdentity LogicalSourceIdentity() {
	LogicalPlanVerificationFunctionIdentity source;
	source.name = "logical_source";
	source.return_type = LogicalType::TABLE;
	return source;
}

static LogicalPlanVerificationFunctionIdentity LogicalSourceIdentity(const LogicalGet &get) {
	LogicalPlanVerificationFunctionIdentity source;
	source.catalog = get.function.GetCatalogName().GetIdentifierName();
	source.schema = get.function.GetSchemaName().GetIdentifierName();
	source.name = get.function.GetName().GetIdentifierName();
	for (auto &parameter : get.parameters) {
		source.arguments.push_back(parameter.type());
	}
	for (auto &parameter : get.named_parameters) {
		source.arguments.push_back(parameter.second.type());
	}
	source.return_type = LogicalType::TABLE;
	return source;
}

static LogicalPlanVerificationIssue UnsupportedSource(const LogicalPlanVerificationPath &path,
                                                      LogicalPlanVerificationFunctionIdentity source, string guard) {
	auto issue = SQLExportHelpers::MakeIssue(
	    LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE, LogicalPlanVerificationPhase::PLAN_EXPORT, path,
	    LogicalPlanVerificationConstructIdentity::SourceFunction(std::move(source)),
	    "The logical source does not expose structural SQL export semantics");
	issue.facts.emplace_back("guard", Value(std::move(guard)));
	return issue;
}

static Identifier FieldIdentifier(idx_t ordinal) {
	return Identifier("c" + to_string(ordinal));
}

static LogicalPlanSQLFieldResult CreateFields(LogicalOperator &op, const LogicalPlanVerificationPath &path) {
	auto bindings = op.GetColumnBindings();
	D_ASSERT(bindings.size() == op.types.size());
	vector<LogicalPlanSQLExportField> fields;
	for (idx_t i = 0; i < bindings.size(); i++) {
		D_ASSERT(bindings[i].table_index.IsValid() && bindings[i].column_index.IsValid());
		if (!SQLExportHelpers::IsSQLValueType(op.types[i])) {
			auto issue = PlanUnsupportedFeature(path, "output_type", "Output type cannot be represented in SQL");
			issue.facts.emplace_back("column_index", Value::UBIGINT(i));
			issue.facts.emplace_back("logical_type", Value(op.types[i].ToString()));
			issue.facts.emplace_back("varchar_collations",
			                         Value(SQLExportHelpers::TypeCollationSignature(op.types[i])));
			return FieldFailure(std::move(issue));
		}
		fields.push_back({bindings[i], op.types[i]});
	}
	return LogicalPlanSQLFieldResult::Success(std::move(fields));
}

struct LogicalPlanSQLBindingEntry {
	ColumnBinding binding;
	LogicalType type;
	optional<LogicalType> optimizer_type;
	vector<Identifier> names;
};

struct LogicalPlanSQLExportedChild {
	LogicalPlanSQLExportRelation relation;
	Identifier relation_alias;
};

static BoundExpressionSQLExportContext
CreateBindingContext(ClientContext &context, const vector<reference<const LogicalPlanSQLExportedChild>> &children,
                     const vector<optional_ptr<const SelectNode>> &plain_scopes = {}) {
	D_ASSERT(plain_scopes.empty() || plain_scopes.size() == children.size());
	vector<LogicalPlanSQLBindingEntry> entries;
	for (idx_t child_index = 0; child_index < children.size(); child_index++) {
		auto &child = children[child_index].get();
		auto plain = plain_scopes.empty() ? nullptr : plain_scopes[child_index];
		for (idx_t i = 0; i < child.relation.fields.size(); i++) {
			auto &field = child.relation.fields[i];
			auto names = plain ? plain->select_list[i]->Cast<ColumnRefExpression>().ColumnNames()
			                   : vector<Identifier> {child.relation_alias, FieldIdentifier(i)};
			entries.push_back({field.source_binding, field.type, field.optimizer_type, std::move(names)});
		}
	}
	BoundExpressionSQLExportContext result;
	result.client_context = &context;
	result.discard_optimizer_metadata = true;
	result.resolve_binding =
	    [entries = std::move(entries)](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		for (auto &entry : entries) {
			if (entry.binding == binding) {
				return ResolvedSQLColumnReference {entry.names, entry.type, entry.optimizer_type};
			}
		}
		return {};
	};
	return result;
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

static void PropagateSemanticTypes(vector<LogicalPlanSQLExportField> &fields,
                                   const vector<reference<const LogicalPlanSQLExportedChild>> &children) {
	for (auto &field : fields) {
		for (auto &child : children) {
			for (auto &child_field : child.get().relation.fields) {
				if (field.source_binding != child_field.source_binding || field.type == child_field.type) {
					continue;
				}
				field.optimizer_type = field.type;
				field.type = child_field.type;
			}
		}
	}
}

static unique_ptr<TableRef> CreateSubquery(LogicalPlanSQLExportedChild child) {
	auto statement = make_uniq<SelectStatement>();
	statement->node = std::move(child.relation.query);
	auto result = make_uniq<SubqueryRef>(std::move(statement), std::move(child.relation_alias));
	for (idx_t i = 0; i < child.relation.fields.size(); i++) {
		result->column_name_alias.push_back(FieldIdentifier(i));
	}
	return std::move(result);
}

static LogicalPlanSQLExportResult ApplyOutputNames(LogicalPlanSQLExportResult result,
                                                   const vector<Identifier> &output_names) {
	if (result.HasError()) {
		return result;
	}
	if (output_names.size() != result.GetValue().fields.size()) {
		return PlanFailure(PlanUnsupportedFeature(LogicalPlanVerificationPath(), "output_names",
		                                          "Output name count does not match the exported plan"));
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

static unique_ptr<ParsedExpression> ChildColumn(const LogicalPlanSQLExportedChild &child, idx_t field_index,
                                                optional_ptr<const SelectNode> plain = nullptr) {
	D_ASSERT(field_index < child.relation.fields.size());
	if (plain) {
		return plain->select_list[field_index]->Copy();
	}
	return make_uniq<ColumnRefExpression>(FieldIdentifier(field_index), child.relation_alias);
}

static vector<reference<const Expression>> CollectExpressions(const LogicalOperator &op) {
	vector<reference<const Expression>> expressions;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expression) {
		D_ASSERT(expression && *expression);
		expressions.push_back(reference<const Expression>(**expression));
	});
	return expressions;
}

static bool HasEffectfulExpressions(const LogicalOperator &op) {
	for (auto &expression : CollectExpressions(op)) {
		if (expression.get().IsVolatile() || expression.get().CanThrow()) {
			return true;
		}
	}
	return false;
}

static bool HasEffectfulExpressionSubtree(const LogicalOperator &op) {
	for (auto &expression : CollectExpressions(op)) {
		auto semantic = expression.get().Copy();
		std::function<void(unique_ptr<Expression> &)> strip_compression = [&](unique_ptr<Expression> &value) {
			if (value->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
				auto &function = value->Cast<BoundFunctionExpression>();
				if (CMUtils::GetExpressionType(function) != CMExpressionType::NONE && !function.GetChildren().empty()) {
					auto child = std::move(function.GetChildrenMutable()[0]);
					value = std::move(child);
					strip_compression(value);
					return;
				}
			}
			ExpressionIterator::EnumerateChildren(*value, strip_compression);
		};
		strip_compression(semantic);
		if (semantic->IsVolatile() || semantic->CanThrow()) {
			return true;
		}
	}
	for (auto &child : op.children) {
		if (HasEffectfulExpressionSubtree(*child)) {
			return true;
		}
	}
	return false;
}

static bool OrderDistinguishesValues(ClientContext &context, const LogicalType &type) {
	auto default_collation = Settings::Get<DefaultCollationSetting>(context);
	return !TypeVisitor::Contains(type, [&](const LogicalType &child) {
		if (child.IsIntegral()) {
			return false;
		}
		switch (child.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::DECIMAL:
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::BLOB:
		case LogicalTypeId::BIT:
		case LogicalTypeId::UUID:
		case LogicalTypeId::ENUM:
		case LogicalTypeId::LIST:
		case LogicalTypeId::ARRAY:
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::MAP:
			return false;
		case LogicalTypeId::VARCHAR:
			return !StringType::GetCollation(child).empty() || !default_collation.empty();
		default:
			return true;
		}
	});
}

static bool OrdersAllFields(ClientContext &context, const vector<BoundOrderByNode> &orders, LogicalOperator &op) {
	auto bindings = op.GetColumnBindings();
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (!OrderDistinguishesValues(context, op.types[i])) {
			return false;
		}
		bool found = false;
		for (auto &order : orders) {
			if (order.expression->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    order.expression->Cast<BoundColumnRefExpression>().Binding() == bindings[i]) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

static bool HasChunkSensitiveConsumer(ClientContext &context, LogicalOperator &op, bool single_join_errors,
                                      bool order_required = false) {
	if (HasEffectfulExpressions(op)) {
		return true;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		if (order_required || (single_join_errors && op.Cast<LogicalJoin>().join_type == JoinType::SINGLE)) {
			return true;
		}
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		if (order_required) {
			return true;
		}
		break;
	case LogicalOperatorType::LOGICAL_LIMIT:
		order_required = true;
		break;
	case LogicalOperatorType::LOGICAL_TOP_N:
		order_required = !OrdersAllFields(context, op.Cast<LogicalTopN>().orders, op);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		if (OrdersAllFields(context, op.Cast<LogicalOrder>().orders, op)) {
			order_required = false;
		}
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		order_required |= op.Cast<LogicalDistinct>().distinct_type == DistinctType::DISTINCT_ON;
		break;
	case LogicalOperatorType::LOGICAL_GET:
		if (!op.children.empty()) {
			return true;
		}
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		break;
	default:
		return true;
	}
	for (auto &child : op.children) {
		if (HasChunkSensitiveConsumer(context, *child, single_join_errors, order_required)) {
			return true;
		}
	}
	return false;
}

static bool OrdersAggregateArguments(ClientContext &context, const BoundAggregateExpression &aggregate) {
	if (aggregate.Function().GetOrderDependent() == AggregateOrderDependent::NOT_ORDER_DEPENDENT) {
		return true;
	}
	if (!aggregate.GetOrderBys()) {
		return false;
	}
	for (auto &argument : aggregate.GetChildren()) {
		if (argument->IsScalar()) {
			continue;
		}
		if (!OrderDistinguishesValues(context, argument->GetReturnType())) {
			return false;
		}
		bool found = false;
		for (auto &order : aggregate.GetOrderBys()->orders) {
			if (argument->Equals(*order.expression)) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

static bool IsCurrentCoreAggregate(ClientContext &context, const AggregateFunction &definition,
                                   const vector<LogicalType> &arguments) {
	try {
		auto &entry = Catalog::GetEntry<AggregateFunctionCatalogEntry>(context, definition.GetQualifiedName());
		if (!entry.internal || entry.extension_name != Identifier("core_functions")) {
			return false;
		}
		ErrorData error;
		FunctionBinder function_binder(context);
		auto function_index = function_binder.BindFunction(entry.name, entry.functions, arguments, error);
		return function_index.IsValid() &&
		       *entry.functions.GetFunctionByOffset(function_index.GetIndex()) == definition;
	} catch (const Exception &) {
		return false;
	}
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

static bool CollectScopeAliases(const TableRef &table, identifier_set_t &aliases) {
	if (table.sample) {
		return false;
	}
	if (!table.alias.empty()) {
		return aliases.insert(table.alias).second;
	}
	if (table.type != TableReferenceType::JOIN) {
		return false;
	}
	auto &join = table.Cast<JoinRef>();
	return join.left && join.right && CollectScopeAliases(*join.left, aliases) &&
	       CollectScopeAliases(*join.right, aliases);
}

static optional_ptr<const SelectNode> PlainScope(const QueryNode &query) {
	if (query.type != QueryNodeType::SELECT_NODE || !query.modifiers.empty() || !query.cte_map.map.empty()) {
		return nullptr;
	}
	auto &select = query.Cast<SelectNode>();
	if (!select.from_table || select.sample || select.from_table->sample || !select.groups.group_expressions.empty() ||
	    !select.groups.grouping_sets.empty() || select.having || select.qualify ||
	    select.aggregate_handling != AggregateHandling::STANDARD_HANDLING) {
		return nullptr;
	}
	identifier_set_t aliases;
	if (!CollectScopeAliases(*select.from_table, aliases)) {
		return nullptr;
	}
	for (auto &expression : select.select_list) {
		if (expression->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			return nullptr;
		}
		auto &names = expression->Cast<ColumnRefExpression>().ColumnNames();
		if (names.size() != 2 || !aliases.count(names[0])) {
			return nullptr;
		}
	}
	return select;
}

static void SetChildScope(SelectNode &select, LogicalPlanSQLExportedChild child, optional_ptr<const SelectNode> plain) {
	if (!plain) {
		select.from_table = CreateSubquery(std::move(child));
		return;
	}
	auto &source = child.relation.query->Cast<SelectNode>();
	select.from_table = std::move(source.from_table);
	if (source.where_clause) {
		if (select.where_clause) {
			vector<unique_ptr<ParsedExpression>> predicates;
			predicates.push_back(std::move(source.where_clause));
			predicates.push_back(std::move(select.where_clause));
			select.where_clause =
			    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(predicates));
		} else {
			select.where_clause = std::move(source.where_clause);
		}
	}
}

static bool IsIdentityProjection(const LogicalProjection &projection, const vector<LogicalPlanSQLExportField> &fields) {
	if (projection.expressions.size() != fields.size()) {
		return false;
	}
	for (idx_t i = 0; i < fields.size(); i++) {
		auto &expression = *projection.expressions[i];
		if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &column = expression.Cast<BoundColumnRefExpression>();
		if (column.Depth() != 0 || column.Binding() != fields[i].source_binding ||
		    !SQLExportHelpers::SQLTypesMatch(column.GetReturnType(), fields[i].type)) {
			return false;
		}
	}
	return true;
}

static bool ExtractConstantPivotRows(LogicalOperator &op, vector<vector<Value>> &rows) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		if (!op.children.empty() || op.types.size() != 1) {
			return false;
		}
		rows.emplace_back();
		return true;
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return op.children.size() == 1;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		auto &values = op.Cast<LogicalExpressionGet>();
		if (values.children.size() != 1) {
			return false;
		}
		vector<vector<Value>> child_rows;
		if (!ExtractConstantPivotRows(*values.children[0], child_rows) || child_rows.size() != 1) {
			return false;
		}
		for (auto &input_row : values.expressions) {
			if (input_row.size() != values.expr_types.size()) {
				return false;
			}
			vector<Value> row;
			for (idx_t column_idx = 0; column_idx < input_row.size(); column_idx++) {
				auto &expression = input_row[column_idx];
				if (!expression || expression->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
				    !SQLExportHelpers::SQLTypesMatch(expression->GetReturnType(), values.expr_types[column_idx])) {
					return false;
				}
				row.push_back(expression->Cast<BoundConstantExpression>().GetValue());
			}
			rows.push_back(std::move(row));
		}
		return true;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1 || projection.expressions.size() != projection.types.size()) {
			return false;
		}
		vector<vector<Value>> child_rows;
		if (!ExtractConstantPivotRows(*projection.children[0], child_rows)) {
			return false;
		}
		auto child_bindings = projection.children[0]->GetColumnBindings();
		for (auto &child_row : child_rows) {
			vector<Value> row;
			for (idx_t column_idx = 0; column_idx < projection.expressions.size(); column_idx++) {
				auto &expression = projection.expressions[column_idx];
				if (!expression ||
				    !SQLExportHelpers::SQLTypesMatch(expression->GetReturnType(), projection.types[column_idx])) {
					return false;
				}
				if (expression->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					row.push_back(expression->Cast<BoundConstantExpression>().GetValue());
					continue;
				}
				if (expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
					return false;
				}
				auto &column = expression->Cast<BoundColumnRefExpression>();
				if (column.Depth() != 0) {
					return false;
				}
				auto child_column = std::find(child_bindings.begin(), child_bindings.end(), column.Binding());
				if (child_column == child_bindings.end() ||
				    NumericCast<idx_t>(child_column - child_bindings.begin()) >= child_row.size()) {
					return false;
				}
				row.push_back(child_row[NumericCast<idx_t>(child_column - child_bindings.begin())]);
			}
			rows.push_back(std::move(row));
		}
		return true;
	}
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &set_operation = op.Cast<LogicalSetOperation>();
		if (!set_operation.setop_all || set_operation.children.empty() ||
		    set_operation.column_count != op.types.size()) {
			return false;
		}
		for (auto &child : set_operation.children) {
			if (child->types.size() != op.types.size()) {
				return false;
			}
			for (idx_t column_idx = 0; column_idx < op.types.size(); column_idx++) {
				if (!SQLExportHelpers::SQLTypesMatch(child->types[column_idx], op.types[column_idx])) {
					return false;
				}
			}
			if (!ExtractConstantPivotRows(*child, rows)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

class LogicalPlanSQLExportState {
public:
	LogicalPlanSQLExportState(ClientContext &context_p, const LogicalPlanSQLExportOptions &options_p)
	    : context(context_p), options(options_p) {
	}

	LogicalPlanSQLExportResult Export(LogicalOperator &op, const LogicalPlanVerificationPath &path) {
		for (auto &source : limit_sources) {
			if (source.op.get() == &op) {
				return LogicalPlanSQLExportResult::Success(
				    {CreateNamedSource(source.name, source.relation.fields), source.relation.fields});
			}
		}
		auto source_count = limit_sources.size();
		ancestors.push_back(op);
		auto result = ExportOperator(op, path);
		ancestors.pop_back();
		limit_sources.resize(source_count);
		return result;
	}

private:
	LogicalPlanSQLExportResult ExportOperator(LogicalOperator &op, const LogicalPlanVerificationPath &path) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
			return ExportExpressionGet(op.Cast<LogicalExpressionGet>(), path);
		case LogicalOperatorType::LOGICAL_FILTER:
			return ExportFilter(op.Cast<LogicalFilter>(), path);
		case LogicalOperatorType::LOGICAL_PROJECTION:
			return ExportProjection(op.Cast<LogicalProjection>(), path);
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			return ExportAggregate(op.Cast<LogicalAggregate>(), path);
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
			return ExportConstantSource(op, path);
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_TOP_N:
		case LogicalOperatorType::LOGICAL_DISTINCT:
			return ExportModifier(op, path);
		case LogicalOperatorType::LOGICAL_LIMIT:
			return ExportLimit(op.Cast<LogicalLimit>(), path);
		case LogicalOperatorType::LOGICAL_SAMPLE:
			return ExportSample(op.Cast<LogicalSample>(), path);
		case LogicalOperatorType::LOGICAL_PIVOT:
			return ExportPivot(op.Cast<LogicalPivot>(), path);
		case LogicalOperatorType::LOGICAL_SECURE_VIEW:
			return ExportSecureView(op.Cast<LogicalSecureView>(), path);
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT:
			return ExportSetOperation(op.Cast<LogicalSetOperation>(), path);
		case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
			return ExportExtension(op.Cast<LogicalExtensionOperator>(), path);
		case LogicalOperatorType::LOGICAL_GET:
			return ExportGet(op.Cast<LogicalGet>(), path);
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
			return ExportJoin(op, path);
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
			return ExportChunkGet(op.Cast<LogicalColumnDataGet>(), path);
		case LogicalOperatorType::LOGICAL_WINDOW:
		case LogicalOperatorType::LOGICAL_UNNEST:
			return ExportContextExpressions(op, path);
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			return ExportMaterializedCTE(op.Cast<LogicalMaterializedCTE>(), path);
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
			return ExportRecursiveCTE(op.Cast<LogicalRecursiveCTE>(), path);
		case LogicalOperatorType::LOGICAL_CTE_REF:
			return ExportCTERef(op.Cast<LogicalCTERef>(), path);
		case LogicalOperatorType::LOGICAL_DELIM_GET:
			return PlanFailure(UnsupportedSource(path, LogicalSourceIdentity(), "delim_get"));
		default:
			D_ASSERT(op.type != LogicalOperatorType::LOGICAL_INVALID);
			return PlanFailure(UnsupportedOperator(path, op.type));
		}
	}

private:
	Identifier NextRelationAlias(const Identifier &preferred = Identifier()) {
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

	LogicalPlanVerificationResult<LogicalPlanSQLExportedChild> ExportChild(LogicalOperator &child,
	                                                                       const LogicalPlanVerificationPath &path) {
		auto exported = Export(child, path);
		if (exported.HasError()) {
			return LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>::Failure(exported.GetIssues());
		}
		LogicalPlanSQLExportedChild result {std::move(exported.GetValue()), NextRelationAlias()};
		return LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>::Success(std::move(result));
	}

	LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
	ExportExpression(const LogicalOperator &op, const vector<reference<const Expression>> &expressions,
	                 idx_t expression_ordinal, const BoundExpressionSQLExportContext &expression_context,
	                 const LogicalPlanVerificationPath &path) {
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
				if (value && SQLExportHelpers::SQLTypesMatch(value->type(), arguments[i]->GetReturnType())) {
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

	LogicalPlanSQLExportResult ExportMaterializedCTE(LogicalMaterializedCTE &cte,
	                                                 const LogicalPlanVerificationPath &path) {
		D_ASSERT(cte.children.size() == 2);
		auto fields = CreateFields(cte, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		auto name = NextRelationAlias(cte.ctename);
		auto producer = ExportNamedProducer(*cte.children[0], PlanChildPath(path, 0), name);
		if (producer.HasError()) {
			return LogicalPlanSQLExportResult::Failure(producer.GetIssues());
		}
		named_relations.push_back({cte.table_index, name, false, 0});
		auto consumer = ExportChild(*cte.children[1], PlanChildPath(path, 1));
		auto references = named_relations.back().references;
		named_relations.pop_back();
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
		auto select = ForwardFields(consumer.GetValue(), fields.GetValue());
		select->from_table = CreateSubquery(std::move(consumer.GetValue()));
		select->cte_map.map.insert(name, std::move(info));
		return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
	}

	LogicalPlanSQLExportResult ExportCTERef(LogicalCTERef &ref, const LogicalPlanVerificationPath &path) {
		D_ASSERT(ref.children.empty());
		optional<Identifier> name;
		for (idx_t i = named_relations.size(); i > 0; i--) {
			auto &relation = named_relations[i - 1];
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
		    {CreateNamedSource(*name, fields.GetValue(), ref.is_recurring), std::move(fields.GetValue())});
	}

	unique_ptr<SelectNode> CreateNamedSource(const Identifier &name, const vector<LogicalPlanSQLExportField> &fields,
	                                         bool recurring = false) {
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

	LogicalPlanSQLExportResult BuildRecursiveCTE(LogicalRecursiveCTE &cte, const LogicalPlanVerificationPath &path,
	                                             const Identifier &name) {
		D_ASSERT(cte.children.size() == 2);
		auto fields = CreateFields(cte, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		auto seed = Export(*cte.children[0], PlanChildPath(path, 0));
		if (seed.HasError()) {
			return LogicalPlanSQLExportResult::Failure(seed.GetIssues());
		}
		named_relations.push_back({cte.table_index, name, false, 0});
		named_relations.push_back({cte.table_index, name, true, 0});
		auto step = Export(*cte.children[1], PlanChildPath(path, 1));
		auto recurring_references = named_relations.back().references;
		named_relations.pop_back();
		auto references = named_relations.back().references;
		named_relations.pop_back();
		if (step.HasError()) {
			return LogicalPlanSQLExportResult::Failure(step.GetIssues());
		}
		if ((references == 0 && recurring_references == 0) || (cte.ref_recurring && recurring_references == 0)) {
			// Binding still needs a self reference when optimization removed the recursive scan.
			auto empty = CreateNamedSource(name, fields.GetValue(), cte.ref_recurring);
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
		key_context.client_context = &context;
		key_context.resolve_binding = [&](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
			if (binding.table_index != cte.table_index ||
			    binding.column_index.GetIndex() >= cte.internal_types.size()) {
				return {};
			}
			return ResolvedSQLColumnReference {{FieldIdentifier(binding.column_index.GetIndex())},
			                                   cte.internal_types[binding.column_index.GetIndex()]};
		};
		auto expressions = CollectExpressions(cte);
		idx_t expression_ordinal = 0;
		unordered_set<ProjectionIndex> key_columns;
		for (auto &key : cte.key_targets) {
			auto exported = ExportExpression(cte, expressions, expression_ordinal++, key_context, path);
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
				if (aggregate.IsDistinct() || aggregate.GetFilter() ||
				    (aggregate.GetOrderBys() && !aggregate.GetOrderBys()->orders.empty()) ||
				    aggregate.StateExportMode() != AggregateStateExportMode::NONE) {
					return PlanFailure(PlanUnsupportedFeature(
					    PlanExpressionPath(path, ordinal), "recursive_payload_modifiers",
					    "The recursive payload clause cannot preserve these aggregate modifiers"));
				}
				auto exported = BoundExpressionSQLExporter::ExportAggregateCallAtPath(
				    aggregate, key_context, PlanExpressionPath(path, ordinal));
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
	ExportNamedProducer(LogicalOperator &op, const LogicalPlanVerificationPath &path, const Identifier &name) {
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
		auto exported = BuildRecursiveCTE(op.Cast<LogicalRecursiveCTE>(), path, name);
		ancestors.pop_back();
		if (exported.HasError()) {
			return LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>::Failure(exported.GetIssues());
		}
		return LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>::Success(
		    {std::move(exported.GetValue()), NextRelationAlias()});
	}

	LogicalPlanSQLExportResult ExportRecursiveCTE(LogicalRecursiveCTE &cte, const LogicalPlanVerificationPath &path) {
		if (Optimizer::OptimizerDisabled(context, OptimizerType::CTE_INLINING) ||
		    Settings::Get<DebugDisableOptimizerSetting>(context)) {
			return PlanFailure(
			    PlanUnsupportedFeature(path, "recursive_cte_materialization",
			                           "The SQL wrapper requires CTE inlining to preserve recursive evaluation"));
		}
		auto name = NextRelationAlias(cte.ctename);
		auto recursive = BuildRecursiveCTE(cte, path, name);
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
		auto select = CreateNamedSource(name, recursive.GetValue().fields);
		select->cte_map.map.insert(name, std::move(info));
		return LogicalPlanSQLExportResult::Success({std::move(select), std::move(recursive.GetValue().fields)});
	}

	LogicalPlanSQLExportResult ExportContextExpressions(LogicalOperator &op, const LogicalPlanVerificationPath &path) {
		D_ASSERT(op.children.size() == 1);
		auto fields = CreateFields(op, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		if (op.type == LogicalOperatorType::LOGICAL_WINDOW &&
		    op.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.children[0]->Cast<LogicalGet>();
			if (get.source_ordinality == OrdinalityType::WITH_ORDINALITY && !get.ordinality_idx.IsValid()) {
				bool supported = op.expressions.size() == 1 && !get.table_filters.HasFilters() &&
				                 !get.extra_info.sample_options &&
				                 (!get.function.to_sql || get.function.to_sql == TableFunction::ToSQLFunctionCall);
				if (supported) {
					auto &window = op.expressions[0]->Cast<BoundWindowExpression>();
					supported = window.GetExpressionType() == ExpressionType::WINDOW_ROW_NUMBER &&
					            window.Partitions().empty() && window.OrderBy().empty() && window.GetChildren().empty();
				}
				if (!supported) {
					return PlanFailure(
					    PlanUnsupportedFeature(path, "ordinality_window",
					                           "The source ordinality cannot be reconstructed through this window"));
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

	bool RequiresMarkGroupMetadata(const LogicalComparisonJoin &join) {
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

	LogicalPlanSQLExportResult ExportJoin(LogicalOperator &op, const LogicalPlanVerificationPath &path) {
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
					return PlanFailure(
					    PlanUnsupportedFeature(path, "mark_condition_semantics",
					                           "The MARK condition requires conjunction execution semantics"));
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
					bool comparisons_only = !comparison.conditions.empty();
					bool all_equal = true;
					bool all_null_safe = true;
					for (auto &condition : comparison.conditions) {
						if (!condition.IsComparison()) {
							comparisons_only = false;
							continue;
						}
						all_equal &= condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL;
						all_null_safe &= condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
					}
					if (!comparisons_only || (comparison.conditions.size() != 1 && !all_equal && !all_null_safe)) {
						return PlanFailure(
						    PlanUnsupportedFeature(path, "mark_condition_semantics",
						                           "The MARK condition requires conjunction execution semantics"));
					}
				}
				if (RequiresMarkGroupMetadata(comparison)) {
					return PlanFailure(PlanUnsupportedFeature(
					    path, "mark_group_null_semantics", "The MARK join requires its group-specific NULL semantics"));
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

	LogicalPlanSQLExportResult ExportChunkGet(LogicalColumnDataGet &get, const LogicalPlanVerificationPath &path) {
		D_ASSERT(get.children.empty() && get.collection);
		if (!get.collection.is_owned()) {
			return PlanFailure(UnsupportedSource(path, LogicalSourceIdentity(), "borrowed_chunk_collection"));
		}
		if (get.collection->Count() == 0) {
			LogicalEmptyResult empty(get.types, get.GetColumnBindings());
			empty.ResolveOperatorTypes();
			return ExportConstantSource(empty, path);
		}
		auto fields = CreateFields(get, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		auto values = make_uniq<ExpressionListRef>();
		values->alias = NextRelationAlias();
		values->expected_types = get.types;
		for (idx_t i = 0; i < fields.GetValue().size(); i++) {
			values->expected_names.push_back(FieldIdentifier(i));
		}
		idx_t remaining = get.collection->Count();
		bool repacked = false;
		for (auto &chunk : get.collection->Chunks(get.GetColumnIds())) {
			repacked |= chunk.size() != MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
			remaining -= chunk.size();
			for (idx_t row = 0; row < chunk.size(); row++) {
				vector<unique_ptr<ParsedExpression>> exported_row;
				for (idx_t column = 0; column < chunk.ColumnCount(); column++) {
					auto value =
					    BoundExpressionSQLExporter::Export(BoundConstantExpression(chunk.GetValue(column, row)), {});
					if (value.HasError()) {
						return PlanFailure(PlanUnsupportedFeature(
						    path, "chunk_value", "The materialized value cannot be represented in SQL"));
					}
					exported_row.push_back(std::move(value.GetValue()));
				}
				values->values.push_back(std::move(exported_row));
			}
		}
		if (repacked && HasChunkSensitiveConsumer(context, ancestors.front().get(),
		                                          Settings::Get<ScalarSubqueryErrorOnMultipleRowsSetting>(context))) {
			return PlanFailure(PlanUnsupportedFeature(path, "chunk_consumer_evaluation",
			                                          "SQL cannot retain source chunks for an effectful consumer"));
		}
		auto select = make_uniq<SelectNode>();
		for (idx_t i = 0; i < fields.GetValue().size(); i++) {
			select->select_list.push_back(make_uniq<ColumnRefExpression>(FieldIdentifier(i), values->alias));
		}
		select->from_table = std::move(values);
		return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
	}

	LogicalPlanSQLExportResult ExportGet(LogicalGet &get, const LogicalPlanVerificationPath &path,
	                                     optional<LogicalPlanSQLExportField> ordinality = {}) {
		D_ASSERT(get.children.size() <= 1);
		auto fields = CreateFields(get, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		if ((!get.extra_info.file_filters.empty() || get.extra_info.total_files.IsValid()) &&
		    (!get.extra_info.file_filter_expressions ||
		     (get.function.to_sql && get.function.to_sql != TableFunction::ToSQLFunctionCall))) {
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
				return PlanFailure(PlanUnsupportedFeature(
				    path, "pruned_offset", "The row group pruning does not retain its original SQL offset"));
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
			if (to_sql != TableFunction::ToSQLFunctionCall) {
				return PlanFailure(UnsupportedSource(path, LogicalSourceIdentity(get), "source_ordinality"));
			}
			fields.GetValue().push_back(*ordinality);
			scan_fields.push_back(*ordinality);
			to_sql = TableFunction::ToSQLFunctionCallWithOrdinality;
		}
		auto relation_alias = NextRelationAlias();
		auto source_sql = to_sql(context, get, std::move(input), relation_alias);
		if (!source_sql.query) {
			auto guard =
			    source_sql.unsupported_reason.empty() ? "to_sql_callback_declined" : source_sql.unsupported_reason;
			return PlanFailure(UnsupportedSource(path, LogicalSourceIdentity(get), std::move(guard)));
		}
		auto query = std::move(source_sql.query);
		if (get.extra_info.sample_options) {
			auto sampling = get.extra_info.sample_options->Copy();
			if (!sampling->repeatable && sampling->seed.IsValid()) {
				sampling->seed = optional_idx::Invalid();
			}
			if (sampling->repeatable && !sampling->seed.IsValid()) {
				return PlanFailure(PlanUnsupportedFeature(path, "sample_repeatability",
				                                          "SQL sampling seeds imply repeatable sampling"));
			}
			if (sampling->seed.IsValid() && sampling->seed.GetIndex() > idx_t(NumericLimits<int64_t>::Maximum())) {
				return PlanFailure(
				    PlanUnsupportedFeature(path, "sample_seed", "The sampling seed has no SQL spelling"));
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
			auto exported = BoundExpressionSQLExporter::ExportAtPath(*predicate, binding_context,
			                                                         PlanExpressionPath(path, ordinal++));
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

	LogicalPlanSQLExportResult ExportExpressionGet(LogicalExpressionGet &get, const LogicalPlanVerificationPath &path) {
		D_ASSERT(get.children.size() == 1 && get.children[0]);
		D_ASSERT(!get.expressions.empty() && !get.expressions[0].empty());
		auto column_count = get.expressions[0].size();
		D_ASSERT(get.expr_types.size() == column_count);
		for (auto &row : get.expressions) {
			D_ASSERT(row.size() == column_count);
			for (idx_t i = 0; i < column_count; i++) {
				D_ASSERT(row[i] && row[i]->GetReturnType() == get.expr_types[i]);
			}
		}
		auto fields = CreateFields(get, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		if (get.children[0]->type != LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
			return ExportExpressionGetInput(get, path, std::move(fields.GetValue()));
		}

		auto values = make_uniq<ExpressionListRef>();
		values->alias = NextRelationAlias();
		values->expected_types = get.expr_types;
		for (idx_t i = 0; i < fields.GetValue().size(); i++) {
			values->expected_names.push_back(FieldIdentifier(i));
		}
		BoundExpressionSQLExportContext expression_context;
		expression_context.client_context = &context;
		auto expressions = CollectExpressions(get);
		idx_t expression_ordinal = 0;
		for (auto &row : get.expressions) {
			vector<unique_ptr<ParsedExpression>> exported_row;
			for (idx_t column_index = 0; column_index < row.size(); column_index++) {
				auto expression = ExportExpression(get, expressions, expression_ordinal++, expression_context, path);
				if (expression.HasError()) {
					return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
				}
				exported_row.push_back(std::move(expression.GetValue()));
			}
			values->values.push_back(std::move(exported_row));
		}
		auto values_alias = values->alias;
		auto select = make_uniq<SelectNode>();
		for (idx_t i = 0; i < fields.GetValue().size(); i++) {
			auto expression = make_uniq<ColumnRefExpression>(FieldIdentifier(i), values_alias);
			select->select_list.push_back(std::move(expression));
		}
		select->from_table = std::move(values);
		LogicalPlanSQLExportRelation relation {std::move(select), std::move(fields.GetValue())};
		return LogicalPlanSQLExportResult::Success(std::move(relation));
	}

	optional<LogicalPlanVerificationIssue> CheckExpressionGetInput(LogicalExpressionGet &get,
	                                                               const LogicalPlanVerificationPath &path) {
		if (HasEffectfulExpressionSubtree(get)) {
			return PlanUnsupportedFeature(path, "values_expression_evaluation",
			                              "VALUES with input requires nonvolatile, nonthrowing expressions");
		}
		return {};
	}

	LogicalPlanSQLExportResult ExportExpressionGetInput(LogicalExpressionGet &get,
	                                                    const LogicalPlanVerificationPath &path,
	                                                    vector<LogicalPlanSQLExportField> fields) {
		auto issue = CheckExpressionGetInput(get, path);
		if (issue) {
			return PlanFailure(std::move(*issue));
		}
		auto child = ExportChild(*get.children[0], PlanChildPath(path, 0));
		if (child.HasError()) {
			return LogicalPlanSQLExportResult::Failure(child.GetIssues());
		}
		auto expression_context = CreateBindingContext(context, {child.GetValue()});
		auto expressions = CollectExpressions(get);
		auto select = make_uniq<SelectNode>();
		select->from_table = CreateSubquery(std::move(child.GetValue()));

		auto row_alias = NextRelationAlias();
		auto cases = make_uniq<CaseExpression>();
		auto rows = make_uniq<ExpressionListRef>();
		rows->alias = row_alias;
		rows->expected_names.push_back(FieldIdentifier(0));
		rows->expected_types.push_back(LogicalType::BIGINT);
		for (idx_t row = 0; row < get.expressions.size(); row++) {
			// Keep all expressions of a VALUES row in one evaluation group.
			vector<FunctionArgument> arguments;
			for (idx_t column = 0; column < fields.size(); column++) {
				auto expression =
				    ExportExpression(get, expressions, row * fields.size() + column, expression_context, path);
				if (expression.HasError()) {
					return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
				}
				arguments.emplace_back(FieldIdentifier(column), std::move(expression.GetValue()));
			}
			auto value =
			    make_uniq<FunctionExpression>(QualifiedName("system", "main", "struct_pack"), std::move(arguments));
			if (row + 1 == get.expressions.size()) {
				cases->ElseMutable() = std::move(value);
			} else {
				CaseCheck check;
				check.when_expr = make_uniq<ComparisonExpression>(
				    ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>(FieldIdentifier(0), row_alias),
				    ConstantExpression::FromValue(Value::BIGINT(int64_t(row))));
				check.then_expr = std::move(value);
				cases->CaseChecksMutable().push_back(std::move(check));
			}
			vector<unique_ptr<ParsedExpression>> ordinal;
			ordinal.push_back(ConstantExpression::FromValue(Value::BIGINT(int64_t(row))));
			rows->values.push_back(std::move(ordinal));
		}
		unique_ptr<ParsedExpression> value;
		if (get.expressions.size() == 1) {
			value = std::move(cases->ElseMutable());
		} else {
			auto join = make_uniq<JoinRef>(JoinRefType::CROSS);
			join->left = std::move(select->from_table);
			join->right = std::move(rows);
			select->from_table = std::move(join);
			value = std::move(cases);
		}
		return ExportRow(std::move(select), std::move(value), std::move(fields));
	}

	LogicalPlanSQLExportResult ExportRow(unique_ptr<SelectNode> select, unique_ptr<ParsedExpression> value,
	                                     vector<LogicalPlanSQLExportField> fields) {
		// Keep row evaluation below consumers that can filter or limit emitted rows.
		vector<unique_ptr<ParsedExpression>> list_arguments;
		list_arguments.push_back(std::move(value));
		auto list =
		    make_uniq<FunctionExpression>(QualifiedName("system", "main", "list_value"), std::move(list_arguments));
		vector<unique_ptr<ParsedExpression>> unnest_arguments;
		unnest_arguments.push_back(std::move(list));
		auto unnest = make_uniq<FunctionExpression>(Identifier("unnest"), std::move(unnest_arguments));
		unnest->SetAlias(FieldIdentifier(0));
		select->select_list.push_back(std::move(unnest));

		auto statement = make_uniq<SelectStatement>();
		statement->node = std::move(select);
		auto packed_alias = NextRelationAlias();
		auto packed = make_uniq<SubqueryRef>(std::move(statement), packed_alias);
		packed->column_name_alias.push_back(FieldIdentifier(0));
		select = make_uniq<SelectNode>();
		select->from_table = std::move(packed);
		for (idx_t column = 0; column < fields.size(); column++) {
			auto value = make_uniq<OperatorExpression>(
			    ExpressionType::STRUCT_EXTRACT, make_uniq<ColumnRefExpression>(FieldIdentifier(0), packed_alias),
			    ConstantExpression::FromValue(Value(FieldIdentifier(column).GetIdentifierName())));
			value->SetAlias(FieldIdentifier(column));
			select->select_list.push_back(std::move(value));
		}
		return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields)});
	}

	LogicalPlanSQLExportResult ExportFilter(LogicalFilter &filter, const LogicalPlanVerificationPath &path) {
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
			select->where_clause =
			    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(predicates));
		}
		SetChildScope(*select, std::move(child.GetValue()), plain);
		LogicalPlanSQLExportRelation relation {std::move(select), std::move(fields.GetValue())};
		return LogicalPlanSQLExportResult::Success(std::move(relation));
	}

	LogicalPlanSQLExportResult ExportProjection(LogicalProjection &projection,
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

	LogicalPlanSQLExportResult ExportSecureView(LogicalSecureView &view, const LogicalPlanVerificationPath &path) {
		auto fields = CreateFields(view, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		if (!view.has_source || view.source_name.Path().empty() || view.source_types.empty()) {
			return PlanFailure(PlanUnsupportedFeature(path, "secure_view_source",
			                                          "The secure view does not retain its qualified source metadata"));
		}
		for (auto &component : view.source_name.Path()) {
			if (!IsValidText(component.GetIdentifierName())) {
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
			    !SQLExportHelpers::SQLTypesMatch(view.output_expressions[i]->GetReturnType(),
			                                     fields.GetValue()[i].type)) {
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

	LogicalPlanSQLExportResult ExportConstantSource(LogicalOperator &op, const LogicalPlanVerificationPath &path) {
		D_ASSERT(op.children.empty() && op.expressions.empty());
		auto fields = CreateFields(op, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		auto select = make_uniq<SelectNode>();
		select->from_table = make_uniq<EmptyTableRef>();
		for (idx_t i = 0; i < fields.GetValue().size(); i++) {
			auto expression = ExportTypedNull(fields.GetValue()[i].type, path);
			if (expression.HasError()) {
				return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
			}
			expression.GetValue()->SetAlias(FieldIdentifier(i));
			select->select_list.push_back(std::move(expression.GetValue()));
		}
		if (op.type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
			select->where_clause = ConstantExpression::FromValue(Value::BOOLEAN(false));
		}
		return LogicalPlanSQLExportResult::Success({std::move(select), std::move(fields.GetValue())});
	}

	unique_ptr<SelectNode> ForwardFields(const LogicalPlanSQLExportedChild &child,
	                                     const vector<LogicalPlanSQLExportField> &fields,
	                                     optional_ptr<const SelectNode> plain = nullptr) {
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

	LogicalPlanSQLExportResult ExportModifier(LogicalOperator &op, const LogicalPlanVerificationPath &path) {
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

	LogicalPlanSQLExportResult ExportSample(LogicalSample &sample, const LogicalPlanVerificationPath &path) {
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

	LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
	ExportPivotDefault(const BoundAggregateExpression &aggregate, const LogicalPlanVerificationPath &path) {
		if (aggregate.Function().GetStability() == FunctionStability::VOLATILE ||
		    aggregate.Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
			return LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>::Failure({PlanUnsupportedFeature(
			    path, "pivot_empty_aggregate",
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

	LogicalPlanVerificationResult<bool> VerifyPivotListSource(LogicalPivot &pivot,
	                                                          const LogicalPlanVerificationPath &path) {
		auto failure = [&](const string &message) {
			return LogicalPlanVerificationResult<bool>::Failure(
			    {PlanUnsupportedFeature(path, "pivot_list_source", message)});
		};
		auto &info = pivot.bound_pivot;
		vector<vector<Value>> constant_rows;
		if (ExtractConstantPivotRows(*pivot.children[0], constant_rows)) {
			if (pivot.children[0]->types.size() != info.group_count + info.aggregates.size() + 1) {
				return failure("The supplied PIVOT child has an unexpected layout");
			}
			for (auto &row : constant_rows) {
				if (row.size() != info.group_count + info.aggregates.size() + 1) {
					return failure("The supplied PIVOT child has an unexpected layout");
				}
				idx_t list_length = 0;
				for (idx_t list_idx = info.group_count; list_idx < row.size(); list_idx++) {
					auto &list = row[list_idx];
					if (list.IsNull() || list.type().id() != LogicalTypeId::LIST) {
						return failure("The supplied PIVOT child has a NULL or non-list value");
					}
					auto current_length = ListValue::GetChildren(list).size();
					if (list_idx == info.group_count) {
						list_length = current_length;
					} else if (current_length != list_length) {
						return failure("The supplied PIVOT child lists are not aligned");
					}
				}
			}
			return LogicalPlanVerificationResult<bool>::Success(true);
		}
		optional_ptr<LogicalOperator> source_op = pivot.children[0].get();
		auto bindings = source_op->GetColumnBindings();
		if (bindings.size() != info.group_count + info.aggregates.size() + 1) {
			return failure("The PIVOT child has an unexpected layout");
		}
		bindings.erase(bindings.begin(), bindings.begin() + info.group_count);
		while (source_op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = source_op->Cast<LogicalProjection>();
			auto outputs = projection.GetColumnBindings();
			for (auto &binding : bindings) {
				auto entry = std::find(outputs.begin(), outputs.end(), binding);
				if (entry == outputs.end()) {
					return failure("The PIVOT list projection has an unresolved input");
				}
				auto &expression = *projection.expressions[NumericCast<idx_t>(entry - outputs.begin())];
				if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
				    expression.Cast<BoundColumnRefExpression>().Depth() != 0) {
					return failure("The PIVOT child projection computes a new list");
				}
				binding = expression.Cast<BoundColumnRefExpression>().Binding();
			}
			source_op = projection.children[0].get();
		}
		if (source_op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			return failure("The PIVOT child list alignment is not represented");
		}
		auto &source = source_op->Cast<LogicalAggregate>();
		if (source.grouping_sets.size() > 1 ||
		    (!source.grouping_sets.empty() && source.grouping_sets[0].size() != source.groups.size())) {
			return failure("The PIVOT list source uses unrepresented grouping sets");
		}
		auto outputs = source.GetColumnBindings();
		for (auto &binding : bindings) {
			auto entry = std::find(outputs.begin(), outputs.end(), binding);
			if (entry == outputs.end() || NumericCast<idx_t>(entry - outputs.begin()) < source.groups.size()) {
				return failure("The PIVOT list column is not an aggregate output");
			}
			auto index = NumericCast<idx_t>(entry - outputs.begin()) - source.groups.size();
			if (index >= source.expressions.size() ||
			    source.expressions[index]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				return failure("The PIVOT list column is not an aggregate output");
			}
			auto &aggregate = source.expressions[index]->Cast<BoundAggregateExpression>();
			auto &definition = aggregate.Function().GetDefinition();
			if (!definition || definition->GetQualifiedName() != QualifiedName("system", "main", "list") ||
			    aggregate.Function().GetName() != Identifier("list") || aggregate.GetChildren().size() != 1 ||
			    aggregate.GetFilter() || aggregate.IsDistinct() ||
			    aggregate.StateExportMode() != AggregateStateExportMode::NONE) {
				return failure("The PIVOT child lists do not cover the same input rows");
			}
			if (!IsCurrentCoreAggregate(context, *definition, {aggregate.GetChildren()[0]->GetReturnType()})) {
				return failure("The PIVOT list source uses a modified aggregate definition");
			}
		}
		return LogicalPlanVerificationResult<bool>::Success(true);
	}

	LogicalPlanSQLExportResult ExportPivot(LogicalPivot &pivot, const LogicalPlanVerificationPath &path) {
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
		auto list_source = VerifyPivotListSource(pivot, path);
		if (list_source.HasError()) {
			return LogicalPlanSQLExportResult::Failure(list_source.GetIssues());
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
			if (!SQLExportHelpers::SQLTypesMatch(aggregate.GetReturnType(),
			                                     fields.GetValue()[info.group_count + aggregate_idx].type)) {
				return PlanFailure(PlanUnsupportedFeature(
				    path, "pivot_layout", "The PIVOT aggregate metadata does not match its output types"));
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
				    !SQLExportHelpers::SQLTypesMatch(info.aggregates[aggregate_idx]->GetReturnType(),
				                                     fields.GetValue()[output_idx].type) ||
				    !SQLExportHelpers::SQLTypesMatch(info.types[output_idx], fields.GetValue()[output_idx].type)) {
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
			if (!SQLExportHelpers::SQLTypesMatch(info.types[group_idx], fields.GetValue()[group_idx].type) ||
			    !SQLExportHelpers::SQLTypesMatch(child.GetValue().relation.fields[group_idx].type,
			                                     fields.GetValue()[group_idx].type)) {
				return PlanFailure(
				    PlanUnsupportedFeature(path, "pivot_layout", "The PIVOT group types do not match its child"));
			}
		}
		for (idx_t aggregate_idx = 0; aggregate_idx < aggregate_count; aggregate_idx++) {
			auto &list_type = child.GetValue().relation.fields[info.group_count + aggregate_idx].type;
			if (list_type.id() != LogicalTypeId::LIST ||
			    !SQLExportHelpers::SQLTypesMatch(ListType::GetChildType(list_type),
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
				position = call("list_position", reversed_keys->Copy(),
				                call("encode", ConstantExpression::FromValue(Value(key))));
			}
			auto index =
			    call("-", call("+", length->Copy(), ConstantExpression::FromValue(Value::BIGINT(1))), position->Copy());
			for (idx_t aggregate_idx = 0; aggregate_idx < aggregate_count; aggregate_idx++) {
				auto expression = call("list_extract", ChildColumn(child.GetValue(), info.group_count + aggregate_idx),
				                       index->Copy());
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

	bool ProducesOneRow(const LogicalOperator &op, const vector<TableIndex> &single_row_ctes = {}) {
		if (op.type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
			return true;
		}
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto &aggregate = op.Cast<LogicalAggregate>();
			return aggregate.groups.empty() && aggregate.grouping_sets.size() <= 1;
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			return ProducesOneRow(*op.children[0], single_row_ctes);
		}
		if (op.type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
			return op.Cast<LogicalExpressionGet>().expressions.size() == 1 &&
			       ProducesOneRow(*op.children[0], single_row_ctes);
		}
		if (op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			return ProducesOneRow(*op.children[0], single_row_ctes) && ProducesOneRow(*op.children[1], single_row_ctes);
		}
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			auto ctes = single_row_ctes;
			if (ProducesOneRow(*op.children[0], ctes)) {
				ctes.push_back(op.Cast<LogicalMaterializedCTE>().table_index);
			}
			return ProducesOneRow(*op.children[1], ctes);
		}
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto index = op.Cast<LogicalCTERef>().cte_index;
			return std::find(single_row_ctes.begin(), single_row_ctes.end(), index) != single_row_ctes.end();
		}
		return false;
	}

	using LimitExpressionResult = LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>;

	LimitExpressionResult LimitBindingFailure(const LogicalPlanVerificationPath &path) {
		return LimitExpressionResult::Failure({PlanUnsupportedFeature(
		    path, "limit_binding", "SQL LIMIT requires an independent, single-row scalar input")});
	}

	LimitExpressionResult ResolveLimitColumn(const ColumnBinding &binding, LogicalOperator &input,
	                                         const LogicalPlanVerificationPath &path) {
		auto bindings = input.GetColumnBindings();
		auto found = std::find(bindings.begin(), bindings.end(), binding);
		if (found == bindings.end()) {
			return LimitBindingFailure(path);
		}
		auto column = NumericCast<idx_t>(found - bindings.begin());
		if (input.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			return ExportLimitExpression(*input.expressions[column], *input.children[0], PlanChildPath(path, 0),
			                             PlanExpressionPath(path, column));
		}
		if (input.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
			return ResolveLimitColumn(binding, *input.children[0], PlanChildPath(path, 0));
		}
		if (input.type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			return LimitBindingFailure(path);
		}
		for (idx_t child_index = 0; child_index < input.children.size(); child_index++) {
			auto &child = *input.children[child_index];
			auto child_bindings = child.GetColumnBindings();
			auto child_column = std::find(child_bindings.begin(), child_bindings.end(), binding);
			if (child_column == child_bindings.end()) {
				continue;
			}
			auto child_path = PlanChildPath(path, child_index);
			if (!ProducesOneRow(child)) {
				return ResolveLimitColumn(binding, child, child_path);
			}
			idx_t source_index = 0;
			for (; source_index < limit_sources.size(); source_index++) {
				if (limit_sources[source_index].op.get() == &child) {
					break;
				}
			}
			if (source_index == limit_sources.size()) {
				auto exported = Export(child, child_path);
				if (exported.HasError()) {
					return LimitExpressionResult::Failure(exported.GetIssues());
				}
				limit_sources.push_back({child, NextRelationAlias(), std::move(exported.GetValue())});
			}
			auto scalar = make_uniq<SelectNode>();
			auto table = make_uniq<BaseTableRef>();
			table->SetTable(limit_sources[source_index].name);
			scalar->from_table = std::move(table);
			scalar->select_list.push_back(make_uniq<ColumnRefExpression>(
			    FieldIdentifier(NumericCast<idx_t>(child_column - child_bindings.begin()))));
			auto subquery = make_uniq<SubqueryExpression>();
			subquery->SubqueryMutable() = make_uniq<SelectStatement>();
			subquery->SubqueryMutable()->node = std::move(scalar);
			subquery->GetSubqueryTypeMutable() = SubqueryType::SCALAR;
			return LimitExpressionResult::Success(std::move(subquery));
		}
		return LimitBindingFailure(path);
	}

	LimitExpressionResult ExportLimitExpression(const Expression &expression, LogicalOperator &input,
	                                            const LogicalPlanVerificationPath &path,
	                                            const LogicalPlanVerificationPath &expression_path) {
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &column = expression.Cast<BoundColumnRefExpression>();
			return column.Depth() == 0 ? ResolveLimitColumn(column.Binding(), input, path) : LimitBindingFailure(path);
		}
		if (expression.IsVolatile()) {
			return LimitBindingFailure(path);
		}
		vector<pair<vector<Identifier>, unique_ptr<ParsedExpression>>> replacements;
		vector<LogicalPlanVerificationIssue> issues;
		BoundExpressionSQLExportContext expression_context;
		expression_context.client_context = context;
		expression_context.resolve_binding = [&](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
			auto resolved = ResolveLimitColumn(binding, input, path);
			if (resolved.HasError()) {
				issues = resolved.GetIssues();
				return {};
			}
			auto bindings = input.GetColumnBindings();
			auto found = std::find(bindings.begin(), bindings.end(), binding);
			D_ASSERT(found != bindings.end());
			vector<Identifier> names {NextRelationAlias(), FieldIdentifier(0)};
			replacements.emplace_back(names, std::move(resolved.GetValue()));
			return ResolvedSQLColumnReference {
			    std::move(names), input.types[NumericCast<idx_t>(found - bindings.begin())], {}};
		};
		auto result = BoundExpressionSQLExporter::ExportAtPath(expression, expression_context, expression_path);
		if (!issues.empty()) {
			return LimitExpressionResult::Failure(std::move(issues));
		}
		if (result.HasError()) {
			return result;
		}
		std::function<void(unique_ptr<ParsedExpression> &)> replace = [&](unique_ptr<ParsedExpression> &expr) {
			if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
				for (auto &entry : replacements) {
					if (expr->Cast<ColumnRefExpression>().ColumnNames() == entry.first) {
						expr = entry.second->Copy();
						return;
					}
				}
			}
			ParsedExpressionIterator::EnumerateChildren(*expr, replace);
		};
		replace(result.GetValue());
		return result;
	}

	LogicalPlanSQLExportResult ExportLimit(LogicalLimit &limit, const LogicalPlanVerificationPath &path) {
		D_ASSERT(limit.children.size() == 1);
		auto fields = CreateFields(limit, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields.GetIssues());
		}
		auto source_start = limit_sources.size();
		auto modifier = make_uniq<LimitModifier>();
		idx_t expression_ordinal = 0;
		for (idx_t i = 0; i < 2; i++) {
			auto &value = i == 0 ? limit.limit_val : limit.offset_val;
			auto &target = i == 0 ? modifier->limit : modifier->offset;
			switch (value.Type()) {
			case LimitNodeType::UNSET:
				break;
			case LimitNodeType::CONSTANT_VALUE:
				target = ConstantExpression::FromValue(Value::BIGINT(NumericCast<int64_t>(value.GetConstantValue())));
				break;
			case LimitNodeType::CONSTANT_PERCENTAGE:
				modifier->limit_type = LimitValueType::PERCENTAGE;
				target = ConstantExpression::FromValue(Value::DOUBLE(value.GetConstantPercentage()));
				break;
			case LimitNodeType::EXPRESSION_VALUE:
			case LimitNodeType::EXPRESSION_PERCENTAGE: {
				if (!value.GetExpression()->IsScalar()) {
					auto expression =
					    ExportLimitExpression(*value.GetExpression(), *limit.children[0], PlanChildPath(path, 0),
					                          PlanExpressionPath(path, expression_ordinal++));
					if (expression.HasError()) {
						return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
					}
					target = std::move(expression.GetValue());
					if (value.Type() == LimitNodeType::EXPRESSION_PERCENTAGE) {
						modifier->limit_type = LimitValueType::PERCENTAGE;
					}
					break;
				}
				if (value.Type() == LimitNodeType::EXPRESSION_PERCENTAGE) {
					modifier->limit_type = LimitValueType::PERCENTAGE;
				}
				auto expression = BoundExpressionSQLExporter::ExportAtPath(
				    *value.GetExpression(), {}, PlanExpressionPath(path, expression_ordinal++));
				if (expression.HasError()) {
					return LogicalPlanSQLExportResult::Failure(expression.GetIssues());
				}
				target = std::move(expression.GetValue());
				break;
			}
			default:
				D_ASSERT(false);
			}
		}
		auto child = ExportChild(*limit.children[0], PlanChildPath(path, 0));
		if (child.HasError()) {
			return LogicalPlanSQLExportResult::Failure(child.GetIssues());
		}
		PropagateSemanticTypes(fields.GetValue(), {child.GetValue()});
		if (limit.unpruned_offset.IsValid()) {
			modifier->offset =
			    ConstantExpression::FromValue(Value::BIGINT(NumericCast<int64_t>(limit.unpruned_offset.GetIndex())));
		}
		// Keep the ordering and its LIMIT consumer in the same query scope.
		bool has_limit = false;
		for (auto &existing : child.GetValue().relation.query->modifiers) {
			has_limit |= existing->type == ResultModifierType::LIMIT_MODIFIER;
		}
		unique_ptr<QueryNode> query;
		if (has_limit) {
			auto select = ForwardFields(child.GetValue(), fields.GetValue());
			select->from_table = CreateSubquery(std::move(child.GetValue()));
			query = std::move(select);
		} else {
			query = std::move(child.GetValue().relation.query);
		}
		query->modifiers.push_back(std::move(modifier));
		for (idx_t source_index = source_start; source_index < limit_sources.size(); source_index++) {
			auto &source = limit_sources[source_index];
			auto info = make_uniq<CommonTableExpressionInfo>();
			for (idx_t i = 0; i < source.relation.fields.size(); i++) {
				info->aliases.push_back(FieldIdentifier(i));
			}
			info->query_node = std::move(source.relation.query);
			info->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
			query->cte_map.map.insert(source.name, std::move(info));
		}
		return LogicalPlanSQLExportResult::Success({std::move(query), std::move(fields.GetValue())});
	}

	LogicalPlanSQLExportResult ExportSetOperation(LogicalSetOperation &op, const LogicalPlanVerificationPath &path) {
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

	LogicalPlanSQLExportResult ExportAggregate(LogicalAggregate &aggregate, const LogicalPlanVerificationPath &path) {
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

	LogicalPlanSQLExportResult ExportExtension(LogicalExtensionOperator &extension,
	                                           const LogicalPlanVerificationPath &path) {
		auto extension_identifier = extension.GetExtensionName();
		D_ASSERT(IsValidText(extension_identifier));
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
		LogicalPlanSQLExportExtensionInput input(extension, child_views, expressions.size(),
		                                         std::move(expression_exporter), expression_context);

		if (options.extension_resolver) {
			auto result = options.extension_resolver(input);
			auto handled = HandleExtensionResult(path, extension_identifier, std::move(result), fields.GetValue());
			if (handled) {
				return std::move(*handled);
			}
			for (auto &child : child_views) {
				D_ASSERT(child.table);
			}
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
			for (auto &child : child_views) {
				D_ASSERT(child.table);
			}
			break;
		}
		return PlanFailure(ExtensionIssue(LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION, path,
		                                  extension_identifier,
		                                  "No SQL export handler accepted the extension operator"));
	}

	optional<LogicalPlanSQLExportResult> HandleExtensionResult(const LogicalPlanVerificationPath &path,
	                                                           const string &extension_identifier,
	                                                           LogicalPlanSQLExportExtensionResult result,
	                                                           const vector<LogicalPlanSQLExportField> &fields) {
		auto malformed = [&](string message) {
			return PlanFailure(ExtensionIssue(LogicalPlanVerificationIssueCode::MALFORMED_EXTENSION_RESULT, path,
			                                  extension_identifier, std::move(message)));
		};
		switch (result.type) {
		case LogicalPlanSQLExportExtensionResultType::NOT_HANDLED:
			D_ASSERT(!result.query && result.reason.empty());
			return {};
		case LogicalPlanSQLExportExtensionResultType::EXPORTED:
			D_ASSERT(result.reason.empty());
			if (!result.query) {
				return malformed("EXPORTED extension result requires a query");
			}
			return LogicalPlanSQLExportResult::Success({std::move(result.query), fields});
		case LogicalPlanSQLExportExtensionResultType::UNSUPPORTED:
			D_ASSERT(!result.query);
			if (result.reason.empty()) {
				return malformed("UNSUPPORTED extension result requires a reason");
			}
			D_ASSERT(IsValidText(result.reason));
			return PlanFailure(ExtensionIssue(LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION, path,
			                                  extension_identifier, std::move(result.reason)));
		default:
			D_ASSERT(false);
			return {};
		}
	}

private:
	struct LimitSource {
		optional_ptr<LogicalOperator> op;
		Identifier name;
		LogicalPlanSQLExportRelation relation;
	};
	vector<LimitSource> limit_sources;

	struct NamedRelation {
		TableIndex index;
		Identifier name;
		bool is_recurring;
		idx_t references;
	};

	ClientContext &context;
	const LogicalPlanSQLExportOptions &options;
	idx_t next_relation_ordinal = 0;
	identifier_set_t relation_aliases;
	vector<reference<LogicalOperator>> ancestors;
	vector<NamedRelation> named_relations;
};

} // namespace logical_plan_sql_export

LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>
LogicalPlanSQLExporter::Export(ClientContext &context, LogicalOperator &root,
                               const LogicalPlanSQLExportOptions &options) {
	auto verification = LogicalPlanVerifier::VerifyAlways(root);
	if (verification.HasError()) {
		return LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>::Failure(verification.GetIssues());
	}
	logical_plan_sql_export::LogicalPlanSQLExportState state(context, options);
	auto result = state.Export(root, LogicalPlanVerificationPath());
	if (!options.output_names) {
		return result;
	}
	return logical_plan_sql_export::ApplyOutputNames(std::move(result), *options.output_names);
}

} // namespace duckdb
