#include "duckdb/planner/sql_export/bound_expression_sql_exporter_internal.hpp"
#include "duckdb/function/scalar/compressed_materialization_utils.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {
namespace bound_expression_sql_export {

static BoundAggregateSQLExportResult AggregateFailure(LogicalPlanVerificationIssue issue) {
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(std::move(issue));
	return BoundAggregateSQLExportResult::Failure(std::move(issues));
}

template <class FUNCTION>
static bool IsOptimizerFunctionQualification(const FUNCTION &function) {
	if (function.GetCatalogName().empty() && function.GetSchemaName().empty()) {
		return true;
	}
	return function.GetCatalogName() == "system" && function.GetSchemaName() == "main";
}

BoundExpressionSQLExportResult BoundExpressionSQLExportState::ExportLambda(const BoundLambdaExpression &lambda,
                                                                           const BoundFunctionExpression &function,
                                                                           idx_t logical_argument_count,
                                                                           const LogicalPlanVerificationPath &path) {
	const bool has_lambda_body = lambda.GetExpressionType() == ExpressionType::LAMBDA &&
	                             lambda.GetReturnType() == LogicalType::LAMBDA && lambda.LambdaExpr();
	const bool has_parameter_names =
	    lambda.ParameterCount() > 0 && lambda.ParameterNames().size() == lambda.ParameterCount();
	if (!has_lambda_body || !lambda.Captures().empty() || !has_parameter_names) {
		return Failure(
		    UnsupportedFeature(path, "lambda_binding", "The bound lambda does not retain its SQL parameter binding"));
	}

	vector<unique_ptr<ParsedExpression>> references;
	for (idx_t index = 0; index < lambda.ParameterCount(); index++) {
		auto &parameter = lambda.ParameterNames()[lambda.ParameterCount() - index - 1];
		references.push_back(make_uniq<ColumnRefExpression>(parameter));
	}
	for (idx_t index = logical_argument_count; index < function.GetChildren().size(); index++) {
		auto reference = Export(*function.GetChildren()[index], ChildPath(path, index));
		if (reference.HasError()) {
			return reference;
		}
		references.push_back(std::move(reference.GetValue()));
	}

	lambda_reference_scopes.push_back(std::move(references));
	try {
		auto body = Export(*lambda.LambdaExpr(), ChildPath(path, 0));
		lambda_reference_scopes.pop_back();
		if (body.HasError()) {
			return body;
		}
		vector<string> parameter_names;
		for (auto &parameter : lambda.ParameterNames()) {
			parameter_names.push_back(parameter.GetIdentifierName());
		}
		return BoundExpressionSQLExportResult::Success(
		    make_uniq<LambdaExpression>(std::move(parameter_names), std::move(body.GetValue())));
	} catch (...) {
		lambda_reference_scopes.pop_back();
		throw;
	}
}

BoundExpressionSQLExportResult BoundExpressionSQLExportState::CompressedMaterializationFailure(
    const BoundFunctionExpression &expression, const LogicalPlanVerificationPath &path, string message) {
	auto &function = expression.Function();
	auto &definition = function.GetDefinition();
	D_ASSERT(definition);
	return Failure(UnsupportedFunction(
	    path, DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(), function.GetLogicalReturnType()),
	    std::move(message)));
}

optional<BoundExpressionSQLExportResult>
BoundExpressionSQLExportState::TryExportCompressedMaterialization(const BoundFunctionExpression &expression,
                                                                  const LogicalPlanVerificationPath &path) {
	auto &function = expression.Function();
	auto &definition = function.GetDefinition();
	if (!definition) {
		return {};
	}
	auto compress = CMUtils::GetExpressionType(expression) == CMExpressionType::COMPRESS;
	auto decompress = CMUtils::GetExpressionType(expression) == CMExpressionType::DECOMPRESS;
	if (!compress && !decompress) {
		return {};
	}
	if (!IsOptimizerFunctionQualification(*definition) || !IsOptimizerFunctionQualification(function) ||
	    expression.GetChildren().empty()) {
		return CompressedMaterializationFailure(expression, path,
		                                        "The compressed materialization expression is malformed");
	}
	if (!context.discard_optimizer_metadata) {
		return CompressedMaterializationFailure(expression, path,
		                                        "Compressed materialization requires its complete logical plan");
	}
	if (decompress && expression.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column = expression.GetChildren()[0]->Cast<BoundColumnRefExpression>();
		auto resolved = context.resolve_binding ? context.resolve_binding(column.Binding())
		                                        : optional<ResolvedSQLColumnReference>();
		if (!resolved || resolved->type != expression.GetReturnType()) {
			return CompressedMaterializationFailure(expression, path,
			                                        "The decompression input has no equivalent SQL representation");
		}
	}
	return ExportChild(*expression.GetChildren()[0], path, 0);
}

BoundExpressionSQLExportResult
BoundExpressionSQLExportState::ExportScalarFunction(const BoundFunctionExpression &expression,
                                                    const LogicalPlanVerificationPath &path) {
	auto &function = expression.Function();
	auto &definition = function.GetDefinition();
	if (!definition) {
		return Failure(
		    InternalExpressionInvariant(path, expression, "Bound scalar function has no retained definition"));
	}
	if (function.GetBindCallback() == TableFilterFunctions::Bind &&
	    TableFilterFunctions::IsTableFilterFunction(function.GetName())) {
		auto identity =
		    DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(), function.GetLogicalReturnType());
		if (!context.discard_optimizer_metadata) {
			return Failure(UnsupportedFunction(path, std::move(identity),
			                                   "Internal table filters require their complete logical plan"));
		}
		if (expression.GetReturnType() != LogicalType::BOOLEAN || expression.GetChildren().size() != 1) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Internal table filter metadata is malformed"));
		}
		return BoundExpressionSQLExportResult::Success(ConstantExpression::FromValue(Value::BOOLEAN(true)));
	}
	auto compressed = TryExportCompressedMaterialization(expression, path);
	if (compressed) {
		return std::move(*compressed);
	}
	auto identity =
	    DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(), function.GetLogicalReturnType());
	if (!identity.IsValid()) {
		identity.arguments.clear();
		for (auto &child : expression.GetChildren()) {
			identity.arguments.push_back(child->GetReturnType());
		}
		identity.return_type = expression.GetReturnType();
		if (!identity.IsValid()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound scalar function identity is incomplete"));
		}
	}
	auto qualified_name = definition->GetQualifiedName();
	optional_idx lambda_index;
	for (idx_t index = 0; index < MinValue(expression.GetChildren().size(), function.GetLogicalArguments().size());
	     index++) {
		if (expression.GetChildren()[index]->GetExpressionClass() == ExpressionClass::BOUND_LAMBDA) {
			if (lambda_index.IsValid()) {
				return Failure(UnsupportedFunction(path, std::move(identity),
				                                   "The scalar function retains multiple SQL lambda arguments"));
			}
			lambda_index = index;
		}
	}
	if (function.HasBindLambdaCallback() != lambda_index.IsValid()) {
		return Failure(UnsupportedFunction(path, std::move(identity),
		                                   "The scalar function does not retain its SQL lambda argument"));
	}
	const bool is_date_part = qualified_name == QualifiedName("system", "main", "date_part") ||
	                          qualified_name == QualifiedName("system", "main", "datepart");
	const bool has_specialized_name = function.GetName() != definition->GetName() &&
	                                  IsOptimizerFunctionQualification(function) && !function.GetName().empty();
	const auto logical_argument_count = function.GetLogicalArguments().size();
	const auto child_count = expression.GetChildren().size();
	const bool rewritten_date_part =
	    is_date_part && logical_argument_count == 2 && child_count == 1 && has_specialized_name;
	const bool retained_variadic_arguments = definition->HasVarArgs() && child_count >= logical_argument_count;
	const bool has_scalar_arguments =
	    child_count == logical_argument_count || retained_variadic_arguments || rewritten_date_part;
	const bool has_expected_arguments =
	    lambda_index.IsValid() ? child_count >= logical_argument_count : has_scalar_arguments;
	if (!has_expected_arguments) {
		return Failure(
		    UnsupportedFunction(path, std::move(identity), "The scalar function does not retain every SQL argument"));
	}
	auto name = rewritten_date_part ? optional<QualifiedName>(QualifiedName("system", "main", function.GetName()))
	                                : RebindableFunctionName(*definition);
	if (!name || !IsSQLValueType(expression.GetReturnType())) {
		return Failure(UnsupportedFunction(path, std::move(identity),
		                                   "The retained scalar function definition is not representable as SQL"));
	}
	bool captured_aliases_are_ignored = *name == QualifiedName("system", "main", "row");
	bool argument_aliases_are_semantic = qualified_name == QualifiedName("system", "main", "struct_pack");
	idx_t first_argument_alias = 0;
	if (qualified_name == QualifiedName("system", "main", "struct_update") ||
	    qualified_name == QualifiedName("system", "main", "write_log")) {
		argument_aliases_are_semantic = true;
		first_argument_alias = 1;
	}
	const bool can_reconstruct_argument_names =
	    argument_aliases_are_semantic || captured_aliases_are_ignored || definition->HasUnbindCallback();
	if (definition->GetProperties().GetCaptureArgumentAliases() && !can_reconstruct_argument_names) {
		return Failure(UnsupportedFunction(path, std::move(identity),
		                                   "The bound scalar function does not expose its SQL argument names"));
	}
	if (definition->GetProperties().RequiresExpressionNames() && !can_reconstruct_argument_names) {
		return Failure(UnsupportedFunction(
		    path, std::move(identity), "The bound scalar function requires expression names that are not retained"));
	}
	vector<Identifier> argument_names;
	if (argument_aliases_are_semantic) {
		argument_names.resize(expression.GetChildren().size());
		for (idx_t argument_index = first_argument_alias; argument_index < argument_names.size(); argument_index++) {
			argument_names[argument_index] = expression.GetChildren()[argument_index]->GetAlias();
			if (argument_names[argument_index].empty()) {
				return Failure(UnsupportedFunction(path, std::move(identity),
				                                   "The bound scalar function is missing a SQL argument name"));
			}
		}
	}
	vector<unique_ptr<ParsedExpression>> children;
	auto sql_argument_count =
	    lambda_index.IsValid() ? function.GetLogicalArguments().size() : expression.GetChildren().size();
	if (rewritten_date_part) {
		sql_argument_count = 1;
	}
	for (idx_t child_index = 0; child_index < sql_argument_count; child_index++) {
		auto child = lambda_index == child_index
		                 ? ExportLambda(expression.GetChildren()[child_index]->Cast<BoundLambdaExpression>(),
		                                expression, sql_argument_count, ChildPath(path, child_index))
		                 : Export(*expression.GetChildren()[child_index], ChildPath(path, child_index));
		if (child.HasError()) {
			return child;
		}
		children.push_back(std::move(child.GetValue()));
	}
	unique_ptr<ParsedExpression> result;
	if (definition->HasUnbindCallback()) {
		FunctionUnbindInput input(expression, std::move(children));
		result = definition->GetUnbindCallback()(input);
		if (!result) {
			return Failure(
			    UnsupportedFunction(path, std::move(identity), "The function cannot reconstruct its bound invocation"));
		}
	} else if (!argument_names.empty()) {
		vector<FunctionArgument> arguments;
		for (idx_t argument_index = 0; argument_index < children.size(); argument_index++) {
			arguments.emplace_back(argument_names[argument_index], std::move(children[argument_index]));
		}
		result = make_uniq<FunctionExpression>(*name, std::move(arguments), nullptr, nullptr, false, false, false);
	} else {
		result = make_uniq<FunctionExpression>(*name, std::move(children), nullptr, nullptr, false, false, false);
	}
	// Restore result types when binding or optimization changed argument types.
	const bool can_restore_result_type = !captured_aliases_are_ignored &&
	                                     IsSQLRepresentableType(expression.GetReturnType()) &&
	                                     !expression.GetReturnType().IsAggregateState();
	const bool has_specialized_result_type =
	    (definition->HasBindCallback() || definition->GetReturnType().id() == LogicalTypeId::SQLNULL) &&
	    definition->GetReturnType() != expression.GetReturnType();
	if (can_restore_result_type && has_specialized_result_type) {
		return RestoreResultType(expression.GetReturnType(), std::move(result), path);
	}
	return BoundExpressionSQLExportResult::Success(std::move(result));
}

BoundAggregateSQLExportResult
BoundExpressionSQLExportState::BuildAggregateCall(const BoundAggregateExpression &expression,
                                                  const LogicalPlanVerificationPath &path) {
	D_ASSERT(expression.GetExpressionType() == ExpressionType::BOUND_AGGREGATE);
	auto &function = expression.Function();
	auto &definition = function.GetDefinition();
	if (!definition) {
		return AggregateFailure(
		    InternalExpressionInvariant(path, expression, "Bound aggregate function has no retained definition"));
	}
	auto identity =
	    DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(), function.GetLogicalReturnType());
	if (!identity.IsValid()) {
		return AggregateFailure(
		    InternalExpressionInvariant(path, expression, "Bound aggregate function identity is incomplete"));
	}
	auto name = RebindableFunctionName(*definition);
	const bool rewritten_min =
	    name && *name == QualifiedName("system", "main", "min") && function.GetName() == "arg_min";
	const bool rewritten_max =
	    name && *name == QualifiedName("system", "main", "max") && function.GetName() == "arg_max";
	const bool has_collation_argument = expression.GetChildren().size() == function.GetLogicalArguments().size() + 1;
	const bool collated_minmax =
	    (rewritten_min || rewritten_max) && IsOptimizerFunctionQualification(function) && has_collation_argument;
	if (collated_minmax) {
		name = QualifiedName("system", "main", function.GetName());
	} else if (expression.GetChildren().size() != function.GetLogicalArguments().size()) {
		return AggregateFailure(
		    UnsupportedFunction(path, std::move(identity), "The aggregate does not retain every SQL argument"));
	}
	if (!name || !IsSQLValueType(expression.GetReturnType())) {
		return AggregateFailure(UnsupportedFunction(
		    path, std::move(identity), "The retained aggregate function definition is not representable as SQL"));
	}
	D_ASSERT(expression.GetAggregateType() == AggregateType::NON_DISTINCT ||
	         expression.GetAggregateType() == AggregateType::DISTINCT);
	D_ASSERT(expression.StateExportMode() == AggregateStateExportMode::NONE ||
	         expression.StateExportMode() == AggregateStateExportMode::STATE_EXPORT);
	if (definition->GetProperties().GetCaptureArgumentAliases() ||
	    definition->GetProperties().RequiresExpressionNames()) {
		return AggregateFailure(UnsupportedFunction(
		    path, std::move(identity), "The bound aggregate requires argument aliases that are not retained"));
	}
	vector<ChildExpression> source_children;
	for (idx_t child_index = 0; child_index < expression.GetChildren().size(); child_index++) {
		source_children.emplace_back(expression.GetChildren()[child_index].get());
	}
	if (expression.GetFilter()) {
		source_children.emplace_back(expression.GetFilter().get(), LogicalType::BOOLEAN);
	}
	if (expression.GetOrderBys()) {
		for (auto &order : expression.GetOrderBys()->orders) {
			const bool has_explicit_order = order.type == OrderType::ASCENDING || order.type == OrderType::DESCENDING;
			const bool has_explicit_null_order =
			    order.null_order == OrderByNullType::NULLS_FIRST || order.null_order == OrderByNullType::NULLS_LAST;
			if (!has_explicit_order || !has_explicit_null_order) {
				return AggregateFailure(
				    InternalExpressionInvariant(path, expression, "Bound aggregate has an invalid ordering mode"));
			}
			source_children.emplace_back(order.expression.get());
		}
	}

	vector<unique_ptr<ParsedExpression>> children;
	vector<LogicalPlanVerificationIssue> issues;
	ExportChildren(source_children, path, children, issues);
	if (!issues.empty()) {
		return BoundAggregateSQLExportResult::Failure(std::move(issues));
	}
	vector<unique_ptr<ParsedExpression>> arguments;
	for (idx_t child_index = 0; child_index < expression.GetChildren().size(); child_index++) {
		arguments.push_back(std::move(children[child_index]));
	}
	idx_t child_index = expression.GetChildren().size();
	unique_ptr<ParsedExpression> filter;
	if (expression.GetFilter()) {
		filter = std::move(children[child_index++]);
	}
	unique_ptr<OrderModifier> order_bys;
	if (expression.GetOrderBys()) {
		order_bys = make_uniq<OrderModifier>();
		for (auto &order : expression.GetOrderBys()->orders) {
			order_bys->orders.emplace_back(order.type, order.null_order, std::move(children[child_index++]));
		}
	}
	auto result = make_uniq<FunctionExpression>(*name, std::move(arguments), std::move(filter), std::move(order_bys),
	                                            expression.IsDistinct(), false,
	                                            expression.StateExportMode() == AggregateStateExportMode::STATE_EXPORT);
	return BoundAggregateSQLExportResult::Success(std::move(result));
}

BoundExpressionSQLExportResult
BoundExpressionSQLExportState::ExportAggregate(const BoundAggregateExpression &expression,
                                               const LogicalPlanVerificationPath &path) {
	auto call = BuildAggregateCall(expression, path);
	if (call.HasError()) {
		return BoundExpressionSQLExportResult::Failure(call.GetIssues());
	}
	auto &function = expression.Function();
	auto &definition = function.GetDefinition();
	unique_ptr<ParsedExpression> result = std::move(call.GetValue());
	const bool can_restore_result_type = expression.StateExportMode() == AggregateStateExportMode::NONE &&
	                                     IsSQLRepresentableType(expression.GetReturnType()) &&
	                                     !expression.GetReturnType().IsAggregateState();
	const bool has_specialized_result_type =
	    definition->HasBindCallback() && definition->GetReturnType() != expression.GetReturnType();
	if (can_restore_result_type && has_specialized_result_type) {
		return RestoreResultType(expression.GetReturnType(), std::move(result), path);
	}
	return BoundExpressionSQLExportResult::Success(std::move(result));
}

BoundAggregateSQLExportResult
BoundExpressionSQLExportState::ExportAggregateCall(const BoundAggregateExpression &expression,
                                                   const LogicalPlanVerificationPath &path) {
	auto call = BuildAggregateCall(expression, path);
	if (call.HasError()) {
		return call;
	}
	if (!expression.GetReturnType().EqualsIncludingCollation(expression.Function().GetLogicalReturnType())) {
		return AggregateFailure(
		    UnsupportedFeature(path, "aggregate_call_result_type",
		                       "A bare aggregate call cannot preserve the bound expression's logical result type"));
	}
	return call;
}

} // namespace bound_expression_sql_export
} // namespace duckdb
