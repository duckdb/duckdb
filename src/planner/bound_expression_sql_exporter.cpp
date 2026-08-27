#include "duckdb/planner/bound_expression_sql_exporter.hpp"

#include "duckdb/common/type_visitor.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

using BoundExpressionSQLExportResult = LogicalPlanCompilerResult<unique_ptr<ParsedExpression>>;

static LogicalPlanCompilerPath ChildPath(const LogicalPlanCompilerPath &path, idx_t child_index) {
	auto child_path = path;
	child_path.components.push_back(
	    LogicalPlanCompilerPathComponent {LogicalPlanCompilerPathComponentType::EXPRESSION_CHILD, child_index});
	return child_path;
}

static bool IsExpressionRootPath(const LogicalPlanCompilerPath &path) {
	if (!path.IsValid()) {
		return false;
	}
	if (path.root == LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION) {
		return true;
	}
	for (auto &component : path.components) {
		if (component.type == LogicalPlanCompilerPathComponentType::OPERATOR_EXPRESSION) {
			return true;
		}
	}
	return false;
}

static LogicalPlanCompilerIssue InternalInvariant(optional<LogicalPlanCompilerPath> path, string message,
                                                  optional<LogicalPlanCompilerConstructIdentity> construct = {}) {
	LogicalPlanCompilerIssue issue;
	issue.code = LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT;
	issue.phase = LogicalPlanCompilerPhase::EXPRESSION_EXPORT;
	issue.path = std::move(path);
	issue.construct = std::move(construct);
	issue.message = std::move(message);
	return issue;
}

static LogicalPlanCompilerIssue InternalExpressionInvariant(const LogicalPlanCompilerPath &path,
                                                            const Expression &expression, string message) {
	return InternalInvariant(path, std::move(message),
	                         LogicalPlanCompilerConstructIdentity::Expression(expression.GetExpressionClass()));
}

static LogicalPlanCompilerIssue UnsupportedExpression(const LogicalPlanCompilerPath &path,
                                                      ExpressionClass expression_class) {
	LogicalPlanCompilerIssue issue;
	issue.code = LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPRESSION;
	issue.phase = LogicalPlanCompilerPhase::EXPRESSION_EXPORT;
	issue.path = path;
	issue.construct = LogicalPlanCompilerConstructIdentity::Expression(expression_class);
	issue.message = "The bound expression class does not have a SQL AST representation in this exporter";
	return issue;
}

static LogicalPlanCompilerIssue UnsupportedFeature(const LogicalPlanCompilerPath &path, string feature,
                                                   string message) {
	LogicalPlanCompilerIssue issue;
	issue.code = LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE;
	issue.phase = LogicalPlanCompilerPhase::EXPRESSION_EXPORT;
	issue.path = path;
	issue.construct = LogicalPlanCompilerConstructIdentity::ExportFeature(std::move(feature));
	issue.message = std::move(message);
	return issue;
}

static LogicalPlanCompilerIssue UnsupportedFunction(const LogicalPlanCompilerPath &path,
                                                    LogicalPlanCompilerFunctionIdentity identity, string message) {
	LogicalPlanCompilerIssue issue;
	issue.code = LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION;
	issue.phase = LogicalPlanCompilerPhase::EXPRESSION_EXPORT;
	issue.path = path;
	issue.construct = LogicalPlanCompilerConstructIdentity::Function(std::move(identity));
	issue.message = std::move(message);
	return issue;
}

static BoundExpressionSQLExportResult Failure(LogicalPlanCompilerIssue issue) {
	vector<LogicalPlanCompilerIssue> issues;
	issues.push_back(std::move(issue));
	return BoundExpressionSQLExportResult::Failure(std::move(issues));
}

static bool IsValidIdentifier(const Identifier &identifier) {
	auto &name = identifier.GetIdentifierName();
	return !name.empty() && name.find('\0') == string::npos && Value::StringIsValid(name);
}

static bool IsSQLRepresentableType(const LogicalType &type) {
	if (!type.IsComplete()) {
		return false;
	}
	return !TypeVisitor::Contains(type, [](const LogicalType &entry) {
		switch (entry.id()) {
		case LogicalTypeId::INVALID:
		case LogicalTypeId::UNKNOWN:
		case LogicalTypeId::ANY:
		case LogicalTypeId::UNBOUND:
		case LogicalTypeId::TEMPLATE:
		case LogicalTypeId::TYPE:
		case LogicalTypeId::STRING_LITERAL:
		case LogicalTypeId::INTEGER_LITERAL:
		case LogicalTypeId::POINTER:
		case LogicalTypeId::VALIDITY:
		case LogicalTypeId::TABLE:
		case LogicalTypeId::LEGACY_AGGREGATE_STATE:
		case LogicalTypeId::LAMBDA:
		case LogicalTypeId::TUPLE:
			return true;
		default:
			return false;
		}
	});
}

static bool DefinitionArityMatches(const FunctionSignature &signature, idx_t argument_count) {
	auto required_count = signature.GetRequiredParameterCount();
	if (argument_count < required_count) {
		return false;
	}
	return signature.HasVarArgs() || argument_count <= signature.GetParameterCount();
}

static bool ChildrenMatchArguments(const vector<unique_ptr<Expression>> &children,
                                   const vector<LogicalType> &arguments) {
	if (children.size() != arguments.size()) {
		return false;
	}
	for (idx_t child_index = 0; child_index < children.size(); child_index++) {
		if (!children[child_index] || !arguments[child_index].IsComplete() ||
		    children[child_index]->GetReturnType() != arguments[child_index]) {
			return false;
		}
	}
	return true;
}

static bool ChildrenAreConsistentWithArguments(const vector<unique_ptr<Expression>> &children,
                                               const vector<LogicalType> &arguments) {
	if (children.size() != arguments.size()) {
		return false;
	}
	for (idx_t child_index = 0; child_index < children.size(); child_index++) {
		if (!children[child_index] || !children[child_index]->GetReturnType().IsComplete() ||
		    (arguments[child_index].IsComplete() && children[child_index]->GetReturnType() != arguments[child_index])) {
			return false;
		}
	}
	return true;
}

template <class FUNCTION>
static LogicalPlanCompilerFunctionIdentity FunctionIdentity(const FUNCTION &function,
                                                            const vector<unique_ptr<Expression>> &children,
                                                            const LogicalType &return_type) {
	auto &definition = function.GetDefinition();
	LogicalPlanCompilerFunctionIdentity identity;
	if (definition) {
		identity.catalog = definition->GetCatalogName().GetIdentifierName();
		identity.schema = definition->GetSchemaName().GetIdentifierName();
		identity.name = definition->GetName().GetIdentifierName();
	} else {
		identity.name = function.GetName().GetIdentifierName();
	}
	identity.arguments = function.GetArguments();
	for (idx_t argument_index = 0; argument_index < identity.arguments.size() && argument_index < children.size();
	     argument_index++) {
		if (!identity.arguments[argument_index].IsComplete() && children[argument_index]) {
			identity.arguments[argument_index] = children[argument_index]->GetReturnType();
		}
	}
	identity.return_type = return_type;
	return identity;
}

template <class FUNCTION>
static bool HasQualifiedDefinition(const FUNCTION &function) {
	auto &definition = function.GetDefinition();
	return definition && IsValidIdentifier(definition->GetCatalogName()) &&
	       IsValidIdentifier(definition->GetSchemaName()) && IsValidIdentifier(definition->GetName());
}

class BoundExpressionSQLExportState {
public:
	explicit BoundExpressionSQLExportState(const BoundExpressionSQLExportContext &context_p) : context(context_p) {
	}

	BoundExpressionSQLExportResult Export(const Expression &expression, const LogicalPlanCompilerPath &path) {
		switch (expression.GetExpressionClass()) {
		case ExpressionClass::BOUND_CONSTANT:
			return ExportConstant(expression.Cast<BoundConstantExpression>(), path);
		case ExpressionClass::BOUND_COLUMN_REF:
			return ExportColumnRef(expression.Cast<BoundColumnRefExpression>(), path);
		case ExpressionClass::BOUND_FUNCTION:
			return ExportFunction(expression.Cast<BoundFunctionExpression>(), path);
		case ExpressionClass::BOUND_CONJUNCTION:
			return ExportConjunction(expression.Cast<BoundConjunctionExpression>(), path);
		case ExpressionClass::BOUND_CASE:
			return ExportCase(expression.Cast<BoundCaseExpression>(), path);
		case ExpressionClass::BOUND_OPERATOR:
			return ExportOperator(expression.Cast<BoundOperatorExpression>(), path);
		case ExpressionClass::BOUND_AGGREGATE:
			return ExportAggregate(expression.Cast<BoundAggregateExpression>(), path);
		case ExpressionClass::BOUND_DEFAULT:
		case ExpressionClass::BOUND_PARAMETER:
		case ExpressionClass::BOUND_REF:
		case ExpressionClass::BOUND_SUBQUERY:
		case ExpressionClass::BOUND_WINDOW:
		case ExpressionClass::BOUND_UNNEST:
		case ExpressionClass::BOUND_LAMBDA:
		case ExpressionClass::BOUND_LAMBDA_REF:
		case ExpressionClass::LEGACY_BOUND_CAST:
		case ExpressionClass::LEGACY_BOUND_COMPARISON:
		case ExpressionClass::LEGACY_BOUND_BETWEEN:
			return Failure(UnsupportedExpression(path, expression.GetExpressionClass()));
		case ExpressionClass::BOUND_EXPANDED:
		case ExpressionClass::BOUND_EXPRESSION:
		case ExpressionClass::AGGREGATE:
		case ExpressionClass::CASE:
		case ExpressionClass::CAST:
		case ExpressionClass::COLUMN_REF:
		case ExpressionClass::COMPARISON:
		case ExpressionClass::CONJUNCTION:
		case ExpressionClass::CONSTANT:
		case ExpressionClass::DEFAULT:
		case ExpressionClass::FUNCTION:
		case ExpressionClass::OPERATOR:
		case ExpressionClass::STAR:
		case ExpressionClass::SUBQUERY:
		case ExpressionClass::WINDOW:
		case ExpressionClass::PARAMETER:
		case ExpressionClass::COLLATE:
		case ExpressionClass::LAMBDA:
		case ExpressionClass::POSITIONAL_REFERENCE:
		case ExpressionClass::BETWEEN:
		case ExpressionClass::LAMBDA_REF:
		case ExpressionClass::TYPE:
			return Failure(
			    InternalExpressionInvariant(path, expression, "Expression export requires a final bound class"));
		case ExpressionClass::INVALID:
			return Failure(InternalInvariant(path, "Expression export received an invalid expression class"));
		default:
			return Failure(InternalInvariant(path, "Expression export received an unknown expression class"));
		}
	}

private:
	BoundExpressionSQLExportResult ExportConstant(const BoundConstantExpression &expression,
	                                              const LogicalPlanCompilerPath &path) {
		auto &return_type = expression.GetReturnType();
		auto &value = expression.GetValue();
		if (!IsSQLRepresentableType(return_type) || !IsSQLRepresentableType(value.type())) {
			return Failure(InternalExpressionInvariant(path, expression, "Bound constant has an unexportable type"));
		}
		if (return_type != value.type()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound constant value and return types differ"));
		}
		unique_ptr<ParsedExpression> result = make_uniq<ConstantExpression>(value);
		if (return_type.id() != LogicalTypeId::SQLNULL) {
			result = make_uniq<CastExpression>(return_type, std::move(result));
		}
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

	BoundExpressionSQLExportResult ExportColumnRef(const BoundColumnRefExpression &expression,
	                                               const LogicalPlanCompilerPath &path) {
		auto &binding = expression.Binding();
		if (!binding.table_index.IsValid() || !binding.column_index.IsValid()) {
			return Failure(InvalidBinding(path, binding, "Bound column reference has an incomplete binding"));
		}
		if (!IsSQLRepresentableType(expression.GetReturnType())) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound column reference has an incomplete type"));
		}
		if (expression.Depth() != 0) {
			auto issue = UnsupportedFeature(path, "correlated_column_reference",
			                                "Correlated column references require an owning query export context");
			issue.facts.emplace_back("depth", Value::UBIGINT(expression.Depth()));
			return Failure(std::move(issue));
		}
		if (!context.resolve_binding) {
			return Failure(InvalidBinding(path, binding, "No SQL column binding resolver was provided"));
		}
		auto resolved = context.resolve_binding(binding);
		if (!resolved) {
			return Failure(InvalidBinding(path, binding, "The SQL column binding resolver has no matching entry"));
		}
		if (resolved->names.empty()) {
			return Failure(InvalidBinding(path, binding, "The resolved SQL column name is empty"));
		}
		for (auto &name : resolved->names) {
			if (!IsValidIdentifier(name)) {
				return Failure(
				    InvalidBinding(path, binding, "The resolved SQL column name contains an invalid identifier"));
			}
		}
		if (!IsSQLRepresentableType(resolved->type)) {
			return Failure(InternalExpressionInvariant(path, expression, "The resolved SQL column type is incomplete"));
		}
		if (resolved->type != expression.GetReturnType()) {
			LogicalPlanCompilerIssue issue;
			issue.code = LogicalPlanCompilerIssueCode::TYPE_MISMATCH;
			issue.phase = LogicalPlanCompilerPhase::EXPRESSION_EXPORT;
			issue.path = path;
			issue.construct =
			    LogicalPlanCompilerConstructIdentity::BindingTypeMismatch(resolved->type, expression.GetReturnType());
			issue.message = "The resolved SQL column type differs from the bound expression type";
			return Failure(std::move(issue));
		}
		return BoundExpressionSQLExportResult::Success(make_uniq<ColumnRefExpression>(std::move(resolved->names)));
	}

	LogicalPlanCompilerIssue InvalidBinding(const LogicalPlanCompilerPath &path, const ColumnBinding &binding,
	                                        string message) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::INVALID_BINDING;
		issue.phase = LogicalPlanCompilerPhase::EXPRESSION_EXPORT;
		issue.path = path;
		issue.facts.emplace_back("column_index", Value::UBIGINT(binding.column_index.GetIndexUnsafe()));
		issue.facts.emplace_back("table_index", Value::UBIGINT(binding.table_index.index));
		issue.message = std::move(message);
		return issue;
	}

	BoundExpressionSQLExportResult ExportFunction(const BoundFunctionExpression &expression,
	                                              const LogicalPlanCompilerPath &path) {
		if (BoundCastExpression::IsCast(expression)) {
			return ExportCast(expression, path);
		}
		if (BoundComparisonExpression::IsComparison(expression)) {
			return ExportComparison(expression, path);
		}
		if (expression.GetExpressionType() == ExpressionType::COMPARE_BETWEEN) {
			return ExportBetween(expression, path);
		}
		return ExportScalarFunction(expression, path);
	}

	BoundExpressionSQLExportResult ExportCast(const BoundFunctionExpression &expression,
	                                          const LogicalPlanCompilerPath &path) {
		if (expression.GetChildren().size() != 1 || !expression.GetChildren()[0] || !expression.BindInfo() ||
		    !IsSQLRepresentableType(expression.GetReturnType()) ||
		    !IsSQLRepresentableType(expression.GetChildren()[0]->GetReturnType())) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound cast has malformed type, data, or arity"));
		}
		if (BoundCastExpression::IsDefaultCast(expression)) {
			return Failure(UnsupportedFeature(path, "default_cast_binding",
			                                  "A default-only bound cast cannot be reconstructed through SQL binding"));
		}
		auto child = ExportChild(*expression.GetChildren()[0], path, 0);
		if (child.HasError()) {
			return child;
		}
		return BoundExpressionSQLExportResult::Success(make_uniq<CastExpression>(
		    expression.GetReturnType(), std::move(child.GetValue()), BoundCastExpression::IsTryCast(expression)));
	}

	BoundExpressionSQLExportResult ExportComparison(const BoundFunctionExpression &expression,
	                                                const LogicalPlanCompilerPath &path) {
		if (expression.GetReturnType() != LogicalType::BOOLEAN || expression.GetChildren().size() != 2 ||
		    !expression.GetChildren()[0] || !expression.GetChildren()[1] ||
		    expression.GetChildren()[0]->GetReturnType() != expression.GetChildren()[1]->GetReturnType()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound comparison has malformed type or arity"));
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanCompilerIssue> issues;
		ExportChildren(expression.GetChildren(), path, children, issues);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		return BoundExpressionSQLExportResult::Success(make_uniq<ComparisonExpression>(
		    expression.GetExpressionType(), std::move(children[0]), std::move(children[1])));
	}

	BoundExpressionSQLExportResult ExportBetween(const BoundFunctionExpression &expression,
	                                             const LogicalPlanCompilerPath &path) {
		if (expression.GetReturnType() != LogicalType::BOOLEAN || expression.GetChildren().size() != 3 ||
		    !expression.GetChildren()[0] || !expression.GetChildren()[1] || !expression.GetChildren()[2] ||
		    !expression.BindInfo() ||
		    expression.GetChildren()[0]->GetReturnType() != expression.GetChildren()[1]->GetReturnType() ||
		    expression.GetChildren()[0]->GetReturnType() != expression.GetChildren()[2]->GetReturnType()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound BETWEEN has malformed type, data, or arity"));
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanCompilerIssue> issues;
		ExportChildren(expression.GetChildren(), path, children, issues);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		auto lower_inclusive = BoundBetweenExpression::LowerInclusive(expression);
		auto upper_inclusive = BoundBetweenExpression::UpperInclusive(expression);
		if (lower_inclusive && upper_inclusive) {
			return BoundExpressionSQLExportResult::Success(
			    make_uniq<BetweenExpression>(std::move(children[0]), std::move(children[1]), std::move(children[2])));
		}
		if (expression.GetChildren()[0]->IsVolatile()) {
			return Failure(UnsupportedFeature(
			    path, "exclusive_between_input_evaluation",
			    "An exclusive BETWEEN cannot duplicate a volatile input while preserving evaluation semantics"));
		}
		auto lower = make_uniq<ComparisonExpression>(BoundBetweenExpression::LowerComparisonType(expression),
		                                             children[0]->Copy(), std::move(children[1]));
		auto upper = make_uniq<ComparisonExpression>(BoundBetweenExpression::UpperComparisonType(expression),
		                                             std::move(children[0]), std::move(children[2]));
		return BoundExpressionSQLExportResult::Success(
		    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(lower), std::move(upper)));
	}

	BoundExpressionSQLExportResult ExportConjunction(const BoundConjunctionExpression &expression,
	                                                 const LogicalPlanCompilerPath &path) {
		if ((expression.GetExpressionType() != ExpressionType::CONJUNCTION_AND &&
		     expression.GetExpressionType() != ExpressionType::CONJUNCTION_OR) ||
		    expression.GetReturnType() != LogicalType::BOOLEAN || expression.GetChildren().size() < 2) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound conjunction has malformed type or arity"));
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanCompilerIssue> issues;
		ExportChildren(expression.GetChildren(), path, children, issues, LogicalType::BOOLEAN);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		auto result = make_uniq<ConjunctionExpression>(expression.GetExpressionType());
		result->GetChildrenMutable() = std::move(children);
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

	BoundExpressionSQLExportResult ExportCase(const BoundCaseExpression &expression,
	                                          const LogicalPlanCompilerPath &path) {
		if (!IsSQLRepresentableType(expression.GetReturnType()) || expression.CaseChecks().empty()) {
			return Failure(InternalExpressionInvariant(path, expression, "Bound CASE has malformed type or arity"));
		}
		vector<optional_ptr<const Expression>> source_children;
		vector<optional<LogicalType>> expected_types;
		for (auto &check : expression.CaseChecks()) {
			source_children.push_back(check.when_expr.get());
			expected_types.push_back(LogicalType::BOOLEAN);
			source_children.push_back(check.then_expr.get());
			expected_types.push_back(expression.GetReturnType());
		}
		source_children.push_back(expression.ElseExpression().get());
		expected_types.push_back(expression.GetReturnType());

		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanCompilerIssue> issues;
		ExportChildren(source_children, path, children, issues, expected_types);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		auto result = make_uniq<CaseExpression>();
		for (idx_t check_index = 0; check_index < expression.CaseChecks().size(); check_index++) {
			CaseCheck check;
			check.when_expr = std::move(children[check_index * 2]);
			check.then_expr = std::move(children[check_index * 2 + 1]);
			result->CaseChecksMutable().push_back(std::move(check));
		}
		result->ElseMutable() = std::move(children.back());
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

	BoundExpressionSQLExportResult ExportOperator(const BoundOperatorExpression &expression,
	                                              const LogicalPlanCompilerPath &path) {
		optional<LogicalType> expected_type;
		auto child_count = expression.GetChildren().size();
		switch (expression.GetExpressionType()) {
		case ExpressionType::OPERATOR_NOT:
			if (child_count != 1 || expression.GetReturnType() != LogicalType::BOOLEAN) {
				return Failure(InternalExpressionInvariant(path, expression, "Bound NOT has malformed type or arity"));
			}
			expected_type = LogicalType::BOOLEAN;
			break;
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			if (child_count != 1 || expression.GetReturnType() != LogicalType::BOOLEAN) {
				return Failure(
				    InternalExpressionInvariant(path, expression, "Bound NULL test has malformed type or arity"));
			}
			break;
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			if (child_count < 2 || expression.GetReturnType() != LogicalType::BOOLEAN || !expression.GetChildren()[0]) {
				return Failure(InternalExpressionInvariant(path, expression, "Bound IN has malformed type or arity"));
			}
			expected_type = expression.GetChildren()[0]->GetReturnType();
			break;
		case ExpressionType::OPERATOR_COALESCE:
			if (child_count < 2 || !IsSQLRepresentableType(expression.GetReturnType())) {
				return Failure(
				    InternalExpressionInvariant(path, expression, "Bound COALESCE has malformed type or arity"));
			}
			expected_type = expression.GetReturnType();
			break;
		case ExpressionType::OPERATOR_TRY:
			if (child_count != 1 || !IsSQLRepresentableType(expression.GetReturnType())) {
				return Failure(InternalExpressionInvariant(path, expression, "Bound TRY has malformed type or arity"));
			}
			expected_type = expression.GetReturnType();
			break;
		default:
			return Failure(
			    UnsupportedFeature(path, "bound_operator", "The bound operator has no admitted parsed SQL AST form"));
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanCompilerIssue> issues;
		ExportChildren(expression.GetChildren(), path, children, issues, expected_type);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		return BoundExpressionSQLExportResult::Success(
		    make_uniq<OperatorExpression>(expression.GetExpressionType(), std::move(children)));
	}

	BoundExpressionSQLExportResult ExportScalarFunction(const BoundFunctionExpression &expression,
	                                                    const LogicalPlanCompilerPath &path) {
		auto &function = expression.Function();
		if (!ChildrenAreConsistentWithArguments(expression.GetChildren(), function.GetArguments()) ||
		    expression.GetReturnType() != function.GetReturnType()) {
			return Failure(InternalExpressionInvariant(path, expression,
			                                           "Bound scalar function has an inconsistent current signature"));
		}
		auto identity = FunctionIdentity(expression.Function(), expression.GetChildren(), expression.GetReturnType());
		if (!identity.IsValid()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound scalar function identity is incomplete"));
		}
		auto &definition = function.GetDefinition();
		if (!definition || !HasQualifiedDefinition(function) || expression.BindInfo() ||
		    definition->GetCaptureArgumentAliases() ||
		    !DefinitionArityMatches(definition->GetSignature(), expression.GetChildren().size()) ||
		    !ChildrenMatchArguments(expression.GetChildren(), function.GetArguments()) ||
		    expression.GetReturnType() != function.GetReturnType() ||
		    !IsSQLRepresentableType(expression.GetReturnType()) ||
		    (expression.IsOperator() && expression.GetChildren().size() != 1 && expression.GetChildren().size() != 2)) {
			return Failure(UnsupportedFunction(
			    path, std::move(identity), "The bound scalar function cannot be rebound from positional SQL syntax"));
		}
		for (auto &argument : function.GetArguments()) {
			if (!IsSQLRepresentableType(argument)) {
				return Failure(UnsupportedFunction(path, std::move(identity),
				                                   "The bound scalar function uses an internal argument type"));
			}
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanCompilerIssue> issues;
		ExportChildren(expression.GetChildren(), path, children, issues);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		auto name = QualifiedName(definition->GetCatalogName(), definition->GetSchemaName(), definition->GetName());
		return BoundExpressionSQLExportResult::Success(make_uniq<FunctionExpression>(
		    name, std::move(children), nullptr, nullptr, false, expression.IsOperator(), false));
	}

	BoundExpressionSQLExportResult ExportAggregate(const BoundAggregateExpression &expression,
	                                               const LogicalPlanCompilerPath &path) {
		auto &function = expression.Function();
		if (!ChildrenAreConsistentWithArguments(expression.GetChildren(), function.GetArguments()) ||
		    expression.GetReturnType() != function.GetReturnType()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound aggregate has an inconsistent current signature"));
		}
		auto identity = FunctionIdentity(expression.Function(), expression.GetChildren(), expression.GetReturnType());
		if (!identity.IsValid()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound aggregate function identity is incomplete"));
		}
		auto &definition = function.GetDefinition();
		if (!definition || !HasQualifiedDefinition(function) || expression.BindInfo() ||
		    !DefinitionArityMatches(definition->GetSignature(), expression.GetChildren().size()) ||
		    !ChildrenMatchArguments(expression.GetChildren(), function.GetArguments()) ||
		    expression.GetReturnType() != function.GetReturnType() ||
		    !IsSQLRepresentableType(expression.GetReturnType())) {
			return Failure(UnsupportedFunction(path, std::move(identity),
			                                   "The bound aggregate cannot be rebound from positional SQL syntax"));
		}
		if (expression.GetAggregateType() != AggregateType::NON_DISTINCT &&
		    expression.GetAggregateType() != AggregateType::DISTINCT) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound aggregate has an invalid distinct mode"));
		}
		if (expression.StateExportMode() != AggregateStateExportMode::NONE &&
		    expression.StateExportMode() != AggregateStateExportMode::STATE_EXPORT) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound aggregate has an invalid state export mode"));
		}
		for (auto &argument : function.GetArguments()) {
			if (!IsSQLRepresentableType(argument)) {
				return Failure(UnsupportedFunction(path, std::move(identity),
				                                   "The bound aggregate uses an internal argument type"));
			}
		}
		vector<optional_ptr<const Expression>> source_children;
		vector<optional<LogicalType>> expected_types;
		for (idx_t child_index = 0; child_index < expression.GetChildren().size(); child_index++) {
			source_children.push_back(expression.GetChildren()[child_index].get());
			expected_types.push_back(function.GetArguments()[child_index]);
		}
		if (expression.GetFilter()) {
			source_children.push_back(expression.GetFilter().get());
			expected_types.push_back(LogicalType::BOOLEAN);
		}
		if (expression.GetOrderBys()) {
			for (auto &order : expression.GetOrderBys()->orders) {
				if ((order.type != OrderType::ASCENDING && order.type != OrderType::DESCENDING) ||
				    (order.null_order != OrderByNullType::NULLS_FIRST &&
				     order.null_order != OrderByNullType::NULLS_LAST)) {
					return Failure(
					    InternalExpressionInvariant(path, expression, "Bound aggregate has an invalid ordering mode"));
				}
				source_children.push_back(order.expression.get());
				expected_types.emplace_back();
			}
		}

		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanCompilerIssue> issues;
		ExportChildren(source_children, path, children, issues, expected_types);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
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
		auto name = QualifiedName(definition->GetCatalogName(), definition->GetSchemaName(), definition->GetName());
		return BoundExpressionSQLExportResult::Success(make_uniq<FunctionExpression>(
		    name, std::move(arguments), std::move(filter), std::move(order_bys), expression.IsDistinct(), false,
		    expression.StateExportMode() == AggregateStateExportMode::STATE_EXPORT));
	}

	BoundExpressionSQLExportResult ExportChild(const Expression &expression, const LogicalPlanCompilerPath &path,
	                                           idx_t child_index) {
		return Export(expression, ChildPath(path, child_index));
	}

	void ExportChildren(const vector<unique_ptr<Expression>> &source, const LogicalPlanCompilerPath &path,
	                    vector<unique_ptr<ParsedExpression>> &result, vector<LogicalPlanCompilerIssue> &issues,
	                    optional<LogicalType> expected_type = {}) {
		vector<optional_ptr<const Expression>> source_refs;
		vector<optional<LogicalType>> expected_types;
		for (auto &child : source) {
			source_refs.push_back(child.get());
			expected_types.push_back(expected_type);
		}
		ExportChildren(source_refs, path, result, issues, expected_types);
	}

	void ExportChildren(const vector<optional_ptr<const Expression>> &source, const LogicalPlanCompilerPath &path,
	                    vector<unique_ptr<ParsedExpression>> &result, vector<LogicalPlanCompilerIssue> &issues,
	                    const vector<optional<LogicalType>> &expected_types) {
		D_ASSERT(source.size() == expected_types.size());
		result.resize(source.size());
		for (idx_t child_index = 0; child_index < source.size(); child_index++) {
			auto child_path = ChildPath(path, child_index);
			if (!source[child_index]) {
				issues.push_back(InternalInvariant(child_path, "Bound expression has a null child"));
				continue;
			}
			if (expected_types[child_index] && source[child_index]->GetReturnType() != *expected_types[child_index]) {
				issues.push_back(InternalExpressionInvariant(child_path, *source[child_index],
				                                             "Bound expression child has an unexpected type"));
				continue;
			}
			auto child = Export(*source[child_index], child_path);
			if (child.HasError()) {
				for (auto &issue : child.GetIssues()) {
					issues.push_back(issue);
				}
			} else {
				result[child_index] = std::move(child.GetValue());
			}
		}
	}

private:
	const BoundExpressionSQLExportContext &context;
};

LogicalPlanCompilerResult<unique_ptr<ParsedExpression>>
BoundExpressionSQLExporter::Export(const Expression &expression, const BoundExpressionSQLExportContext &context) {
	LogicalPlanCompilerPath path;
	path.root = LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION;
	return ExportAtPath(expression, context, path);
}

LogicalPlanCompilerResult<unique_ptr<ParsedExpression>>
BoundExpressionSQLExporter::ExportAtPath(const Expression &expression, const BoundExpressionSQLExportContext &context,
                                         const LogicalPlanCompilerPath &path) {
	if (!IsExpressionRootPath(path)) {
		return Failure(InternalInvariant({}, "Expression export requires an already-valid expression root path"));
	}
	BoundExpressionSQLExportState state(context);
	return state.Export(expression, path);
}

} // namespace duckdb
