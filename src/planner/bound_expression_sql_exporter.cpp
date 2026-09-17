#include "duckdb/function/scalar/compressed_materialization_utils.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"

#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
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
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/scalar/generic_common.hpp"

#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

namespace duckdb {

using BoundExpressionSQLExportResult = LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>;
using BoundAggregateSQLExportResult = LogicalPlanVerificationResult<unique_ptr<FunctionExpression>>;

using SQLExportHelpers::ChildPath;
using SQLExportHelpers::IsSQLRepresentableType;
using SQLExportHelpers::IsSQLValueType;
using SQLExportHelpers::IsValidIdentifier;

static bool HasUnsupportedVariantKeys(const VariantNode &node) {
	// Struct literals require nonempty, case-insensitively unique keys; inspect before the lossy STRUCT conversion.
	if (node.GetTypeId() == VariantLogicalType::OBJECT) {
		identifier_set_t names;
		for (auto &child : node.GetObjectChildren()) {
			if (child.key.GetSize() == 0 || !names.insert(Identifier(child.key.GetString())).second ||
			    HasUnsupportedVariantKeys(child.value)) {
				return true;
			}
		}
	} else if (node.GetTypeId() == VariantLogicalType::ARRAY) {
		for (auto child : node.GetArrayChildren()) {
			if (HasUnsupportedVariantKeys(child)) {
				return true;
			}
		}
	}
	return false;
}

static bool HasUnsupportedVariantKeys(const Value &value) {
	if (value.IsNull()) {
		return false;
	}
	optional_ptr<const vector<Value>> children;
	switch (value.type().id()) {
	case LogicalTypeId::VARIANT: {
		Vector vector(value, count_t(1));
		VariantIterator iterator(vector);
		return HasUnsupportedVariantKeys(iterator.Root(0));
	}
	case LogicalTypeId::STRUCT:
		children = StructValue::GetChildren(value);
		break;
	case LogicalTypeId::LIST:
		children = ListValue::GetChildren(value);
		break;
	case LogicalTypeId::ARRAY:
		children = ArrayValue::GetChildren(value);
		break;
	case LogicalTypeId::MAP:
		children = MapValue::GetChildren(value);
		break;
	case LogicalTypeId::UNION:
		return HasUnsupportedVariantKeys(UnionValue::GetValue(value));
	default:
		return false;
	}
	for (auto &child : *children) {
		if (HasUnsupportedVariantKeys(child)) {
			return true;
		}
	}
	return false;
}

static inline bool IsExpressionRootPath(const LogicalPlanVerificationPath &path) {
	if (!path.IsValid()) {
		return false;
	}
	if (path.root == LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION) {
		return true;
	}
	for (auto &component : path.components) {
		if (component.type == LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION) {
			return true;
		}
	}
	return false;
}

static LogicalPlanVerificationIssue
InternalInvariant(optional<LogicalPlanVerificationPath> path, string message,
                  optional<LogicalPlanVerificationConstructIdentity> construct = {}) {
	return SQLExportHelpers::MakeIssue(LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT,
	                                   LogicalPlanVerificationPhase::EXPRESSION_EXPORT, std::move(path),
	                                   std::move(construct), std::move(message));
}

static LogicalPlanVerificationIssue InternalExpressionInvariant(const LogicalPlanVerificationPath &path,
                                                                const Expression &expression, string message) {
	return InternalInvariant(path, std::move(message),
	                         LogicalPlanVerificationConstructIdentity::Expression(expression.GetExpressionClass()));
}

static LogicalPlanVerificationIssue UnsupportedExpression(const LogicalPlanVerificationPath &path,
                                                          ExpressionClass expression_class) {
	return SQLExportHelpers::MakeIssue(
	    LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, path,
	    LogicalPlanVerificationConstructIdentity::Expression(expression_class),
	    "The bound expression class does not have a SQL AST representation in this exporter");
}

static LogicalPlanVerificationIssue UnsupportedFeature(const LogicalPlanVerificationPath &path, string feature,
                                                       string message) {
	return SQLExportHelpers::MakeIssue(
	    LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, LogicalPlanVerificationPhase::EXPRESSION_EXPORT,
	    path, LogicalPlanVerificationConstructIdentity::ExportFeature(std::move(feature)), std::move(message));
}

static LogicalPlanVerificationIssue UnsupportedFunction(const LogicalPlanVerificationPath &path,
                                                        LogicalPlanVerificationFunctionIdentity identity,
                                                        string message) {
	return SQLExportHelpers::MakeIssue(
	    LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, path,
	    LogicalPlanVerificationConstructIdentity::Function(std::move(identity)), std::move(message));
}

static BoundExpressionSQLExportResult Failure(LogicalPlanVerificationIssue issue) {
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(std::move(issue));
	return BoundExpressionSQLExportResult::Failure(std::move(issues));
}

static BoundAggregateSQLExportResult AggregateFailure(LogicalPlanVerificationIssue issue) {
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(std::move(issue));
	return BoundAggregateSQLExportResult::Failure(std::move(issues));
}

static bool HasNestedCollation(const LogicalType &type) {
	return type.id() != LogicalTypeId::VARCHAR && TypeVisitor::Contains(type, [](const LogicalType &child) {
		       return child.id() == LogicalTypeId::VARCHAR && !StringType::GetCollation(child).empty();
	       });
}

static BoundExpressionSQLExportResult PreserveCollation(const LogicalType &type, BoundExpressionSQLExportResult result,
                                                        const LogicalPlanVerificationPath &path) {
	if (result.HasError()) {
		return result;
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		auto collation = StringType::GetCollation(type);
		if (!collation.empty()) {
			result.GetValue() = make_uniq<CollateExpression>(std::move(collation), std::move(result.GetValue()));
		}
	} else if (HasNestedCollation(type)) {
		auto issue = UnsupportedFeature(path, "nested_result_collation",
		                                "Nested result collations require a typed SQL representation");
		issue.facts.emplace_back("logical_type", Value(type.ToString()));
		issue.facts.emplace_back("varchar_collations", Value(SQLExportHelpers::TypeCollationSignature(type)));
		return Failure(std::move(issue));
	}
	return result;
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
static LogicalPlanVerificationFunctionIdentity DefinitionFunctionIdentity(const FUNCTION &definition,
                                                                          const vector<LogicalType> &arguments,
                                                                          const LogicalType &return_type) {
	LogicalPlanVerificationFunctionIdentity identity;
	identity.catalog = definition.GetCatalogName().GetIdentifierName();
	identity.schema = definition.GetSchemaName().GetIdentifierName();
	identity.name = definition.GetName().GetIdentifierName();
	identity.arguments = arguments;
	identity.return_type = return_type;
	return identity;
}

template <class FUNCTION>
static optional<QualifiedName> RebindableFunctionName(const FUNCTION &definition) {
	auto name =
	    QualifiedName(definition.GetCatalogName().empty() ? Identifier::SystemCatalog() : definition.GetCatalogName(),
	                  definition.GetSchemaName().empty() ? Identifier::DefaultSchema() : definition.GetSchemaName(),
	                  definition.GetName());
	if (name.Path().empty()) {
		return {};
	}
	for (auto &component : name.Path()) {
		if (!IsValidIdentifier(component)) {
			return {};
		}
	}
	return name;
}

template <class FUNCTION>
static bool IsOptimizerFunctionQualification(const FUNCTION &function) {
	if (function.GetCatalogName().empty() && function.GetSchemaName().empty()) {
		return true;
	}
	return function.GetCatalogName() == "system" && function.GetSchemaName() == "main";
}

static LogicalType SQLCastType(const LogicalType &type) {
	// Scalar collations are applied by COLLATE, outside the cast's type expression.
	return type.id() == LogicalTypeId::VARCHAR && !type.HasAlias() ? LogicalType::VARCHAR : type;
}

static unique_ptr<ParsedExpression> SQLCast(const LogicalType &type, unique_ptr<ParsedExpression> child,
                                            bool try_cast = false) {
	return make_uniq<CastExpression>(SQLCastType(type), std::move(child), try_cast);
}

static unique_ptr<ParsedExpression> SystemFunction(const string &name, vector<unique_ptr<ParsedExpression>> arguments) {
	return make_uniq<FunctionExpression>(QualifiedName("system", "main", Identifier(name)), std::move(arguments));
}

static unique_ptr<ParsedExpression> UnarySystemFunction(const string &name, unique_ptr<ParsedExpression> argument) {
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(std::move(argument));
	return SystemFunction(name, std::move(arguments));
}

static unique_ptr<ParsedExpression> BinarySystemFunction(const string &name, unique_ptr<ParsedExpression> left,
                                                         unique_ptr<ParsedExpression> right) {
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(std::move(left));
	arguments.push_back(std::move(right));
	return SystemFunction(name, std::move(arguments));
}

static unique_ptr<ParsedExpression> IntervalSQLConstant(const interval_t &value) {
	auto months = UnarySystemFunction("to_months", ConstantExpression::FromValue(Value::INTEGER(value.months)));
	auto days = UnarySystemFunction("to_days", ConstantExpression::FromValue(Value::INTEGER(value.days)));
	auto micros = UnarySystemFunction("to_microseconds", ConstantExpression::FromValue(Value::BIGINT(value.micros)));
	return BinarySystemFunction("add", BinarySystemFunction("add", std::move(months), std::move(days)),
	                            std::move(micros));
}

class BoundExpressionSQLExportState {
	struct ChildExpression {
		ChildExpression(optional_ptr<const Expression> expression_p, optional<LogicalType> expected_type_p = {})
		    : expression(expression_p), expected_type(std::move(expected_type_p)) {
		}

		optional_ptr<const Expression> expression;
		optional<LogicalType> expected_type;
	};

public:
	explicit BoundExpressionSQLExportState(const BoundExpressionSQLExportContext &context_p) : context(context_p) {
	}

	BoundExpressionSQLExportResult Export(const Expression &expression, const LogicalPlanVerificationPath &path) {
		auto result = ExportInternal(expression, path);
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF ||
		    ((expression.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT ||
		      expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) &&
		     expression.GetReturnType().id() != LogicalTypeId::VARCHAR)) {
			return result;
		}
		return PreserveCollation(expression.GetReturnType(), std::move(result), path);
	}

private:
	BoundExpressionSQLExportResult ExportInternal(const Expression &expression,
	                                              const LogicalPlanVerificationPath &path) {
		switch (expression.GetExpressionClass()) {
		case ExpressionClass::BOUND_CONSTANT:
			return ExportConstant(expression.Cast<BoundConstantExpression>(), path);
		case ExpressionClass::BOUND_COLUMN_REF:
			return ExportColumnRef(expression.Cast<BoundColumnRefExpression>(), path);
		case ExpressionClass::BOUND_REF:
			return ExportReference(expression.Cast<BoundReferenceExpression>(), path);
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
		}
		return Failure(InternalInvariant(path, "Expression export received an unknown expression class"));
	}

public:
	BoundExpressionSQLExportResult ExportWindow(const BoundWindowExpression &expression,
	                                            const LogicalPlanVerificationPath &path) {
		if (expression.AggregateFunction()) {
			return PreserveCollation(expression.GetReturnType(),
			                         ExportWindowFunction(expression, *expression.AggregateFunction(), path), path);
		}
		D_ASSERT(expression.WindowFunction());
		return PreserveCollation(expression.GetReturnType(),
		                         ExportWindowFunction(expression, *expression.WindowFunction(), path), path);
	}

	BoundExpressionSQLExportResult ExportUnnest(const BoundUnnestExpression &expression,
	                                            const LogicalPlanVerificationPath &path) {
		D_ASSERT(expression.Child());
		auto child = ExportChild(*expression.Child(), path, 0);
		if (child.HasError()) {
			return child;
		}
		vector<unique_ptr<ParsedExpression>> arguments;
		arguments.push_back(std::move(child.GetValue()));
		return PreserveCollation(
		    expression.GetReturnType(),
		    BoundExpressionSQLExportResult::Success(make_uniq<FunctionExpression>("unnest", std::move(arguments))),
		    path);
	}

private:
	template <class FUNCTION>
	BoundExpressionSQLExportResult ExportWindowFunction(const BoundWindowExpression &expression,
	                                                    const FUNCTION &function,
	                                                    const LogicalPlanVerificationPath &path) {
		auto &definition = function.GetDefinition();
		if (!definition) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound window function has no retained definition"));
		}
		auto identity =
		    DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(), function.GetLogicalReturnType());
		if (!identity.IsValid()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound window function identity is incomplete"));
		}
		if (expression.GetChildren().size() != function.GetLogicalArguments().size()) {
			return Failure(UnsupportedFunction(path, std::move(identity),
			                                   "The window function does not retain every SQL argument"));
		}
		auto name = RebindableFunctionName(*definition);
		if (!name || !IsSQLValueType(expression.GetReturnType()) ||
		    definition->GetProperties().GetCaptureArgumentAliases()) {
			return Failure(UnsupportedFunction(path, std::move(identity),
			                                   "The window function no longer represents its logical SQL signature"));
		}
		auto is_range_offset = [](WindowBoundary boundary) {
			return boundary == WindowBoundary::EXPR_PRECEDING_RANGE || boundary == WindowBoundary::EXPR_FOLLOWING_RANGE;
		};
		optional_ptr<const Expression> sql_order;
		if (expression.OrderBy().size() == 1) {
			sql_order = expression.OrderBy()[0].expression.get();
			if (sql_order && expression.SQLRangeOrderType().IsComplete() &&
			    !sql_order->GetReturnType().EqualsWithCollation(expression.SQLRangeOrderType()) &&
			    BoundCastExpression::IsCast(*sql_order)) {
				auto &cast = sql_order->Cast<BoundFunctionExpression>();
				if (BoundCastExpression::HasValidBindData(cast) && !BoundCastExpression::IsTryCast(cast) &&
				    cast.GetChildren().size() == 1 && cast.GetChildren()[0] &&
				    cast.GetChildren()[0]->GetReturnType().EqualsWithCollation(expression.SQLRangeOrderType())) {
					sql_order = cast.GetChildren()[0].get();
				}
			}
		}
		auto range_offset = [&](const unique_ptr<Expression> &endpoint, WindowBoundary boundary,
		                        const unique_ptr<Expression> &literal) -> optional_ptr<const Expression> {
			if (!is_range_offset(boundary)) {
				return endpoint.get();
			}
			if (endpoint && sql_order && literal && literal->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
			    Expression::Equals(*endpoint, *expression.OrderBy()[0].expression)) {
				auto &value = literal->Cast<BoundConstantExpression>().GetValue();
				if (!value.IsNull() && value.type().IsNumeric() && value == Value::Numeric(value.type(), 0)) {
					return literal.get();
				}
			}
			if (!endpoint || expression.OrderBy().size() != 1 ||
			    endpoint->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
				return nullptr;
			}
			auto &arithmetic = endpoint->Cast<BoundFunctionExpression>();
			auto &order = expression.OrderBy()[0];
			const bool subtract =
			    (boundary == WindowBoundary::EXPR_PRECEDING_RANGE) == (order.type == OrderType::ASCENDING);
			auto definition = arithmetic.Function().GetDefinition();
			auto arithmetic_name = definition ? RebindableFunctionName(*definition) : optional<QualifiedName>();
			if (!arithmetic_name || *arithmetic_name != QualifiedName("system", "main", subtract ? "-" : "+") ||
			    arithmetic.GetChildren().size() != 2 || !arithmetic.GetChildren()[0] || !arithmetic.GetChildren()[1] ||
			    !order.expression || (order.type != OrderType::ASCENDING && order.type != OrderType::DESCENDING) ||
			    endpoint->GetReturnType() != order.expression->GetReturnType() || !sql_order ||
			    (!Expression::Equals(*arithmetic.GetChildren()[0], *sql_order) &&
			     !Expression::Equals(*arithmetic.GetChildren()[0], *order.expression))) {
				return nullptr;
			}
			auto &offset = *arithmetic.GetChildren()[1];
			if (offset.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
			    offset.Cast<BoundConstantExpression>().GetValue().IsNull()) {
				return nullptr;
			}
			if (literal && literal->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
			    offset.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				auto &original = literal->Cast<BoundConstantExpression>().GetValue();
				auto &current = offset.Cast<BoundConstantExpression>().GetValue();
				if (!original.IsNull() && original.type().IsNumeric() && current.type().IsNumeric() &&
				    original == current) {
					return literal.get();
				}
			}
			return &offset;
		};
		std::function<optional<Value>(const ParsedExpression &)> numeric_literal =
		    [&](const ParsedExpression &literal) -> optional<Value> {
			if (literal.GetExpressionClass() == ExpressionClass::CONSTANT) {
				auto value = literal.Cast<ConstantExpression>().GetLiteral().ToValue();
				return value.type().IsNumeric() && !value.IsNull() ? optional<Value>(value) : optional<Value>();
			}
			if (literal.GetExpressionClass() == ExpressionClass::CAST) {
				auto &cast = literal.Cast<CastExpression>();
				auto type = UnboundType::TryDefaultBind(cast.TargetType());
				auto child = numeric_literal(cast.Child());
				if (child && type.IsNumeric() && !cast.IsTryCast()) {
					return child->DefaultTryCastAs(type);
				}
			}
			return {};
		};
		auto retained_offset = [&](const unique_ptr<ParsedExpression> &literal) -> unique_ptr<Expression> {
			auto value = literal ? numeric_literal(*literal) : optional<Value>();
			return value ? make_uniq<BoundConstantExpression>(*value) : nullptr;
		};
		auto start_literal = retained_offset(expression.SQLRangeStart());
		auto end_literal = retained_offset(expression.SQLRangeEnd());
		auto start = range_offset(expression.StartExpr(), expression.WindowStart(), start_literal);
		auto end = range_offset(expression.EndExpr(), expression.WindowEnd(), end_literal);
		if ((expression.StartExpr() && !start) || (expression.EndExpr() && !end)) {
			return Failure(UnsupportedFeature(
			    path, "window_range_offset", "The RANGE endpoint does not retain its SQL offset and ordering operand"));
		}

		vector<ChildExpression> source_children;
		for (auto &partition : expression.Partitions()) {
			source_children.emplace_back(partition.get());
		}
		for (auto &order : expression.OrderBy()) {
			source_children.emplace_back(sql_order ? sql_order : order.expression.get());
		}
		for (idx_t i = 0; i < expression.GetChildren().size(); i++) {
			source_children.emplace_back(expression.GetChildren()[i].get());
		}
		if (expression.Filter()) {
			source_children.emplace_back(expression.Filter().get(), LogicalType::BOOLEAN);
		}
		if (expression.StartExpr()) {
			source_children.emplace_back(start);
		}
		if (expression.EndExpr()) {
			source_children.emplace_back(end);
		}
		for (auto &order : expression.ArgOrders()) {
			source_children.emplace_back(order.expression.get());
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanVerificationIssue> issues;
		ExportChildren(source_children, path, children, issues);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		auto window = make_uniq<WindowExpression>(name->Catalog().GetIdentifierName(),
		                                          name->Schema().GetIdentifierName(), name->Name().GetIdentifierName());
		window->SetQualifiedName(*name);
		idx_t ordinal = 0;
		for (idx_t i = 0; i < expression.Partitions().size(); i++) {
			window->PartitionsMutable().push_back(std::move(children[ordinal++]));
		}
		for (auto &order : expression.OrderBy()) {
			window->OrderByMutable().emplace_back(order.type, order.null_order, std::move(children[ordinal++]));
		}
		for (idx_t i = 0; i < expression.GetChildren().size(); i++) {
			window->GetArgumentsMutable().emplace_back(std::move(children[ordinal++]));
		}
		if (expression.Filter()) {
			window->FilterMutable() = std::move(children[ordinal++]);
		}
		if (expression.StartExpr()) {
			window->StartExprMutable() = std::move(children[ordinal++]);
		}
		if (expression.EndExpr()) {
			window->EndExprMutable() = std::move(children[ordinal++]);
		}
		for (auto &order : expression.ArgOrders()) {
			window->ArgOrdersMutable().emplace_back(order.type, order.null_order, std::move(children[ordinal++]));
		}
		window->IgnoreNullsMutable() = expression.IgnoreNulls();
		window->HasIgnoreNullsMutable() = expression.IgnoreNulls();
		window->DistinctMutable() = expression.Distinct();
		window->WindowStartMutable() = expression.WindowStart();
		window->WindowEndMutable() = expression.WindowEnd();
		window->WindowExcludeMutable() = expression.WindowExclude();
		unique_ptr<ParsedExpression> result = std::move(window);
		if (IsSQLRepresentableType(expression.GetReturnType()) && definition->HasBindCallback() &&
		    definition->GetReturnType() != expression.GetReturnType()) {
			result = SQLCast(expression.GetReturnType(), std::move(result));
		}
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

	static bool RequiresConstantConstructor(const LogicalType &type) {
		if (HasNestedCollation(type)) {
			return true;
		}
		return TypeVisitor::Contains(type, [](const LogicalType &child) {
			return child.IsAggregateState() || child.id() == LogicalTypeId::TYPE ||
			       (child.id() == LogicalTypeId::GEOMETRY && GeoType::HasCRS(child));
		});
	}

	BoundExpressionSQLExportResult ExportNestedConstant(const LogicalType &type, optional_ptr<const Value> value,
	                                                    const LogicalPlanVerificationPath &path) {
		if (type.id() == LogicalTypeId::TYPE) {
			if (!value || value->IsNull()) {
				return BoundExpressionSQLExportResult::Success(SQLCast(type, ConstantExpression::FromValue(Value())));
			}
			auto witness = ExportNestedConstant(TypeValue::GetType(*value), nullptr, path);
			if (witness.HasError()) {
				return witness;
			}
			return BoundExpressionSQLExportResult::Success(
			    UnarySystemFunction("get_type", std::move(witness.GetValue())));
		}
		if (type.IsAggregateState() || type.id() == LogicalTypeId::GEOMETRY ||
		    (IsSQLRepresentableType(type) && !RequiresConstantConstructor(type))) {
			auto constant = BoundConstantExpression(value ? *value : Value(type));
			constant.SetReturnType(type);
			return Export(constant, path);
		}
		const bool empty_list =
		    value && !value->IsNull() && type.id() == LogicalTypeId::LIST && ListValue::GetChildren(*value).empty();
		if (value && (value->IsNull() || empty_list)) {
			auto witness = ExportNestedConstant(type, nullptr, path);
			if (witness.HasError()) {
				return witness;
			}
			auto result = make_uniq<CaseExpression>();
			result->CaseChecksMutable().push_back(
			    {ConstantExpression::FromValue(Value::BOOLEAN(false)), std::move(witness.GetValue())});
			result->ElseMutable() =
			    ConstantExpression::FromValue(empty_list ? Value::LIST(LogicalType::SQLNULL, {}) : Value());
			return BoundExpressionSQLExportResult::Success(std::move(result));
		}
		if (type.id() == LogicalTypeId::MAP) {
			vector<Value> keys, values;
			if (value) {
				for (auto &entry : MapValue::GetChildren(*value)) {
					auto &children = StructValue::GetChildren(entry);
					keys.push_back(children[0]);
					values.push_back(children[1]);
				}
			}
			auto key_list = Value::LIST(MapType::KeyType(type), std::move(keys));
			auto value_list = Value::LIST(MapType::ValueType(type), std::move(values));
			auto left = ExportNestedConstant(key_list.type(), key_list, path);
			if (left.HasError()) {
				return left;
			}
			auto right = ExportNestedConstant(value_list.type(), value_list, path);
			if (right.HasError()) {
				return right;
			}
			auto result = BinarySystemFunction("map", std::move(left.GetValue()), std::move(right.GetValue()));
			if (type.HasAlias()) {
				result = SQLCast(type, std::move(result));
			}
			return BoundExpressionSQLExportResult::Success(std::move(result));
		}
		vector<FunctionArgument> arguments;
		child_list_t<LogicalType> child_types;
		optional_ptr<const vector<Value>> children;
		string function;
		switch (type.id()) {
		case LogicalTypeId::TUPLE:
		case LogicalTypeId::STRUCT:
			function = type.id() == LogicalTypeId::TUPLE || StructType::IsUnnamed(type) ? "row" : "struct_pack";
			child_types = StructType::GetChildTypes(type);
			if (value) {
				children = StructValue::GetChildren(*value);
			}
			break;
		case LogicalTypeId::LIST:
			function = "list_value";
			if (value) {
				children = ListValue::GetChildren(*value);
			}
			child_types.resize(children ? children->size() : 1, {Identifier(), ListType::GetChildType(type)});
			break;
		case LogicalTypeId::ARRAY:
			function = "array_value";
			if (value) {
				children = ArrayValue::GetChildren(*value);
			}
			child_types.resize(ArrayType::GetSize(type), {Identifier(), ArrayType::GetChildType(type)});
			break;
		default:
			return Failure(
			    UnsupportedFeature(path, "nested_constant_type", "The nested value type has no SQL constructor"));
		}
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto child =
			    ExportNestedConstant(child_types[i].second, children ? &(*children)[i] : nullptr, ChildPath(path, i));
			if (child.HasError()) {
				return child;
			}
			arguments.emplace_back(function == "struct_pack" ? child_types[i].first : Identifier(),
			                       std::move(child.GetValue()));
		}
		unique_ptr<ParsedExpression> result =
		    make_uniq<FunctionExpression>(QualifiedName("system", "main", Identifier(function)), std::move(arguments));
		if (type.HasAlias()) {
			result = SQLCast(type, std::move(result));
		}
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

	BoundExpressionSQLExportResult CastToConstructedType(const LogicalType &type, unique_ptr<ParsedExpression> child,
	                                                     const LogicalPlanVerificationPath &path) {
		auto target = ExportNestedConstant(type, nullptr, path);
		if (target.HasError()) {
			return target;
		}
		return BoundExpressionSQLExportResult::Success(
		    BinarySystemFunction("cast_to_type", std::move(child), std::move(target.GetValue())));
	}

	BoundExpressionSQLExportResult ExportConstant(const BoundConstantExpression &expression,
	                                              const LogicalPlanVerificationPath &path) {
		D_ASSERT(expression.GetExpressionType() == ExpressionType::VALUE_CONSTANT);
		auto &return_type = expression.GetReturnType();
		auto &value = expression.GetValue();
		D_ASSERT(return_type == value.type());
		if (!IsSQLValueType(return_type)) {
			return Failure(InternalExpressionInvariant(path, expression, "Bound constant has an unexportable type"));
		}
		if (TypeVisitor::Contains(return_type, LogicalTypeId::VARIANT) && HasUnsupportedVariantKeys(value)) {
			return Failure(UnsupportedFeature(path, "variant_literal",
			                                  "VARIANT object keys cannot be represented by a struct literal"));
		}
		if (return_type.IsAggregateState()) {
			auto storage_type = return_type.WithAlias("").WithExtensionInfo(nullptr);
			Vector source(value, count_t(1));
			Vector storage(storage_type, 1);
			storage.Reinterpret(source);
			auto child = ExportConstant(BoundConstantExpression(storage.GetValue(0)), path);
			if (child.HasError()) {
				return child;
			}
			auto result = ExportAggregateFunction::StateToSQL(return_type, std::move(child.GetValue()));
			if (!result) {
				return Failure(UnsupportedFeature(path, "aggregate_state_parameters",
				                                  "Aggregate state SQL parameters are not representable"));
			}
			return BoundExpressionSQLExportResult::Success(std::move(result));
		}
		if (return_type.id() != LogicalTypeId::GEOMETRY &&
		    (!IsSQLRepresentableType(return_type) || RequiresConstantConstructor(return_type))) {
			return ExportNestedConstant(return_type, value, path);
		}
		unique_ptr<ParsedExpression> result;
		if (!value.IsNull() && return_type.id() == LogicalTypeId::INTERVAL) {
			return BoundExpressionSQLExportResult::Success(IntervalSQLConstant(IntervalValue::Get(value)));
		}
		if (return_type.id() == LogicalTypeId::GEOMETRY) {
			if (value.IsNull()) {
				auto geometry = GeoType::HasCRS(return_type) ? Value("GEOMETRYCOLLECTION EMPTY") : Value();
				result = SQLCast(LogicalType::GEOMETRY(), ConstantExpression::FromValue(geometry));
			} else {
				result = UnarySystemFunction("st_geomfromwkb",
				                             ConstantExpression::FromValue(Value::BLOB_RAW(StringValue::Get(value))));
			}
			if (GeoType::HasCRS(return_type)) {
				vector<unique_ptr<ParsedExpression>> arguments;
				arguments.push_back(std::move(result));
				arguments.push_back(ConstantExpression::FromValue(Value(GeoType::GetCRS(return_type).GetDefinition())));
				result =
				    make_uniq<FunctionExpression>(QualifiedName("system", "main", "st_setcrs"), std::move(arguments));
				if (value.IsNull()) {
					auto typed_null = make_uniq<CaseExpression>();
					typed_null->CaseChecksMutable().push_back(
					    {ConstantExpression::FromValue(Value::BOOLEAN(false)), std::move(result)});
					typed_null->ElseMutable() = ConstantExpression::FromValue(Value());
					result = std::move(typed_null);
				}
				return BoundExpressionSQLExportResult::Success(std::move(result));
			}
		} else {
			result = ConstantExpression::FromValue(value.WithType(SQLCastType(return_type)));
		}
		if (return_type.id() != LogicalTypeId::SQLNULL &&
		    (result->GetExpressionClass() != ExpressionClass::CAST ||
		     !result->Cast<CastExpression>().TargetType().Equals(
		         *TypeExpression::FromLogicalType(SQLCastType(return_type))))) {
			result = SQLCast(return_type, std::move(result));
		}
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

	BoundExpressionSQLExportResult ExportReference(const BoundReferenceExpression &expression,
	                                               const LogicalPlanVerificationPath &path) {
		if (expression.GetExpressionType() != ExpressionType::BOUND_REF || lambda_reference_scopes.empty() ||
		    expression.Index() >= lambda_reference_scopes.back().size()) {
			return Failure(UnsupportedExpression(path, expression.GetExpressionClass()));
		}
		return BoundExpressionSQLExportResult::Success(lambda_reference_scopes.back()[expression.Index()]->Copy());
	}

	BoundExpressionSQLExportResult ExportLambda(const BoundLambdaExpression &lambda,
	                                            const BoundFunctionExpression &function, idx_t logical_argument_count,
	                                            const LogicalPlanVerificationPath &path) {
		if (lambda.GetExpressionType() != ExpressionType::LAMBDA || lambda.GetReturnType() != LogicalType::LAMBDA ||
		    !lambda.LambdaExpr() || !lambda.Captures().empty() || lambda.ParameterCount() == 0 ||
		    lambda.ParameterNames().size() != lambda.ParameterCount()) {
			return Failure(UnsupportedFeature(path, "lambda_binding",
			                                  "The bound lambda does not retain its SQL parameter binding"));
		}
		identifier_set_t unique_parameters;
		for (auto &parameter : lambda.ParameterNames()) {
			if (!IsValidIdentifier(parameter) || !unique_parameters.insert(parameter).second) {
				return Failure(
				    UnsupportedFeature(path, "lambda_binding", "The bound lambda has an invalid SQL parameter name"));
			}
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

	BoundExpressionSQLExportResult ExportColumnRef(const BoundColumnRefExpression &expression,
	                                               const LogicalPlanVerificationPath &path) {
		D_ASSERT(expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF);
		auto &binding = expression.Binding();
		if (!binding.table_index.IsValid() || !binding.column_index.IsValid()) {
			return Failure(InvalidBinding(path, binding, "Bound column reference has an incomplete binding"));
		}
		if (!IsSQLValueType(expression.GetReturnType())) {
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
		if (!IsSQLValueType(resolved->type)) {
			return Failure(InternalExpressionInvariant(path, expression, "The resolved SQL column type is incomplete"));
		}
		auto optimizer_type_match = resolved->optimizer_type && *resolved->optimizer_type == expression.GetReturnType();
		if (resolved->type != expression.GetReturnType() && !optimizer_type_match) {
			LogicalPlanVerificationIssue issue;
			issue.code = LogicalPlanVerificationIssueCode::TYPE_MISMATCH;
			issue.phase = LogicalPlanVerificationPhase::EXPRESSION_EXPORT;
			issue.path = path;
			issue.construct = LogicalPlanVerificationConstructIdentity::BindingTypeMismatch(resolved->type,
			                                                                                expression.GetReturnType());
			issue.message = "The resolved SQL column type differs from the bound expression type";
			return Failure(std::move(issue));
		}
		auto result =
		    BoundExpressionSQLExportResult::Success(make_uniq<ColumnRefExpression>(std::move(resolved->names)));
		if (optimizer_type_match) {
			return result;
		}
		if (!expression.GetReturnType().EqualsWithCollation(resolved->type)) {
			if (expression.GetReturnType().id() != LogicalTypeId::VARCHAR) {
				auto issue = UnsupportedFeature(path, "nested_result_collation",
				                                "Changing nested input collations requires a typed SQL representation");
				issue.facts.emplace_back("logical_type", Value(expression.GetReturnType().ToString()));
				issue.facts.emplace_back("input_logical_type", Value(resolved->type.ToString()));
				issue.facts.emplace_back("input_varchar_collations",
				                         Value(SQLExportHelpers::TypeCollationSignature(resolved->type)));
				issue.facts.emplace_back("varchar_collations",
				                         Value(SQLExportHelpers::TypeCollationSignature(expression.GetReturnType())));
				return Failure(std::move(issue));
			}
			if (StringType::GetCollation(expression.GetReturnType()).empty()) {
				return Failure(UnsupportedFeature(path, "column_collation_reset",
				                                  "Clearing an input collation requires a SQL representation"));
			}
			return PreserveCollation(expression.GetReturnType(), std::move(result), path);
		}
		return result;
	}

	LogicalPlanVerificationIssue InvalidBinding(const LogicalPlanVerificationPath &path, const ColumnBinding &binding,
	                                            string message) {
		LogicalPlanVerificationIssue issue;
		issue.code = LogicalPlanVerificationIssueCode::INVALID_BINDING;
		issue.phase = LogicalPlanVerificationPhase::EXPRESSION_EXPORT;
		issue.path = path;
		issue.facts.emplace_back("column_index", Value::UBIGINT(binding.column_index.GetIndexUnsafe()));
		issue.facts.emplace_back("table_index", Value::UBIGINT(binding.table_index.index));
		issue.message = std::move(message);
		return issue;
	}

	BoundExpressionSQLExportResult ExportFunction(const BoundFunctionExpression &expression,
	                                              const LogicalPlanVerificationPath &path) {
		switch (expression.GetExpressionType()) {
		case ExpressionType::BOUND_FUNCTION:
			return ExportScalarFunction(expression, path);
		case ExpressionType::OPERATOR_CAST:
			return ExportCast(expression, path);
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_DISTINCT_FROM:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			return ExportComparison(expression, path);
		case ExpressionType::COMPARE_BETWEEN:
			return ExportBetween(expression, path);
		default:
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound function has an invalid expression type"));
		}
	}

	BoundExpressionSQLExportResult ExportCast(const BoundFunctionExpression &expression,
	                                          const LogicalPlanVerificationPath &path) {
		if (expression.GetExpressionType() != ExpressionType::OPERATOR_CAST || expression.GetChildren().size() != 1 ||
		    !expression.GetChildren()[0] || !BoundCastExpression::HasValidBindData(expression) ||
		    !ChildrenAreConsistentWithArguments(expression.GetChildren(), expression.Function().GetArguments()) ||
		    expression.GetReturnType() != expression.Function().GetReturnType() ||
		    !IsSQLRepresentableType(expression.GetReturnType()) ||
		    !IsSQLRepresentableType(expression.GetChildren()[0]->GetReturnType())) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound cast has malformed type, data, or arity"));
		}
		if (context.discard_optimizer_metadata && CMUtils::GetExpressionType(expression) == CMExpressionType::CAST) {
			if (!BoundCastExpression::IsDefaultCast(expression)) {
				return Failure(
				    UnsupportedFeature(path, "compressed_materialization_cast",
				                       "The compressed materialization projection contains a non-default cast"));
			}
			return ExportChild(*expression.GetChildren()[0], path, 0);
		}
		if (BoundCastExpression::IsDefaultCast(expression)) {
			if (!context.client_context ||
			    CastFunctionSet::Get(*context.client_context.get_mutable()).HasRegisteredCastFunctions()) {
				return Failure(
				    UnsupportedFeature(path, "default_cast_binding",
				                       "A default-only bound cast cannot be reconstructed through this SQL binding"));
			}
		}
		auto child = ExportChild(*expression.GetChildren()[0], path, 0);
		if (child.HasError()) {
			return child;
		}
		if (expression.GetReturnType().IsAggregateState()) {
			if (BoundCastExpression::IsTryCast(expression) || !context.client_context ||
			    CastFunctionSet::Get(*context.client_context.get_mutable()).HasRegisteredCastFunctions()) {
				return Failure(
				    UnsupportedFeature(path, "aggregate_state_try_cast",
				                       "Aggregate state TRY_CAST or custom casts require a SQL representation"));
			}
			auto storage_type = expression.GetReturnType().WithAlias("").WithExtensionInfo(nullptr);
			auto result = ExportAggregateFunction::StateToSQL(expression.GetReturnType(),
			                                                  SQLCast(storage_type, std::move(child.GetValue())));
			if (!result) {
				return Failure(UnsupportedFeature(path, "aggregate_state_parameters",
				                                  "Aggregate state SQL parameters are not representable"));
			}
			return BoundExpressionSQLExportResult::Success(std::move(result));
		}
		if (HasNestedCollation(expression.GetReturnType())) {
			if (BoundCastExpression::IsTryCast(expression) ||
			    expression.GetReturnType() == expression.GetChildren()[0]->GetReturnType()) {
				return Failure(UnsupportedFeature(path, "nested_result_collation",
				                                  "This cast cannot preserve nested collations through SQL"));
			}
			return CastToConstructedType(expression.GetReturnType(), std::move(child.GetValue()), path);
		}
		return BoundExpressionSQLExportResult::Success(SQLCast(expression.GetReturnType(), std::move(child.GetValue()),
		                                                       BoundCastExpression::IsTryCast(expression)));
	}

	BoundExpressionSQLExportResult ExportComparison(const BoundFunctionExpression &expression,
	                                                const LogicalPlanVerificationPath &path) {
		if (!BoundComparisonExpression::IsComparison(expression.GetExpressionType()) ||
		    expression.GetReturnType() != LogicalType::BOOLEAN || expression.GetChildren().size() != 2 ||
		    !expression.GetChildren()[0] || !expression.GetChildren()[1] || expression.BindInfo() ||
		    !ChildrenAreConsistentWithArguments(expression.GetChildren(), expression.Function().GetArguments()) ||
		    expression.GetReturnType() != expression.Function().GetReturnType() ||
		    !IsSQLValueType(expression.GetChildren()[0]->GetReturnType()) ||
		    expression.GetChildren()[0]->GetReturnType() != expression.GetChildren()[1]->GetReturnType()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound comparison has malformed type or arity"));
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanVerificationIssue> issues;
		ExportChildren(expression.GetChildren(), path, children, issues);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		return BoundExpressionSQLExportResult::Success(make_uniq<ComparisonExpression>(
		    expression.GetExpressionType(), std::move(children[0]), std::move(children[1])));
	}

	BoundExpressionSQLExportResult ExportBetween(const BoundFunctionExpression &expression,
	                                             const LogicalPlanVerificationPath &path) {
		if (expression.GetExpressionType() != ExpressionType::COMPARE_BETWEEN ||
		    expression.GetReturnType() != LogicalType::BOOLEAN || expression.GetChildren().size() != 3 ||
		    !expression.GetChildren()[0] || !expression.GetChildren()[1] || !expression.GetChildren()[2] ||
		    !BoundBetweenExpression::HasValidBindData(expression) ||
		    !ChildrenAreConsistentWithArguments(expression.GetChildren(), expression.Function().GetArguments()) ||
		    expression.GetReturnType() != expression.Function().GetReturnType() ||
		    !IsSQLValueType(expression.GetChildren()[0]->GetReturnType()) ||
		    expression.GetChildren()[0]->GetReturnType() != expression.GetChildren()[1]->GetReturnType() ||
		    expression.GetChildren()[0]->GetReturnType() != expression.GetChildren()[2]->GetReturnType()) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound BETWEEN has malformed type, data, or arity"));
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanVerificationIssue> issues;
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
	                                                 const LogicalPlanVerificationPath &path) {
		if ((expression.GetExpressionType() != ExpressionType::CONJUNCTION_AND &&
		     expression.GetExpressionType() != ExpressionType::CONJUNCTION_OR) ||
		    expression.GetReturnType() != LogicalType::BOOLEAN || expression.GetChildren().size() < 2) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound conjunction has malformed type or arity"));
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanVerificationIssue> issues;
		ExportChildren(expression.GetChildren(), path, children, issues, LogicalType::BOOLEAN);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		auto result = make_uniq<ConjunctionExpression>(expression.GetExpressionType());
		result->GetChildrenMutable() = std::move(children);
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

	BoundExpressionSQLExportResult ExportCase(const BoundCaseExpression &expression,
	                                          const LogicalPlanVerificationPath &path) {
		D_ASSERT(expression.GetExpressionType() == ExpressionType::CASE_EXPR);
		if (!IsSQLValueType(expression.GetReturnType()) || expression.CaseChecks().empty()) {
			return Failure(InternalExpressionInvariant(path, expression, "Bound CASE has malformed type or arity"));
		}
		vector<ChildExpression> source_children;
		for (auto &check : expression.CaseChecks()) {
			source_children.emplace_back(check.when_expr.get(), LogicalType::BOOLEAN);
			source_children.emplace_back(check.then_expr.get(), expression.GetReturnType());
		}
		source_children.emplace_back(expression.ElseExpression().get(), expression.GetReturnType());

		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanVerificationIssue> issues;
		ExportChildren(source_children, path, children, issues);
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
	                                              const LogicalPlanVerificationPath &path) {
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
			if (child_count < 2 || !IsSQLValueType(expression.GetReturnType())) {
				return Failure(
				    InternalExpressionInvariant(path, expression, "Bound COALESCE has malformed type or arity"));
			}
			expected_type = expression.GetReturnType();
			break;
		case ExpressionType::OPERATOR_TRY:
			if (child_count != 1 || !expression.GetChildren()[0] || !IsSQLValueType(expression.GetReturnType())) {
				return Failure(InternalExpressionInvariant(path, expression, "Bound TRY has malformed type or arity"));
			}
			if (expression.GetChildren()[0]->IsVolatile()) {
				return Failure(UnsupportedFeature(path, "try_volatile_child",
				                                  "TRY cannot be rebound around a volatile expression"));
			}
			expected_type = expression.GetReturnType();
			break;
		case ExpressionType::OPERATOR_UNPACK:
		case ExpressionType::OPERATOR_NULLIF:
		case ExpressionType::GROUPING_FUNCTION:
		case ExpressionType::ARRAY_EXTRACT:
		case ExpressionType::ARRAY_SLICE:
		case ExpressionType::STRUCT_EXTRACT:
		case ExpressionType::ARRAY_CONSTRUCTOR:
		case ExpressionType::ARROW:
			return Failure(
			    UnsupportedFeature(path, "bound_operator", "The bound operator has no admitted parsed SQL AST form"));
		default:
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound operator has an invalid expression type"));
		}
		vector<unique_ptr<ParsedExpression>> children;
		vector<LogicalPlanVerificationIssue> issues;
		ExportChildren(expression.GetChildren(), path, children, issues, expected_type);
		if (!issues.empty()) {
			return BoundExpressionSQLExportResult::Failure(std::move(issues));
		}
		return BoundExpressionSQLExportResult::Success(
		    make_uniq<OperatorExpression>(expression.GetExpressionType(), std::move(children)));
	}

	BoundExpressionSQLExportResult CompressedMaterializationFailure(const BoundFunctionExpression &expression,
	                                                                const LogicalPlanVerificationPath &path,
	                                                                string message) {
		auto &function = expression.Function();
		auto &definition = function.GetDefinition();
		D_ASSERT(definition);
		return Failure(UnsupportedFunction(
		    path,
		    DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(), function.GetLogicalReturnType()),
		    std::move(message)));
	}

	optional<BoundExpressionSQLExportResult>
	TryExportCompressedMaterialization(const BoundFunctionExpression &expression,
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

	BoundExpressionSQLExportResult ExportScalarFunction(const BoundFunctionExpression &expression,
	                                                    const LogicalPlanVerificationPath &path) {
		auto &function = expression.Function();
		auto &definition = function.GetDefinition();
		if (!definition) {
			return Failure(
			    InternalExpressionInvariant(path, expression, "Bound scalar function has no retained definition"));
		}
		if (function.GetBindCallback() == TableFilterFunctions::Bind &&
		    TableFilterFunctions::IsTableFilterFunction(function.GetName())) {
			auto identity = DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(),
			                                           function.GetLogicalReturnType());
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
		bool rewritten_date_part = (qualified_name == QualifiedName("system", "main", "date_part") ||
		                            qualified_name == QualifiedName("system", "main", "datepart")) &&
		                           function.GetLogicalArguments().size() == 2 && expression.GetChildren().size() == 1 &&
		                           function.GetName() != definition->GetName() &&
		                           IsOptimizerFunctionQualification(function) && IsValidIdentifier(function.GetName());
		const bool retained_variadic_arguments =
		    definition->HasVarArgs() && expression.GetChildren().size() >= function.GetLogicalArguments().size();
		if ((!lambda_index.IsValid() && !retained_variadic_arguments &&
		     expression.GetChildren().size() != function.GetLogicalArguments().size() && !rewritten_date_part) ||
		    (lambda_index.IsValid() && expression.GetChildren().size() < function.GetLogicalArguments().size())) {
			return Failure(UnsupportedFunction(path, std::move(identity),
			                                   "The scalar function does not retain every SQL argument"));
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
		if (definition->GetProperties().GetCaptureArgumentAliases() && !argument_aliases_are_semantic &&
		    !captured_aliases_are_ignored && !definition->HasUnbindCallback()) {
			return Failure(UnsupportedFunction(path, std::move(identity),
			                                   "The bound scalar function does not expose its SQL argument names"));
		}
		if (definition->GetProperties().RequiresExpressionNames() && !definition->HasUnbindCallback() &&
		    !argument_aliases_are_semantic && !captured_aliases_are_ignored) {
			return Failure(
			    UnsupportedFunction(path, std::move(identity),
			                        "The bound scalar function requires expression names that are not retained"));
		}
		vector<Identifier> argument_names;
		if (argument_aliases_are_semantic) {
			argument_names.resize(expression.GetChildren().size());
			for (idx_t argument_index = first_argument_alias; argument_index < argument_names.size();
			     argument_index++) {
				argument_names[argument_index] = expression.GetChildren()[argument_index]->GetAlias();
				if (argument_names[argument_index].empty()) {
					return Failure(UnsupportedFunction(path, std::move(identity),
					                                   "The bound scalar function is missing a SQL argument name"));
				}
			}
		}
		for (idx_t argument_index = 0; argument_index < argument_names.size(); argument_index++) {
			auto &argument_name = argument_names[argument_index];
			if (argument_name.empty()) {
				continue;
			}
			if (!IsValidIdentifier(argument_name)) {
				return Failure(InternalExpressionInvariant(
				    ChildPath(path, argument_index), *expression.GetChildren()[argument_index],
				    "The scalar SQL argument-name callback returned an invalid identifier"));
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
				return Failure(UnsupportedFunction(path, std::move(identity),
				                                   "The function cannot reconstruct its bound invocation"));
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
		if (!captured_aliases_are_ignored && IsSQLRepresentableType(expression.GetReturnType()) &&
		    !expression.GetReturnType().IsAggregateState() &&
		    (definition->HasBindCallback() || definition->GetReturnType().id() == LogicalTypeId::SQLNULL) &&
		    definition->GetReturnType() != expression.GetReturnType()) {
			if (HasNestedCollation(expression.GetReturnType())) {
				return CastToConstructedType(expression.GetReturnType(), std::move(result), path);
			}
			result = SQLCast(expression.GetReturnType(), std::move(result));
		}
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

	BoundAggregateSQLExportResult BuildAggregateCall(const BoundAggregateExpression &expression,
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
		const bool collated_minmax =
		    name &&
		    ((*name == QualifiedName("system", "main", "min") && function.GetName() == "arg_min") ||
		     (*name == QualifiedName("system", "main", "max") && function.GetName() == "arg_max")) &&
		    IsOptimizerFunctionQualification(function) &&
		    expression.GetChildren().size() == function.GetLogicalArguments().size() + 1;
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
				if ((order.type != OrderType::ASCENDING && order.type != OrderType::DESCENDING) ||
				    (order.null_order != OrderByNullType::NULLS_FIRST &&
				     order.null_order != OrderByNullType::NULLS_LAST)) {
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
		auto result = make_uniq<FunctionExpression>(
		    *name, std::move(arguments), std::move(filter), std::move(order_bys), expression.IsDistinct(), false,
		    expression.StateExportMode() == AggregateStateExportMode::STATE_EXPORT);
		return BoundAggregateSQLExportResult::Success(std::move(result));
	}

	BoundExpressionSQLExportResult ExportAggregate(const BoundAggregateExpression &expression,
	                                               const LogicalPlanVerificationPath &path) {
		auto call = BuildAggregateCall(expression, path);
		if (call.HasError()) {
			return BoundExpressionSQLExportResult::Failure(call.GetIssues());
		}
		auto &function = expression.Function();
		auto &definition = function.GetDefinition();
		unique_ptr<ParsedExpression> result = std::move(call.GetValue());
		if (expression.StateExportMode() == AggregateStateExportMode::NONE &&
		    IsSQLRepresentableType(expression.GetReturnType()) && !expression.GetReturnType().IsAggregateState() &&
		    definition->HasBindCallback() && definition->GetReturnType() != expression.GetReturnType()) {
			result = SQLCast(expression.GetReturnType(), std::move(result));
		}
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}

public:
	BoundAggregateSQLExportResult ExportAggregateCall(const BoundAggregateExpression &expression,
	                                                  const LogicalPlanVerificationPath &path) {
		auto call = BuildAggregateCall(expression, path);
		if (call.HasError()) {
			return call;
		}
		if (!expression.GetReturnType().EqualsWithCollation(expression.Function().GetLogicalReturnType())) {
			return AggregateFailure(
			    UnsupportedFeature(path, "aggregate_call_result_type",
			                       "A bare aggregate call cannot preserve the bound expression's logical result type"));
		}
		return call;
	}

private:
	BoundExpressionSQLExportResult ExportChild(const Expression &expression, const LogicalPlanVerificationPath &path,
	                                           idx_t child_index) {
		return Export(expression, ChildPath(path, child_index));
	}

	void ExportChildren(const vector<unique_ptr<Expression>> &source, const LogicalPlanVerificationPath &path,
	                    vector<unique_ptr<ParsedExpression>> &result, vector<LogicalPlanVerificationIssue> &issues,
	                    const optional<LogicalType> &expected_type = {}) {
		vector<ChildExpression> source_children;
		for (auto &child : source) {
			source_children.emplace_back(child.get(), expected_type);
		}
		ExportChildren(source_children, path, result, issues);
	}

	void ExportChildren(const vector<ChildExpression> &source, const LogicalPlanVerificationPath &path,
	                    vector<unique_ptr<ParsedExpression>> &result, vector<LogicalPlanVerificationIssue> &issues) {
		result.resize(source.size());
		for (idx_t child_index = 0; child_index < source.size(); child_index++) {
			auto child_path = ChildPath(path, child_index);
			auto &input = source[child_index];
			if (!input.expression) {
				issues.push_back(InternalInvariant(child_path, "Bound expression has a null child"));
				continue;
			}
			if (input.expected_type && input.expression->GetReturnType() != *input.expected_type) {
				issues.push_back(InternalExpressionInvariant(child_path, *input.expression,
				                                             "Bound expression child has an unexpected type"));
				continue;
			}
			auto child = Export(*input.expression, child_path);
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
	vector<vector<unique_ptr<ParsedExpression>>> lambda_reference_scopes;
};

LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
BoundExpressionSQLExporter::Export(const Expression &expression, const BoundExpressionSQLExportContext &context) {
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	return ExportAtPath(expression, context, path);
}

LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
BoundExpressionSQLExporter::ExportAtPath(const Expression &expression, const BoundExpressionSQLExportContext &context,
                                         const LogicalPlanVerificationPath &path) {
	D_ASSERT(IsExpressionRootPath(path));
	BoundExpressionSQLExportState state(context);
	return state.Export(expression, path);
}

LogicalPlanVerificationResult<unique_ptr<FunctionExpression>>
BoundExpressionSQLExporter::ExportAggregateCallAtPath(const BoundAggregateExpression &expression,
                                                      const BoundExpressionSQLExportContext &context,
                                                      const LogicalPlanVerificationPath &path) {
	D_ASSERT(IsExpressionRootPath(path));
	BoundExpressionSQLExportState state(context);
	return state.ExportAggregateCall(expression, path);
}

LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
BoundExpressionSQLExporter::ExportWindowAtPath(const BoundWindowExpression &expression,
                                               const BoundExpressionSQLExportContext &context,
                                               const LogicalPlanVerificationPath &path) {
	D_ASSERT(IsExpressionRootPath(path));
	BoundExpressionSQLExportState state(context);
	return state.ExportWindow(expression, path);
}

LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
BoundExpressionSQLExporter::ExportUnnestAtPath(const BoundUnnestExpression &expression,
                                               const BoundExpressionSQLExportContext &context,
                                               const LogicalPlanVerificationPath &path) {
	D_ASSERT(IsExpressionRootPath(path));
	BoundExpressionSQLExportState state(context);
	return state.ExportUnnest(expression, path);
}

} // namespace duckdb
