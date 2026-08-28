#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/comparison_functions.hpp"
#include "duckdb/function/scalar/operator_functions.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_expanded_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_between_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_cast_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/planner.hpp"

using namespace duckdb;

namespace {

using ExportResult = LogicalPlanCompilerResult<unique_ptr<ParsedExpression>>;

static unique_ptr<LogicalOperator> OptimizeExportQuery(Connection &connection, const string &query) {
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(query);
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	Optimizer optimizer(*planner.binder, *connection.context);
	return optimizer.Optimize(std::move(planner.plan));
}

static unique_ptr<LogicalOperator> BindExportQuery(Connection &connection, const string &query) {
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(query);
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	return std::move(planner.plan);
}

static optional_ptr<const Expression> FindExpression(const Expression &expression,
                                                     const std::function<bool(const Expression &)> &matches) {
	if (matches(expression)) {
		return expression;
	}
	optional_ptr<const Expression> result;
	ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) {
		if (!result) {
			result = FindExpression(child, matches);
		}
	});
	return result;
}

static optional_ptr<const Expression> FindExpression(const LogicalOperator &op,
                                                     const std::function<bool(const Expression &)> &matches) {
	for (auto &expression : op.expressions) {
		auto result = FindExpression(*expression, matches);
		if (result) {
			return result;
		}
	}
	for (auto &child : op.children) {
		auto result = FindExpression(*child, matches);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

static BoundExpressionSQLExportContext ResolveBinding(ColumnBinding target, vector<Identifier> names,
                                                      LogicalType type) {
	BoundExpressionSQLExportContext context;
	context.resolve_binding = [target, names = std::move(names), type = std::move(type)](
	                              const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		if (binding != target) {
			return {};
		}
		return ResolvedSQLColumnReference {names, type};
	};
	return context;
}

static void RequireRoundTrip(Connection &connection, const Expression &expression,
                             const BoundExpressionSQLExportContext &context, const string &from_clause,
                             const string &oracle_expression) {
	auto exported = BoundExpressionSQLExporter::Export(expression, context);
	REQUIRE(exported.IsValid());
	string issue_description;
	if (exported.HasError()) {
		for (auto &issue : exported.GetIssues()) {
			issue_description +=
			    StringUtil::Format("code=%d message=%s; ", static_cast<idx_t>(issue.code), issue.message);
		}
	}
	INFO(issue_description);
	REQUIRE(exported.IsSuccess());
	auto exported_sql = "SELECT " + exported.GetValue()->ToString() + from_clause;
	auto exported_result = connection.Query(exported_sql);
	REQUIRE_FALSE(exported_result->HasError());
	auto oracle_result = connection.Query("SELECT " + oracle_expression + from_clause);
	REQUIRE_FALSE(oracle_result->HasError());
	INFO("exported SQL=" << exported_sql);
	INFO("exported result=" << exported_result->ToString());
	INFO("oracle result=" << oracle_result->ToString());
	REQUIRE(exported_result->Equals(*oracle_result, false));
}

static void RequireIssue(const ExportResult &result, LogicalPlanCompilerIssueCode code,
                         const LogicalPlanCompilerPath &path) {
	REQUIRE(result.IsValid());
	REQUIRE(result.HasError());
	REQUIRE(result.GetIssues().size() == 1);
	REQUIRE(result.GetIssues()[0].code == code);
	REQUIRE(result.GetIssues()[0].phase == LogicalPlanCompilerPhase::EXPRESSION_EXPORT);
	REQUIRE(result.GetIssues()[0].path == optional<LogicalPlanCompilerPath>(path));
}

static unique_ptr<Expression> Constant(Value value) {
	return make_uniq<BoundConstantExpression>(std::move(value));
}

struct SQLBindingEntry {
	ColumnBinding binding;
	LogicalType type;
	Identifier name;
};

static void CollectSQLBindings(const Expression &expression, vector<SQLBindingEntry> &entries) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column = expression.Cast<BoundColumnRefExpression>();
		for (auto &entry : entries) {
			if (entry.binding == column.Binding()) {
				REQUIRE(entry.type == column.GetReturnType());
				return;
			}
		}
		entries.push_back(SQLBindingEntry {column.Binding(), column.GetReturnType(),
		                                   Identifier("exported_" + to_string(entries.size()))});
		return;
	}
	ExpressionIterator::EnumerateChildren(expression,
	                                      [&](const Expression &child) { CollectSQLBindings(child, entries); });
}

class SyntheticExpression : public Expression {
public:
	SyntheticExpression(ExpressionClass expression_class, ExpressionType expression_type, LogicalType return_type)
	    : Expression(expression_type, expression_class, std::move(return_type)) {
	}

	string ToString() const override {
		return "synthetic_expression";
	}

	unique_ptr<Expression> Copy() const override {
		return make_uniq<SyntheticExpression>(expression_class, type, return_type);
	}
};

class OpaqueSQLFunctionData : public FunctionData {
public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<OpaqueSQLFunctionData>();
	}

	bool Equals(const FunctionData &) const override {
		return true;
	}
};

static unique_ptr<FunctionData> BindOpaqueSQLFunction(BindScalarFunctionInput &) {
	return make_uniq<OpaqueSQLFunctionData>();
}

static ExpressionType ClaimCastExpressionType(FunctionToStringInput &) {
	return ExpressionType::OPERATOR_CAST;
}

static ExpressionType ClaimComparisonExpressionType(FunctionToStringInput &) {
	return ExpressionType::COMPARE_EQUAL;
}

static ExpressionType ClaimBetweenExpressionType(FunctionToStringInput &) {
	return ExpressionType::COMPARE_BETWEEN;
}

struct NotEqualOperation {
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return left != right;
	}
};

struct OutsideRangeOperation {
	template <class INPUT_TYPE, class LOWER_TYPE, class UPPER_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, LOWER_TYPE lower, UPPER_TYPE upper) {
		return input < lower || input > upper;
	}
};

struct SubtractOperation {
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return left - right;
	}
};

static void RequireInvalidExpressionTypes(const Expression &expression, const BoundExpressionSQLExportContext &context,
                                          const string &label) {
	LogicalPlanCompilerPath path;
	path.root = LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION;
	for (auto type : {ExpressionType::INVALID, static_cast<ExpressionType>(255)}) {
		INFO("expression class=" << label << " expression type=" << static_cast<uint32_t>(type));
		auto malformed = expression.Copy();
		malformed->SetExpressionTypeUnsafe(type);
		RequireIssue(BoundExpressionSQLExporter::Export(*malformed, context),
		             LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT, path);
	}
}

} // namespace

TEST_CASE("Bound expression SQL export preserves exact constant types and values", "[bound_expression_sql_export]") {
	vector<Value> values;
	values.push_back(Value());
	values.push_back(Value(LogicalType::SMALLINT));
	values.push_back(Value::TINYINT(-12));
	values.push_back(Value::UTINYINT(250));
	values.push_back(Value::BIGINT(-9000000000));
	values.push_back(Value::UBIGINT(18000000000000000000ULL));
	values.push_back(Value::HUGEINT(hugeint_t(42)));
	values.push_back(Value::UHUGEINT(uhugeint_t(84)));
	values.push_back(Value::FLOAT(1.25));
	values.push_back(Value::DOUBLE(-3.5));
	values.push_back(Value::DECIMAL(12345, 9, 3));
	values.push_back(Value::LIST(LogicalType::SMALLINT, {Value::SMALLINT(1), Value::SMALLINT(2)}));
	auto struct_type =
	    LogicalType::STRUCT({{"n", LogicalType::INTEGER}, {"items", LogicalType::LIST(LogicalType::VARCHAR)}});
	values.push_back(Value::STRUCT(struct_type, {Value::INTEGER(7), Value::LIST({Value("x"), Value("y")})}));

	DuckDB db;
	Connection connection(db);
	BoundExpressionSQLExportContext context;
	for (auto &value : values) {
		BoundConstantExpression expression(value);
		auto result = BoundExpressionSQLExporter::Export(expression, context);
		REQUIRE(result.IsValid());
		REQUIRE(result.IsSuccess());
		if (value.type().id() == LogicalTypeId::SQLNULL) {
			REQUIRE(result.GetValue()->GetExpressionClass() == ExpressionClass::CONSTANT);
		} else {
			REQUIRE(result.GetValue()->GetExpressionClass() == ExpressionClass::CAST);
			REQUIRE(result.GetValue()->Cast<CastExpression>().TargetType() == value.type());
		}
		auto rebound = connection.Query("SELECT " + result.GetValue()->ToString());
		REQUIRE_FALSE(rebound->HasError());
		REQUIRE(rebound->GetTypes()[0] == value.type());
		if (value.IsNull()) {
			REQUIRE(rebound->GetValue(0, 0).IsNull());
		} else {
			REQUIRE(rebound->GetValue(0, 0) == value);
		}
	}
	BoundConstantExpression metadata_expression(Value::INTEGER(1));
	metadata_expression.SetAlias(Identifier("display_only"));
	metadata_expression.SetQueryLocation(optional_idx(17));
	auto metadata_result = BoundExpressionSQLExporter::Export(metadata_expression, context);
	REQUIRE(metadata_result.IsSuccess());
	REQUIRE_FALSE(metadata_result.GetValue()->HasAlias());
	REQUIRE_FALSE(metadata_result.GetValue()->HasQueryLocation());
	REQUIRE_FALSE(metadata_result.GetValue()->Cast<CastExpression>().Child().HasAlias());
	REQUIRE_FALSE(metadata_result.GetValue()->Cast<CastExpression>().Child().HasQueryLocation());
}

TEST_CASE("Bound expression SQL export resolves columns only by binding", "[bound_expression_sql_export]") {
	auto left_binding = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	auto right_binding = ColumnBinding(TableIndex(11), ProjectionIndex(0));
	auto left = make_uniq<BoundColumnRefExpression>(Identifier("duplicate_alias"), LogicalType::INTEGER, left_binding);
	auto right =
	    make_uniq<BoundColumnRefExpression>(Identifier("duplicate_alias"), LogicalType::INTEGER, right_binding);
	auto comparison =
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHAN, std::move(left), std::move(right));
	BoundExpressionSQLExportContext context;
	context.resolve_binding = [=](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		if (binding == left_binding) {
			return ResolvedSQLColumnReference {{Identifier("src"), Identifier("select")}, LogicalType::INTEGER};
		}
		if (binding == right_binding) {
			return ResolvedSQLColumnReference {{Identifier("src"), Identifier("Mixed Case")}, LogicalType::INTEGER};
		}
		return {};
	};
	auto result = BoundExpressionSQLExporter::Export(*comparison, context);
	REQUIRE(result.IsSuccess());
	REQUIRE(result.GetValue()->GetExpressionClass() == ExpressionClass::COMPARISON);
	auto &parsed = result.GetValue()->Cast<ComparisonExpression>();
	REQUIRE(parsed.Left().Cast<ColumnRefExpression>().ColumnNames() ==
	        vector<Identifier> {Identifier("src"), Identifier("select")});
	REQUIRE(parsed.Right().Cast<ColumnRefExpression>().ColumnNames() ==
	        vector<Identifier> {Identifier("src"), Identifier("Mixed Case")});

	DuckDB db;
	Connection connection(db);
	RequireRoundTrip(connection, *comparison, context, " FROM (SELECT 11 AS \"select\", 22 AS \"Mixed Case\") AS src",
	                 "src.\"select\" < src.\"Mixed Case\"");

	BoundColumnRefExpression missing(Identifier("usable_alias"), LogicalType::INTEGER, left_binding);
	BoundExpressionSQLExportContext missing_context;
	auto missing_result = BoundExpressionSQLExporter::Export(missing, missing_context);
	LogicalPlanCompilerPath standalone_path;
	standalone_path.root = LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(missing_result, LogicalPlanCompilerIssueCode::INVALID_BINDING, standalone_path);
	REQUIRE(missing_result.GetIssues()[0].facts.size() == 2);

	auto wrong_type = ResolveBinding(left_binding, {Identifier("src"), Identifier("select")}, LogicalType::BIGINT);
	auto mismatch = BoundExpressionSQLExporter::Export(missing, wrong_type);
	RequireIssue(mismatch, LogicalPlanCompilerIssueCode::TYPE_MISMATCH, standalone_path);
	auto &type_mismatch = *mismatch.GetIssues()[0].construct->type_mismatch;
	REQUIRE(type_mismatch.expected_type == LogicalType::BIGINT);
	REQUIRE(type_mismatch.actual_type == LogicalType::INTEGER);

	BoundColumnRefExpression correlated(LogicalType::INTEGER, left_binding, 1);
	auto correlated_result = BoundExpressionSQLExporter::Export(correlated, context);
	RequireIssue(correlated_result, LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE, standalone_path);
	REQUIRE(*correlated_result.GetIssues()[0].construct->identifier == "correlated_column_reference");

	BoundColumnRefExpression invalid_binding(LogicalType::INTEGER, ColumnBinding());
	auto invalid_result = BoundExpressionSQLExporter::Export(invalid_binding, context);
	RequireIssue(invalid_result, LogicalPlanCompilerIssueCode::INVALID_BINDING, standalone_path);

	auto invalid_name = ResolveBinding(left_binding, {Identifier(string("\xFF", 1))}, LogicalType::INTEGER);
	auto invalid_name_result = BoundExpressionSQLExporter::Export(missing, invalid_name);
	RequireIssue(invalid_name_result, LogicalPlanCompilerIssueCode::INVALID_BINDING, standalone_path);

	BoundColumnRefExpression incomplete_type(LogicalType::ANY, left_binding);
	auto incomplete_type_result = BoundExpressionSQLExporter::Export(incomplete_type, context);
	RequireIssue(incomplete_type_result, LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT, standalone_path);
	auto incomplete_resolution =
	    ResolveBinding(left_binding, {Identifier("src"), Identifier("select")}, LogicalType::ANY);
	auto incomplete_resolution_result = BoundExpressionSQLExporter::Export(missing, incomplete_resolution);
	RequireIssue(incomplete_resolution_result, LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT, standalone_path);
}

TEST_CASE("Bound expression SQL export composes deterministic expression paths", "[bound_expression_sql_export]") {
	auto binding = ColumnBinding(TableIndex(20), ProjectionIndex(3));
	auto expression = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding));
	BoundExpressionSQLExportContext context;
	LogicalPlanCompilerPath root;
	root.root = LogicalPlanCompilerPathRoot::LOGICAL_PLAN;
	root.components.push_back(
	    LogicalPlanCompilerPathComponent {LogicalPlanCompilerPathComponentType::OPERATOR_CHILD, 2});
	root.components.push_back(
	    LogicalPlanCompilerPathComponent {LogicalPlanCompilerPathComponentType::OPERATOR_EXPRESSION, 4});
	auto result = BoundExpressionSQLExporter::ExportAtPath(*expression, context, root);
	REQUIRE(result.IsValid());
	REQUIRE(result.HasError());
	REQUIRE(result.GetIssues().size() == 2);
	for (idx_t child_index = 0; child_index < 2; child_index++) {
		auto expected = root;
		expected.components.push_back(
		    LogicalPlanCompilerPathComponent {LogicalPlanCompilerPathComponentType::EXPRESSION_CHILD, child_index});
		REQUIRE(result.GetIssues()[child_index].path == optional<LogicalPlanCompilerPath>(expected));
	}

	LogicalPlanCompilerPath invalid_root;
	invalid_root.root = LogicalPlanCompilerPathRoot::LOGICAL_PLAN;
	auto invalid = BoundExpressionSQLExporter::ExportAtPath(*expression, context, invalid_root);
	REQUIRE(invalid.IsValid());
	REQUIRE(invalid.HasError());
	REQUIRE(invalid.GetIssues().size() == 1);
	REQUIRE(invalid.GetIssues()[0].code == LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT);
	REQUIRE_FALSE(invalid.GetIssues()[0].path.has_value());
}

TEST_CASE("Bound expression SQL export reconstructs structural expression forms", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	BoundExpressionSQLExportContext context;

	auto cast = BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(42)),
	                                               LogicalType::BIGINT, true);
	auto cast_result = BoundExpressionSQLExporter::Export(*cast, context);
	REQUIRE(cast_result.IsSuccess());
	REQUIRE(cast_result.GetValue()->Cast<CastExpression>().IsTryCast());
	REQUIRE(cast_result.GetValue()->Cast<CastExpression>().TargetType() == LogicalType::BIGINT);

	auto default_cast = BoundCastExpression::AddDefaultCastToType(Constant(Value::INTEGER(42)), LogicalType::BIGINT);
	auto default_result = BoundExpressionSQLExporter::Export(*default_cast, context);
	REQUIRE(default_result.HasError());
	REQUIRE(default_result.GetIssues()[0].code == LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(*default_result.GetIssues()[0].construct->identifier == "default_cast_binding");

	for (auto comparison_type :
	     {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_NOTEQUAL, ExpressionType::COMPARE_LESSTHAN,
	      ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_LESSTHANOREQUALTO,
	      ExpressionType::COMPARE_GREATERTHANOREQUALTO, ExpressionType::COMPARE_DISTINCT_FROM,
	      ExpressionType::COMPARE_NOT_DISTINCT_FROM}) {
		auto comparison = BoundComparisonExpression::Create(comparison_type, Constant(Value::INTEGER(1)),
		                                                    Constant(Value::INTEGER(2)));
		auto result = BoundExpressionSQLExporter::Export(*comparison, context);
		REQUIRE(result.IsSuccess());
		REQUIRE(result.GetValue()->GetExpressionClass() == ExpressionClass::COMPARISON);
		REQUIRE(result.GetValue()->GetExpressionType() == comparison_type);
	}

	auto inclusive = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(1)),
	                                                Constant(Value::INTEGER(3)), true, true);
	auto inclusive_result = BoundExpressionSQLExporter::Export(*inclusive, context);
	REQUIRE(inclusive_result.IsSuccess());
	REQUIRE(inclusive_result.GetValue()->GetExpressionClass() == ExpressionClass::BETWEEN);

	auto exclusive = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(1)),
	                                                Constant(Value::INTEGER(3)), false, true);
	auto exclusive_result = BoundExpressionSQLExporter::Export(*exclusive, context);
	REQUIRE(exclusive_result.IsSuccess());
	REQUIRE(exclusive_result.GetValue()->GetExpressionClass() == ExpressionClass::CONJUNCTION);
	auto &exclusive_conjunction = exclusive_result.GetValue()->Cast<ConjunctionExpression>();
	REQUIRE(exclusive_conjunction.GetChildren().size() == 2);
	REQUIRE(exclusive_conjunction.GetChildren()[0]->GetExpressionType() == ExpressionType::COMPARE_GREATERTHAN);
	REQUIRE(exclusive_conjunction.GetChildren()[1]->GetExpressionType() == ExpressionType::COMPARE_LESSTHANOREQUALTO);

	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	conjunction->GetChildrenMutable().push_back(BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(1)), Constant(Value::INTEGER(1))));
	auto nested = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	nested->GetChildrenMutable().push_back(BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(2)), Constant(Value::INTEGER(2))));
	nested->GetChildrenMutable().push_back(BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(3)), Constant(Value::INTEGER(3))));
	conjunction->GetChildrenMutable().push_back(std::move(nested));
	auto conjunction_result = BoundExpressionSQLExporter::Export(*conjunction, context);
	REQUIRE(conjunction_result.IsSuccess());
	REQUIRE(conjunction_result.GetValue()->Cast<ConjunctionExpression>().GetChildren().size() == 2);
	REQUIRE(conjunction_result.GetValue()
	            ->Cast<ConjunctionExpression>()
	            .GetChildren()[1]
	            ->Cast<ConjunctionExpression>()
	            .GetChildren()
	            .size() == 2);

	auto case_expression = make_uniq<BoundCaseExpression>(LogicalType::INTEGER);
	case_expression->CaseChecksMutable().push_back({Constant(Value::BOOLEAN(false)), Constant(Value::INTEGER(10))});
	case_expression->CaseChecksMutable().push_back({Constant(Value::BOOLEAN(true)), Constant(Value::INTEGER(20))});
	case_expression->ElseMutable() = Constant(Value::INTEGER(30));
	auto case_result = BoundExpressionSQLExporter::Export(*case_expression, context);
	REQUIRE(case_result.IsSuccess());
	REQUIRE(case_result.GetValue()->Cast<CaseExpression>().CaseChecks().size() == 2);
	RequireRoundTrip(connection, *case_expression, context, string(),
	                 "CASE WHEN false THEN 10 WHEN true THEN 20 ELSE 30 END");
}

TEST_CASE("Bound expression SQL export admits only validated bound operators", "[bound_expression_sql_export]") {
	BoundExpressionSQLExportContext context;
	vector<unique_ptr<BoundOperatorExpression>> expressions;

	auto not_expression = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
	not_expression->GetChildrenMutable().push_back(Constant(Value::BOOLEAN(true)));
	expressions.push_back(std::move(not_expression));

	for (auto type : {ExpressionType::OPERATOR_IS_NULL, ExpressionType::OPERATOR_IS_NOT_NULL}) {
		auto expression = make_uniq<BoundOperatorExpression>(type, LogicalType::BOOLEAN);
		expression->GetChildrenMutable().push_back(Constant(Value(LogicalType::INTEGER)));
		expressions.push_back(std::move(expression));
	}
	for (auto type : {ExpressionType::COMPARE_IN, ExpressionType::COMPARE_NOT_IN}) {
		auto expression = make_uniq<BoundOperatorExpression>(type, LogicalType::BOOLEAN);
		expression->GetChildrenMutable().push_back(Constant(Value::INTEGER(1)));
		expression->GetChildrenMutable().push_back(Constant(Value::INTEGER(2)));
		expression->GetChildrenMutable().push_back(Constant(Value::INTEGER(3)));
		expressions.push_back(std::move(expression));
	}
	auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, LogicalType::INTEGER);
	coalesce->GetChildrenMutable().push_back(Constant(Value(LogicalType::INTEGER)));
	coalesce->GetChildrenMutable().push_back(Constant(Value::INTEGER(4)));
	expressions.push_back(std::move(coalesce));
	auto try_expression = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_TRY, LogicalType::INTEGER);
	try_expression->GetChildrenMutable().push_back(Constant(Value::INTEGER(5)));
	expressions.push_back(std::move(try_expression));

	for (auto &expression : expressions) {
		auto result = BoundExpressionSQLExporter::Export(*expression, context);
		REQUIRE(result.IsValid());
		REQUIRE(result.IsSuccess());
		REQUIRE(result.GetValue()->GetExpressionClass() == ExpressionClass::OPERATOR);
		REQUIRE(result.GetValue()->GetExpressionType() == expression->GetExpressionType());
	}

	BoundOperatorExpression invalid(ExpressionType::ARRAY_EXTRACT, LogicalType::INTEGER);
	invalid.GetChildrenMutable().push_back(Constant(Value::INTEGER(1)));
	invalid.GetChildrenMutable().push_back(Constant(Value::INTEGER(2)));
	auto invalid_result = BoundExpressionSQLExporter::Export(invalid, context);
	REQUIRE(invalid_result.HasError());
	REQUIRE(invalid_result.GetIssues()[0].code == LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE);

	BoundOperatorExpression invalid_arity(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
	auto arity_result = BoundExpressionSQLExporter::Export(invalid_arity, context);
	REQUIRE(arity_result.HasError());
	REQUIRE(arity_result.GetIssues()[0].code == LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT);
}

TEST_CASE("Bound expression SQL export rejects TRY around volatile children", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	LogicalPlanCompilerPath path;
	path.root = LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION;

	auto fallible_cast = BoundCastExpression::AddCastToType(*connection.context, Constant(Value("not an integer")),
	                                                        LogicalType::INTEGER);
	BoundOperatorExpression nonvolatile_try(ExpressionType::OPERATOR_TRY, LogicalType::INTEGER);
	nonvolatile_try.GetChildrenMutable().push_back(std::move(fallible_cast));
	auto original = ExpressionExecutor::EvaluateScalar(*connection.context, nonvolatile_try);
	REQUIRE(original.type() == LogicalType::INTEGER);
	REQUIRE(original.IsNull());
	auto nonvolatile_result = BoundExpressionSQLExporter::Export(nonvolatile_try, context);
	REQUIRE(nonvolatile_result.IsSuccess());
	auto rebound = connection.Query("SELECT " + nonvolatile_result.GetValue()->ToString());
	REQUIRE_FALSE(rebound->HasError());
	REQUIRE(rebound->GetTypes() == vector<LogicalType> {LogicalType::INTEGER});
	REQUIRE(rebound->GetValue(0, 0).IsNull());

	auto random_plan = BindExportQuery(connection, "SELECT random()");
	auto random = FindExpression(*random_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION && expression.IsVolatile() &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "random";
	});
	REQUIRE(random);
	REQUIRE(BoundExpressionSQLExporter::Export(*random, context).IsSuccess());
	BoundOperatorExpression volatile_try(ExpressionType::OPERATOR_TRY, LogicalType::DOUBLE);
	volatile_try.GetChildrenMutable().push_back(random->Copy());
	auto volatile_result = BoundExpressionSQLExporter::Export(volatile_try, context);
	RequireIssue(volatile_result, LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
	REQUIRE(*volatile_result.GetIssues()[0].construct->identifier == "try_volatile_child");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export reconstructs optimizer-produced scalar functions",
          "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE scalar_values(i INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO scalar_values VALUES (-7, 'AbC')"));
	connection.BeginTransaction();

	auto plan = OptimizeExportQuery(connection, "SELECT abs(i), i + 2, struct_pack(value := i) FROM scalar_values");
	auto abs_expression = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "abs";
	});
	REQUIRE(abs_expression);
	auto &abs_function = abs_expression->Cast<BoundFunctionExpression>();
	REQUIRE(abs_function.GetChildren().size() == 1);
	auto &abs_column = abs_function.GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto abs_context = ResolveBinding(abs_column.Binding(), {Identifier("v"), Identifier("i")}, LogicalType::INTEGER);
	RequireRoundTrip(connection, *abs_expression, abs_context, " FROM scalar_values AS v", "abs(v.i)");

	auto addition = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().IsOperator() &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "+";
	});
	REQUIRE(addition);
	auto &addition_function = addition->Cast<BoundFunctionExpression>();
	auto &addition_column = addition_function.GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto addition_context =
	    ResolveBinding(addition_column.Binding(), {Identifier("v"), Identifier("i")}, LogicalType::INTEGER);
	RequireRoundTrip(connection, *addition, addition_context, " FROM scalar_values AS v", "v.i + 2");
	auto addition_result = BoundExpressionSQLExporter::Export(*addition, addition_context);
	REQUIRE(addition_result.IsSuccess());
	auto &addition_parsed = addition_result.GetValue()->Cast<FunctionExpression>();
	REQUIRE_FALSE(addition_parsed.IsOperator());
	auto &addition_definition = addition_function.Function().GetDefinition();
	REQUIRE(addition_definition);
	REQUIRE(addition_parsed.GetQualifiedName().Catalog() == addition_definition->GetCatalogName());
	REQUIRE(addition_parsed.GetQualifiedName().Schema() == addition_definition->GetSchemaName());
	REQUIRE(addition_parsed.GetQualifiedName().Name() == addition_definition->GetName());

	auto struct_pack = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "struct_pack";
	});
	REQUIRE(struct_pack);
	auto struct_result = BoundExpressionSQLExporter::Export(*struct_pack, abs_context);
	REQUIRE(struct_result.IsValid());
	REQUIRE(struct_result.HasError());
	INFO(struct_result.GetIssues()[0].message);
	REQUIRE(struct_result.GetIssues()[0].code == LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION);

	ScalarFunction opaque_function(Identifier("opaque_scalar"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                               ScalarFunction::NopFunction);
	opaque_function.SetCatalogName(Identifier("synthetic_catalog"));
	opaque_function.SetSchemaName(Identifier("synthetic_schema"));
	vector<unique_ptr<Expression>> opaque_children;
	opaque_children.push_back(Constant(Value::INTEGER(1)));
	BoundFunctionExpression opaque_expression(BoundScalarFunction(opaque_function), std::move(opaque_children),
	                                          make_uniq<OpaqueSQLFunctionData>());
	BoundExpressionSQLExportContext opaque_context;
	auto opaque_result = BoundExpressionSQLExporter::Export(opaque_expression, opaque_context);
	REQUIRE(opaque_result.IsValid());
	REQUIRE(opaque_result.HasError());
	REQUIRE(opaque_result.GetIssues()[0].code == LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION);

	auto random_plan = OptimizeExportQuery(connection, "SELECT random()");
	auto random_expression = FindExpression(*random_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "random";
	});
	REQUIRE(random_expression);
	auto volatile_between = BoundBetweenExpression::Create(random_expression->Copy(), Constant(Value::DOUBLE(0)),
	                                                       Constant(Value::DOUBLE(1)), false, false);
	BoundExpressionSQLExportContext empty_context;
	auto volatile_result = BoundExpressionSQLExporter::Export(*volatile_between, empty_context);
	REQUIRE(volatile_result.HasError());
	REQUIRE(volatile_result.GetIssues()[0].code == LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(*volatile_result.GetIssues()[0].construct->identifier == "exclusive_between_input_evaluation");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export preserves qualified operator function identity",
          "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_operator_extension");
	loader.UseDedicatedSchemaForExtension(Identifier("synthetic_operator_schema"));
	loader.RegisterFunction(
	    ScalarFunction(Identifier("+"), {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
	                   ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, SubtractOperation>));
	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();

	auto plan =
	    BindExportQuery(connection, "SELECT synthetic_operator_schema.\"+\"(CAST(7 AS INTEGER), CAST(2 AS INTEGER))");
	auto expression = FindExpression(*plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       candidate.Cast<BoundFunctionExpression>().Function().GetName() == "+";
	});
	REQUIRE(expression);
	auto operator_expression = expression->Copy();
	auto &operator_function = operator_expression->Cast<BoundFunctionExpression>();
	operator_function.IsOperatorMutable() = true;
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, operator_function) == Value::INTEGER(5));

	BoundExpressionSQLExportContext context;
	auto result = BoundExpressionSQLExporter::Export(operator_function, context);
	REQUIRE(result.IsSuccess());
	auto &parsed = result.GetValue()->Cast<FunctionExpression>();
	auto &definition = operator_function.Function().GetDefinition();
	REQUIRE(definition);
	REQUIRE_FALSE(parsed.IsOperator());
	REQUIRE(parsed.GetQualifiedName().Catalog() == definition->GetCatalogName());
	REQUIRE(parsed.GetQualifiedName().Schema() == definition->GetSchemaName());
	REQUIRE(parsed.GetQualifiedName().Name() == definition->GetName());

	auto rebound = connection.Query("SELECT " + parsed.ToString());
	REQUIRE_FALSE(rebound->HasError());
	REQUIRE(rebound->GetTypes() == vector<LogicalType> {LogicalType::INTEGER});
	REQUIRE(rebound->GetValue(0, 0) == Value::INTEGER(5));
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export does not trust scalar expression type callbacks",
          "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_expression_type_extension");
	loader.UseDedicatedSchemaForExtension(Identifier("synthetic_expression_type_schema"));

	ScalarFunction comparison(Identifier("misleading_equal"), {LogicalType::INTEGER, LogicalType::INTEGER},
	                          LogicalType::BOOLEAN,
	                          ScalarFunction::BinaryFunction<int32_t, int32_t, bool, NotEqualOperation>);
	comparison.SetGetExpressionTypeCallback(ClaimComparisonExpressionType);
	loader.RegisterFunction(std::move(comparison));

	ScalarFunction cast(Identifier("misleading_cast"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                    ScalarFunction::NopFunction, BindOpaqueSQLFunction);
	cast.SetGetExpressionTypeCallback(ClaimCastExpressionType);
	loader.RegisterFunction(std::move(cast));

	ScalarFunction between(Identifier("misleading_between"),
	                       {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::BOOLEAN,
	                       ScalarFunction::TernaryFunction<int32_t, int32_t, int32_t, bool, OutsideRangeOperation>,
	                       BindOpaqueSQLFunction);
	between.SetGetExpressionTypeCallback(ClaimBetweenExpressionType);
	loader.RegisterFunction(std::move(between));

	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();
	auto bind_function = [&](const string &query, const Identifier &name) {
		auto plan = BindExportQuery(connection, query);
		auto expression = FindExpression(*plan, [&](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
			       candidate.Cast<BoundFunctionExpression>().Function().GetName() == name;
		});
		REQUIRE(expression);
		return expression->Copy();
	};

	auto misleading_equal = bind_function(
	    "SELECT synthetic_expression_type_schema.misleading_equal(CAST(7 AS INTEGER), CAST(7 AS INTEGER))",
	    Identifier("misleading_equal"));
	REQUIRE(misleading_equal->GetExpressionType() == ExpressionType::COMPARE_EQUAL);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *misleading_equal) == Value::BOOLEAN(false));
	BoundExpressionSQLExportContext context;
	auto comparison_result = BoundExpressionSQLExporter::Export(*misleading_equal, context);
	REQUIRE(comparison_result.IsSuccess());
	REQUIRE(comparison_result.GetValue()->GetExpressionClass() == ExpressionClass::FUNCTION);
	auto &parsed = comparison_result.GetValue()->Cast<FunctionExpression>();
	REQUIRE(parsed.GetQualifiedName().Schema() == Identifier("synthetic_expression_type_schema"));
	REQUIRE(parsed.GetQualifiedName().Name() == Identifier("misleading_equal"));
	auto comparison_rebound = connection.Query("SELECT " + parsed.ToString());
	REQUIRE_FALSE(comparison_rebound->HasError());
	REQUIRE(comparison_rebound->GetTypes() == vector<LogicalType> {LogicalType::BOOLEAN});
	REQUIRE(comparison_rebound->GetValue(0, 0) == Value::BOOLEAN(false));
	LogicalPlanCompilerPath path;
	path.root = LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION;

	auto callback_spoof = OperatorEqualFun::GetFunction();
	callback_spoof.SetFunctionCallback(ScalarFunction::BinaryFunction<int32_t, int32_t, bool, NotEqualOperation>);
	vector<unique_ptr<Expression>> callback_children;
	callback_children.push_back(Constant(Value::INTEGER(7)));
	callback_children.push_back(Constant(Value::INTEGER(7)));
	auto callback_expression = callback_spoof.Bind(*connection.context, std::move(callback_children));
	REQUIRE(callback_expression->GetExpressionType() == ExpressionType::COMPARE_EQUAL);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *callback_expression) == Value::BOOLEAN(false));
	RequireIssue(BoundExpressionSQLExporter::Export(*callback_expression, context),
	             LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION, path);

	auto property_spoof = IsDistinctFromFun::GetFunction();
	property_spoof.SetNullHandling(FunctionNullHandling::DEFAULT_NULL_HANDLING);
	vector<unique_ptr<Expression>> property_children;
	property_children.push_back(Constant(Value(LogicalType::INTEGER)));
	property_children.push_back(Constant(Value::INTEGER(1)));
	auto property_expression = property_spoof.Bind(*connection.context, std::move(property_children));
	REQUIRE(property_expression->GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *property_expression).IsNull());
	RequireIssue(BoundExpressionSQLExporter::Export(*property_expression, context),
	             LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION, path);

	vector<pair<ExpressionType, unique_ptr<Expression>>> malformed_bind_data;
	malformed_bind_data.emplace_back(
	    ExpressionType::OPERATOR_CAST,
	    bind_function("SELECT synthetic_expression_type_schema.misleading_cast(CAST(7 AS INTEGER))",
	                  Identifier("misleading_cast")));
	malformed_bind_data.emplace_back(
	    ExpressionType::COMPARE_BETWEEN,
	    bind_function(
	        "SELECT synthetic_expression_type_schema.misleading_between(CAST(7 AS INTEGER), CAST(2 AS INTEGER), "
	        "CAST(9 AS INTEGER))",
	        Identifier("misleading_between")));
	for (auto &entry : malformed_bind_data) {
		REQUIRE(entry.second->GetExpressionType() == entry.first);
		RequireIssue(BoundExpressionSQLExporter::Export(*entry.second, context),
		             LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION, path);
	}

	auto malformed_cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	malformed_cast->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<OpaqueSQLFunctionData>();
	RequireIssue(BoundExpressionSQLExporter::Export(*malformed_cast, context),
	             LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT, path);

	auto malformed_between = BoundBetweenExpression::Create(Constant(Value::INTEGER(7)), Constant(Value::INTEGER(2)),
	                                                        Constant(Value::INTEGER(9)), true, true);
	malformed_between->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<OpaqueSQLFunctionData>();
	RequireIssue(BoundExpressionSQLExporter::Export(*malformed_between, context),
	             LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT, path);
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export preserves aggregate modifiers", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE aggregate_values(i INTEGER)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO aggregate_values VALUES (1), (2), (2), (NULL)"));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE count_values(i INTEGER NOT NULL)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO count_values VALUES (1), (2)"));
	REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='compressed_materialization'"));
	connection.BeginTransaction();

	auto check_aggregate = [&](const string &query, const string &oracle, const Identifier &name, bool optimize) {
		INFO("aggregate query=" << query);
		auto plan = optimize ? OptimizeExportQuery(connection, query) : BindExportQuery(connection, query);
		auto aggregate = FindExpression(*plan, [&](const Expression &expression) {
			if (expression.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				return false;
			}
			auto &definition = expression.Cast<BoundAggregateExpression>().Function().GetDefinition();
			return definition && definition->GetName() == name;
		});
		REQUIRE(aggregate);
		auto &bound_aggregate = aggregate->Cast<BoundAggregateExpression>();
		REQUIRE_FALSE(bound_aggregate.GetChildren().empty());
		REQUIRE(bound_aggregate.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF);
		vector<SQLBindingEntry> entries;
		CollectSQLBindings(*aggregate, entries);
		BoundExpressionSQLExportContext context;
		context.resolve_binding = [entries](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
			for (auto &entry : entries) {
				if (entry.binding == binding) {
					return ResolvedSQLColumnReference {{Identifier("v"), entry.name}, entry.type};
				}
			}
			return {};
		};
		string from_clause = " FROM (SELECT i";
		for (auto &entry : entries) {
			from_clause += ", CAST(i AS " + entry.type.ToString() + ") AS " + entry.name.GetIdentifierName();
		}
		from_clause += " FROM aggregate_values) AS v";
		RequireRoundTrip(connection, *aggregate, context, from_clause, oracle);
		return BoundExpressionSQLExporter::Export(*aggregate, context);
	};

	auto sum = check_aggregate("SELECT sum(i) FROM aggregate_values", "sum(v.i)", Identifier("sum"), true);
	REQUIRE(sum.GetValue()->Cast<FunctionExpression>().FunctionName() == "sum");

	auto distinct_filter = check_aggregate("SELECT sum(DISTINCT i) FILTER (WHERE i > 1) FROM aggregate_values",
	                                       "sum(DISTINCT v.i) FILTER (WHERE v.i > 1)", Identifier("sum"), false);
	auto &distinct_function = distinct_filter.GetValue()->Cast<FunctionExpression>();
	REQUIRE(distinct_function.Distinct());
	REQUIRE(distinct_function.Filter());

	auto ordered = check_aggregate("SELECT list(i ORDER BY i DESC NULLS FIRST) FROM aggregate_values",
	                               "list(v.i ORDER BY v.i DESC NULLS FIRST)", Identifier("list"), false);
	auto &ordered_function = ordered.GetValue()->Cast<FunctionExpression>();
	REQUIRE(ordered_function.OrderBy());
	REQUIRE(ordered_function.OrderBy()->orders.size() == 1);
	REQUIRE(ordered_function.OrderBy()->orders[0].type == OrderType::DESCENDING);
	REQUIRE(ordered_function.OrderBy()->orders[0].null_order == OrderByNullType::NULLS_FIRST);

	auto state = check_aggregate("SELECT sum(i) EXPORT_STATE FROM aggregate_values", "sum(v.i) EXPORT_STATE",
	                             Identifier("sum"), false);
	REQUIRE(state.GetValue()->Cast<FunctionExpression>().ExportState());

	auto count_plan = OptimizeExportQuery(connection, "SELECT count(i) FROM count_values WHERE random() > 0.5");
	auto count = FindExpression(*count_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       expression.Cast<BoundAggregateExpression>().Function().GetName() == "count_star";
	});
	REQUIRE(count);
	auto &count_aggregate = count->Cast<BoundAggregateExpression>();
	REQUIRE(count_aggregate.Function().GetName() == "count_star");
	REQUIRE(count_aggregate.GetChildren().empty());
	BoundExpressionSQLExportContext count_context;
	auto count_result = BoundExpressionSQLExporter::Export(*count, count_context);
	REQUIRE(count_result.IsValid());
	REQUIRE(count_result.HasError());
	REQUIRE(count_result.GetIssues()[0].code == LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION);
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export fails closed for deferred and malformed inputs",
          "[bound_expression_sql_export]") {
	BoundExpressionSQLExportContext context;
	LogicalPlanCompilerPath path;
	path.root = LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION;
	vector<unique_ptr<Expression>> deferred;
	deferred.push_back(make_uniq<BoundDefaultExpression>(LogicalType::INTEGER));
	deferred.push_back(make_uniq<BoundParameterExpression>(Identifier("1")));
	deferred.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, storage_t(0)));
	deferred.push_back(make_uniq<BoundSubqueryExpression>(LogicalType::INTEGER));
	deferred.push_back(make_uniq<BoundWindowExpression>(LogicalType::INTEGER, nullptr, nullptr, nullptr));
	deferred.push_back(make_uniq<BoundUnnestExpression>(LogicalType::INTEGER));
	deferred.push_back(
	    make_uniq<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::INTEGER, Constant(Value::INTEGER(1)), 1));
	deferred.push_back(
	    make_uniq<BoundLambdaRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(1), ProjectionIndex(0)), 0));
	deferred.push_back(make_uniq<LegacyBoundCastExpression>(Constant(Value::INTEGER(1)), LogicalType::BIGINT, false));
	deferred.push_back(make_uniq<LegacyBoundComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(1)), Constant(Value::INTEGER(1))));
	deferred.push_back(make_uniq<LegacyBoundBetweenExpression>(Constant(Value::INTEGER(1)), Constant(Value::INTEGER(0)),
	                                                           Constant(Value::INTEGER(2)), true, true));
	for (auto &expression : deferred) {
		RequireIssue(BoundExpressionSQLExporter::Export(*expression, context),
		             LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPRESSION, path);
	}

	vector<unique_ptr<Expression>> expanded_children;
	expanded_children.push_back(Constant(Value::INTEGER(1)));
	BoundExpandedExpression expanded(std::move(expanded_children));
	RequireIssue(BoundExpressionSQLExporter::Export(expanded, context),
	             LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT, path);
	SyntheticExpression parsed(ExpressionClass::CONSTANT, ExpressionType::VALUE_CONSTANT, LogicalType::INTEGER);
	RequireIssue(BoundExpressionSQLExporter::Export(parsed, context), LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT,
	             path);
	SyntheticExpression invalid(ExpressionClass::INVALID, ExpressionType::INVALID, LogicalType::INTEGER);
	RequireIssue(BoundExpressionSQLExporter::Export(invalid, context), LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT,
	             path);
	SyntheticExpression unknown(static_cast<ExpressionClass>(255), ExpressionType::INVALID, LogicalType::INTEGER);
	RequireIssue(BoundExpressionSQLExporter::Export(unknown, context), LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT,
	             path);

	auto malformed = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	malformed->GetChildrenMutable().push_back(nullptr);
	malformed->GetChildrenMutable().push_back(make_uniq<BoundDefaultExpression>(LogicalType::BOOLEAN));
	auto malformed_result = BoundExpressionSQLExporter::Export(*malformed, context);
	REQUIRE(malformed_result.IsValid());
	REQUIRE(malformed_result.HasError());
	REQUIRE(malformed_result.GetIssues().size() == 2);
	REQUIRE(malformed_result.GetIssues()[0].code == LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT);
	REQUIRE(malformed_result.GetIssues()[1].code == LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPRESSION);
}

TEST_CASE("Bound expression SQL export rejects invalid class and type combinations", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE expression_type_values(i INTEGER)"));
	connection.BeginTransaction();
	auto plan = BindExportQuery(connection, "SELECT abs(i), sum(i) FROM expression_type_values GROUP BY i");
	auto function = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "abs";
	});
	REQUIRE(function);
	auto aggregate = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       expression.Cast<BoundAggregateExpression>().Function().GetName() == "sum";
	});
	REQUIRE(aggregate);

	vector<pair<string, unique_ptr<Expression>>> expressions;
	expressions.emplace_back("constant", Constant(Value::INTEGER(1)));
	expressions.emplace_back(
	    "column reference",
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(1), ProjectionIndex(0))));
	expressions.emplace_back("function", function->Copy());
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	conjunction->GetChildrenMutable().push_back(Constant(Value::BOOLEAN(true)));
	conjunction->GetChildrenMutable().push_back(Constant(Value::BOOLEAN(false)));
	expressions.emplace_back("conjunction", std::move(conjunction));
	auto case_expression = make_uniq<BoundCaseExpression>(LogicalType::INTEGER);
	case_expression->CaseChecksMutable().push_back({Constant(Value::BOOLEAN(true)), Constant(Value::INTEGER(1))});
	case_expression->ElseMutable() = Constant(Value::INTEGER(2));
	expressions.emplace_back("case", std::move(case_expression));
	auto operator_expression = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
	operator_expression->GetChildrenMutable().push_back(Constant(Value::BOOLEAN(true)));
	expressions.emplace_back("operator", std::move(operator_expression));
	expressions.emplace_back("aggregate", aggregate->Copy());

	BoundExpressionSQLExportContext context;
	for (auto &entry : expressions) {
		RequireInvalidExpressionTypes(*entry.second, context, entry.first);
	}

	BoundOperatorExpression deferred(ExpressionType::ARRAY_EXTRACT, LogicalType::INTEGER);
	deferred.GetChildrenMutable().push_back(Constant(Value::INTEGER(1)));
	deferred.GetChildrenMutable().push_back(Constant(Value::INTEGER(2)));
	LogicalPlanCompilerPath path;
	path.root = LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(BoundExpressionSQLExporter::Export(deferred, context),
	             LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export owns outputs and propagates resolver exceptions",
          "[bound_expression_sql_export]") {
	auto success = []() {
		BoundConstantExpression expression(Value::SMALLINT(9));
		BoundExpressionSQLExportContext context;
		return BoundExpressionSQLExporter::Export(expression, context);
	}();
	REQUIRE(success.IsSuccess());
	REQUIRE(success.GetValue()->Cast<CastExpression>().TargetType() == LogicalType::SMALLINT);

	auto failure = []() {
		BoundColumnRefExpression expression(LogicalType::INTEGER, ColumnBinding(TableIndex(88), ProjectionIndex(7)));
		BoundExpressionSQLExportContext context;
		return BoundExpressionSQLExporter::Export(expression, context);
	}();
	REQUIRE(failure.HasError());
	REQUIRE(failure.GetIssues()[0].facts.size() == 2);

	BoundColumnRefExpression expression(LogicalType::INTEGER, ColumnBinding(TableIndex(1), ProjectionIndex(0)));
	BoundExpressionSQLExportContext throwing_context;
	throwing_context.resolve_binding = [](const ColumnBinding &) -> optional<ResolvedSQLColumnReference> {
		throw InvalidInputException("synthetic resolver failure");
	};
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(expression, throwing_context), InvalidInputException);
}
