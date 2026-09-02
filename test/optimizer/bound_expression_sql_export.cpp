#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/function/function_binder.hpp"
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
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
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

#include <new>

using namespace duckdb;

namespace {

using ExportResult = LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>;

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

static void RequireIssue(const ExportResult &result, LogicalPlanVerificationIssueCode code,
                         const LogicalPlanVerificationPath &path) {
	REQUIRE(result.IsValid());
	REQUIRE(result.HasError());
	REQUIRE_FALSE(result.IsSuccess());
	REQUIRE(result.GetIssues().size() == 1);
	INFO(result.GetIssues()[0].message);
	REQUIRE(result.GetIssues()[0].code == code);
	REQUIRE(result.GetIssues()[0].phase == LogicalPlanVerificationPhase::EXPRESSION_EXPORT);
	REQUIRE(result.GetIssues()[0].path == optional<LogicalPlanVerificationPath>(path));
}

static void RequireInvalidHookResult(const ExportResult &result, const LogicalPlanVerificationPath &path) {
	RequireIssue(result, LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	REQUIRE(result.GetIssues()[0].construct);
	REQUIRE(result.GetIssues()[0].construct->type == LogicalPlanVerificationConstructType::FUNCTION);
}

static void RequireFunctionIssue(const ExportResult &result, LogicalPlanVerificationIssueCode code,
                                 const LogicalPlanVerificationPath &path, const Identifier &catalog,
                                 const Identifier &schema, const Identifier &name, const vector<LogicalType> &arguments,
                                 const LogicalType &return_type) {
	RequireIssue(result, code, path);
	auto &issue = result.GetIssues()[0];
	REQUIRE(issue.construct);
	REQUIRE(issue.construct->type == LogicalPlanVerificationConstructType::FUNCTION);
	REQUIRE(issue.construct->function);
	auto &identity = *issue.construct->function;
	REQUIRE(identity.catalog == catalog.GetIdentifierName());
	REQUIRE(identity.schema == schema.GetIdentifierName());
	REQUIRE(identity.name == name.GetIdentifierName());
	REQUIRE(identity.arguments == arguments);
	REQUIRE(identity.return_type == return_type);
}

static unique_ptr<Expression> Constant(Value value) {
	return make_uniq<BoundConstantExpression>(std::move(value));
}

static unique_ptr<Expression> BinaryRoundTrip(ClientContext &context, const Expression &expression) {
	MemoryStream stream(Allocator::Get(context));
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::Latest();
	BinarySerializer::Serialize(expression, stream, options);
	stream.Rewind();
	bound_parameter_map_t parameters;
	return BinaryDeserializer::Deserialize<Expression>(stream, context, parameters);
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

static void CollectAggregateExpressions(const Expression &expression,
                                        vector<reference<const BoundAggregateExpression>> &aggregates) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE) {
		aggregates.push_back(expression.Cast<BoundAggregateExpression>());
	}
	ExpressionIterator::EnumerateChildren(
	    expression, [&](const Expression &child) { CollectAggregateExpressions(child, aggregates); });
}

static void CollectAggregateExpressions(const LogicalOperator &op,
                                        vector<reference<const BoundAggregateExpression>> &aggregates) {
	for (auto &expression : op.expressions) {
		CollectAggregateExpressions(*expression, aggregates);
	}
	for (auto &child : op.children) {
		CollectAggregateExpressions(*child, aggregates);
	}
}

struct SyntheticSQLSource {
	BoundExpressionSQLExportContext context;
	string from_clause;
};

static SyntheticSQLSource CreateSyntheticSQLSource(const Expression &expression) {
	vector<SQLBindingEntry> entries;
	CollectSQLBindings(expression, entries);
	REQUIRE_FALSE(entries.empty());

	SyntheticSQLSource result;
	result.context.resolve_binding = [entries](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		for (auto &entry : entries) {
			if (entry.binding == binding) {
				return ResolvedSQLColumnReference {{Identifier("v"), entry.name}, entry.type};
			}
		}
		return {};
	};

	string first_row;
	string second_row;
	string column_names;
	for (idx_t entry_idx = 0; entry_idx < entries.size(); entry_idx++) {
		auto &entry = entries[entry_idx];
		if (entry_idx > 0) {
			first_row += ", ";
			second_row += ", ";
			column_names += ", ";
		}
		if (entry.type.id() == LogicalTypeId::BOOLEAN) {
			first_row += "TRUE";
			second_row += "TRUE";
		} else {
			REQUIRE(entry.type.IsNumeric());
			first_row += "CAST(1 AS " + entry.type.ToString() + ")";
			second_row += "CAST(2 AS " + entry.type.ToString() + ")";
		}
		column_names += entry.name.GetIdentifierName();
	}
	result.from_clause = " FROM (VALUES (" + first_row + "), (" + second_row + ")) AS v(" + column_names + ")";
	return result;
}

static void RequireLiveCatalogDefinition(ClientContext &context, const BoundAggregateExpression &aggregate) {
	auto &definition = aggregate.Function().GetDefinition();
	REQUIRE(definition);
	auto &catalog = Catalog::GetCatalog(context, definition->GetCatalogName());
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    context, QualifiedName(definition->GetCatalogName(), definition->GetSchemaName(), definition->GetName()));
	idx_t definition_matches = 0;
	for (auto &candidate : entry.functions.functions) {
		if (candidate == definition) {
			definition_matches++;
		}
	}
	REQUIRE(definition_matches == 1);
}

static void RequireAggregateRewriteRoundTrip(Connection &connection, const BoundAggregateExpression &aggregate) {
	REQUIRE(aggregate.GetAggregateType() == AggregateType::NON_DISTINCT);
	REQUIRE_FALSE(aggregate.GetOrderBys());
	REQUIRE(aggregate.StateExportMode() == AggregateStateExportMode::NONE);
	auto source = CreateSyntheticSQLSource(aggregate);
	auto exported = BoundExpressionSQLExporter::Export(aggregate, source.context);
	string issue_description;
	if (exported.HasError()) {
		for (auto &issue : exported.GetIssues()) {
			issue_description +=
			    StringUtil::Format("code=%d message=%s; ", static_cast<idx_t>(issue.code), issue.message);
		}
	}
	INFO("aggregate=" << aggregate.ToString());
	INFO("bind_info=" << static_cast<bool>(aggregate.BindInfo()));
	INFO("return_type=" << aggregate.GetReturnType().ToString());
	INFO("function_return_type=" << aggregate.Function().GetReturnType().ToString());
	INFO("definition_signature=" << aggregate.Function().GetDefinition()->GetSignature().ToString());
	INFO(issue_description);
	REQUIRE(exported.IsSuccess());
	auto &exported_function = exported.GetValue()->Cast<FunctionExpression>();
	auto &definition = aggregate.Function().GetDefinition();
	REQUIRE(exported_function.GetQualifiedName() ==
	        QualifiedName(definition->GetCatalogName(), definition->GetSchemaName(), definition->GetName()));

	vector<unique_ptr<ParsedExpression>> oracle_children;
	for (auto &child : aggregate.GetChildren()) {
		auto exported_child = BoundExpressionSQLExporter::Export(*child, source.context);
		REQUIRE(exported_child.IsSuccess());
		oracle_children.push_back(std::move(exported_child.GetValue()));
	}
	unique_ptr<ParsedExpression> oracle_filter;
	if (aggregate.GetFilter()) {
		auto exported_filter = BoundExpressionSQLExporter::Export(*aggregate.GetFilter(), source.context);
		REQUIRE(exported_filter.IsSuccess());
		oracle_filter = std::move(exported_filter.GetValue());
	}
	FunctionExpression oracle(
	    QualifiedName(definition->GetCatalogName(), definition->GetSchemaName(), definition->GetName()),
	    std::move(oracle_children), std::move(oracle_filter));

	auto exported_result = connection.Query("SELECT " + exported.GetValue()->ToString() + source.from_clause);
	REQUIRE_FALSE(exported_result->HasError());
	auto oracle_result = connection.Query("SELECT " + oracle.ToString() + source.from_clause);
	REQUIRE_FALSE(oracle_result->HasError());
	REQUIRE(exported_result->GetTypes() == oracle_result->GetTypes());
	REQUIRE(exported_result->Equals(*oracle_result, false));
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

class SpoofCastFunctionData : public FunctionData {
public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SpoofCastFunctionData>(*this);
	}

	bool Equals(const FunctionData &) const override {
		return true;
	}

	FunctionDataKind GetKind() const {
		return FunctionDataKind::BOUND_CAST;
	}

	int32_t value = 0;
};

class SpoofBetweenFunctionData : public FunctionData {
public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SpoofBetweenFunctionData>(*this);
	}

	bool Equals(const FunctionData &) const override {
		return true;
	}

	FunctionDataKind GetKind() const {
		return FunctionDataKind::BOUND_BETWEEN;
	}

	int32_t value = 0;
};

static unique_ptr<FunctionData> BindOpaqueSQLFunction(BindScalarFunctionInput &) {
	return make_uniq<OpaqueSQLFunctionData>();
}

static unique_ptr<FunctionData> BindOpaqueSQLAggregate(BindAggregateFunctionInput &) {
	return make_uniq<OpaqueSQLFunctionData>();
}

static FunctionSQLExportResult ExportScalarFirstChild(ScalarFunctionSQLExportInput &input) {
	if (input.children.size() != 2 || !input.children[0] || !input.children[1]) {
		return FunctionSQLExportResult::Success(nullptr);
	}
	return FunctionSQLExportResult::Success(std::move(input.children[0]));
}

static FunctionSQLExportResult ExportAggregateAsSum(AggregateFunctionSQLExportInput &input) {
	if (input.children.size() != 1 || !input.children[0]) {
		return FunctionSQLExportResult::Success(nullptr);
	}
	auto name = QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(), Identifier("sum"));
	return FunctionSQLExportResult::Success(
	    make_uniq<FunctionExpression>(name, std::move(input.children), std::move(input.filter),
	                                  std::move(input.order_bys), input.aggregate_type == AggregateType::DISTINCT,
	                                  false, input.state_export_mode == AggregateStateExportMode::STATE_EXPORT));
}

static FunctionSQLExportResult ExportMalformedScalar(ScalarFunctionSQLExportInput &) {
	return FunctionSQLExportResult::Success(nullptr);
}

static FunctionSQLExportResult ExportMalformedAggregate(AggregateFunctionSQLExportInput &) {
	return FunctionSQLExportResult::Success(nullptr);
}

static FunctionSQLExportResult ExportThrowingScalar(ScalarFunctionSQLExportInput &) {
	throw InvalidInputException("synthetic scalar SQL export failure");
}

static FunctionSQLExportResult ExportThrowingAggregate(AggregateFunctionSQLExportInput &) {
	throw InvalidInputException("synthetic aggregate SQL export failure");
}

static FunctionSQLExportResult ExportBadAllocScalar(ScalarFunctionSQLExportInput &) {
	throw std::bad_alloc();
}

static FunctionSQLExportResult EmptySQLExportFailure() {
	return FunctionSQLExportResult::Failure({});
}

static FunctionSQLExportResult InvalidSQLExportFailure() {
	vector<LogicalPlanVerificationIssue> issues;
	LogicalPlanVerificationIssue issue;
	issue.code = LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION;
	issues.push_back(std::move(issue));
	return FunctionSQLExportResult::Failure(std::move(issues));
}

static FunctionSQLExportResult ExportEmptyScalarFailure(ScalarFunctionSQLExportInput &) {
	return EmptySQLExportFailure();
}

static FunctionSQLExportResult ExportEmptyAggregateFailure(AggregateFunctionSQLExportInput &) {
	return EmptySQLExportFailure();
}

static FunctionSQLExportResult ExportInvalidScalarFailure(ScalarFunctionSQLExportInput &) {
	return InvalidSQLExportFailure();
}

static FunctionSQLExportResult ExportInvalidAggregateFailure(AggregateFunctionSQLExportInput &) {
	return InvalidSQLExportFailure();
}

template <class INPUT>
static LogicalPlanVerificationIssue ScopedFunctionIssue(INPUT &input, LogicalPlanVerificationPhase phase,
                                                        LogicalPlanVerificationPath path) {
	LogicalPlanVerificationIssue issue;
	issue.code = LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION;
	issue.phase = phase;
	issue.path = std::move(path);
	issue.construct = LogicalPlanVerificationConstructIdentity::Function(input.identity);
	issue.message = "Synthetic function SQL export hook rejected the expression";
	return issue;
}

template <class INPUT>
static FunctionSQLExportResult ExportScopedFunctionFailure(INPUT &input, LogicalPlanVerificationPhase phase,
                                                           LogicalPlanVerificationPath path) {
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(ScopedFunctionIssue(input, phase, std::move(path)));
	return FunctionSQLExportResult::Failure(std::move(issues));
}

template <class INPUT>
static LogicalPlanVerificationPath OtherRootPath(INPUT &input) {
	LogicalPlanVerificationPath path;
	path.root = input.path.root == LogicalPlanVerificationPathRoot::LOGICAL_PLAN
	                ? LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION
	                : LogicalPlanVerificationPathRoot::LOGICAL_PLAN;
	return path;
}

template <class INPUT>
static LogicalPlanVerificationPath SiblingPath(INPUT &input) {
	auto path = input.path;
	D_ASSERT(!path.components.empty());
	path.components.back().ordinal++;
	return path;
}

template <class INPUT>
static LogicalPlanVerificationPath DescendantPath(INPUT &input, idx_t child_index = 0) {
	auto path = input.path;
	path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, child_index});
	return path;
}

static FunctionSQLExportResult ExportScalarVerifyFailure(ScalarFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::VERIFY, input.path);
}

static FunctionSQLExportResult ExportAggregateVerifyFailure(AggregateFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::VERIFY, input.path);
}

static FunctionSQLExportResult ExportScalarPlanFailure(ScalarFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::PLAN_EXPORT, input.path);
}

static FunctionSQLExportResult ExportAggregatePlanFailure(AggregateFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::PLAN_EXPORT, input.path);
}

static FunctionSQLExportResult ExportScalarWrongRootFailure(ScalarFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, OtherRootPath(input));
}

static FunctionSQLExportResult ExportAggregateWrongRootFailure(AggregateFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, OtherRootPath(input));
}

static FunctionSQLExportResult ExportScalarSiblingFailure(ScalarFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, SiblingPath(input));
}

static FunctionSQLExportResult ExportAggregateSiblingFailure(AggregateFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, SiblingPath(input));
}

static FunctionSQLExportResult ExportScalarDescendantFailure(ScalarFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, DescendantPath(input));
}

static FunctionSQLExportResult ExportAggregateDescendantFailure(AggregateFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, DescendantPath(input));
}

static FunctionSQLExportResult ExportScalarChildZeroFailure(ScalarFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT,
	                                   DescendantPath(input, 0));
}

static FunctionSQLExportResult ExportScalarChild99Failure(ScalarFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT,
	                                   DescendantPath(input, 99));
}

static FunctionSQLExportResult ExportScalarChildOneFailure(ScalarFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT,
	                                   DescendantPath(input, 1));
}

static FunctionSQLExportResult ExportAggregatePastModifiersFailure(AggregateFunctionSQLExportInput &input) {
	return ExportScopedFunctionFailure(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT,
	                                   DescendantPath(input, 3));
}

template <class INPUT>
static FunctionSQLExportResult ExportMixedFunctionFailure(INPUT &input) {
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(ScopedFunctionIssue(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, input.path));
	issues.push_back(
	    ScopedFunctionIssue(input, LogicalPlanVerificationPhase::EXPRESSION_EXPORT, DescendantPath(input, 99)));
	return FunctionSQLExportResult::Failure(std::move(issues));
}

static FunctionSQLExportResult ExportMixedScalarFailure(ScalarFunctionSQLExportInput &input) {
	return ExportMixedFunctionFailure(input);
}

static FunctionSQLExportResult ExportMixedAggregateFailure(AggregateFunctionSQLExportInput &input) {
	return ExportMixedFunctionFailure(input);
}

static FunctionSQLExportResult ExportScalarFailure(ScalarFunctionSQLExportInput &input) {
	LogicalPlanVerificationIssue issue;
	issue.code = LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE;
	issue.phase = LogicalPlanVerificationPhase::EXPRESSION_EXPORT;
	issue.path = input.path;
	issue.construct = LogicalPlanVerificationConstructIdentity::ExportFeature("synthetic_scalar_export_failure");
	issue.message = "Synthetic scalar SQL export hook rejected the expression";
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(std::move(issue));
	return FunctionSQLExportResult::Failure(std::move(issues));
}

static FunctionSQLExportResult ExportAggregateFailure(AggregateFunctionSQLExportInput &input) {
	LogicalPlanVerificationIssue issue;
	issue.code = LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE;
	issue.phase = LogicalPlanVerificationPhase::EXPRESSION_EXPORT;
	issue.path = input.path;
	issue.construct = LogicalPlanVerificationConstructIdentity::ExportFeature("synthetic_aggregate_export_failure");
	issue.message = "Synthetic aggregate SQL export hook rejected the expression";
	vector<LogicalPlanVerificationIssue> issues;
	issues.push_back(std::move(issue));
	return FunctionSQLExportResult::Failure(std::move(issues));
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

struct PlusOneOperation {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		return static_cast<RESULT_TYPE>(input + 1);
	}
};

template <int32_t OFFSET>
struct OffsetCastOperation {
	template <class SOURCE_TYPE, class TARGET_TYPE>
	static TARGET_TYPE Operation(SOURCE_TYPE input) {
		return static_cast<TARGET_TYPE>(input + OFFSET);
	}
};

static bool IntegerToBigintPlusOne(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	return VectorCastHelpers::TemplatedCastLoop<int32_t, int64_t, OffsetCastOperation<1>>(source, result, count,
	                                                                                      parameters);
}

template <int64_t FINAL_OFFSET>
struct SyntheticSumOperation {
	static bool IgnoreNull() {
		return true;
	}

	static void Initialize(int64_t &state) {
		state = 0;
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP>
	static void Operation(STATE_TYPE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state += input;
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP>
	static void ConstantOperation(STATE_TYPE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state += input * static_cast<INPUT_TYPE>(count);
	}

	template <class STATE_TYPE, class OP>
	static void Combine(const STATE_TYPE &source, STATE_TYPE &target, AggregateInputData &) {
		target += source;
	}

	template <class RESULT_TYPE, class STATE_TYPE>
	static void Finalize(STATE_TYPE &state, RESULT_TYPE &target, AggregateFinalizeData &) {
		target = static_cast<RESULT_TYPE>(state + FINAL_OFFSET);
	}
};

static void ReturnSeven(DataChunk &, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<int32_t>(result)[0] = 7;
}

static void ReturnEight(DataChunk &, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<int32_t>(result)[0] = 8;
}

static unique_ptr<BaseStatistics> MutateScalarDuringStatistics(ClientContext &, FunctionStatisticsInput &input) {
	input.expr.FunctionMutable().SetFunctionCallback(ReturnEight);
	return nullptr;
}

static unique_ptr<BaseStatistics> PreserveScalarDuringStatistics(ClientContext &, FunctionStatisticsInput &) {
	return nullptr;
}

static AggregateFunction SyntheticSum(const Identifier &name, int64_t offset) {
	auto result = offset == 0 ? AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<0>>(
	                                LogicalType::INTEGER, LogicalType::BIGINT)
	                          : AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<1>>(
	                                LogicalType::INTEGER, LogicalType::BIGINT);
	result.SetName(name);
	return result;
}

static unique_ptr<BaseStatistics> MutateAggregateDuringStatistics(ClientContext &, BoundAggregateExpression &expression,
                                                                  AggregateStatisticsInput &) {
	expression.FunctionMutable().ReplaceImplementation(SyntheticSum(Identifier("statistics_sum_plus_one"), 1));
	return nullptr;
}

static unique_ptr<BaseStatistics> PreserveAggregateDuringStatistics(ClientContext &, BoundAggregateExpression &,
                                                                    AggregateStatisticsInput &) {
	return nullptr;
}

static void NameSensitiveScalar(DataChunk &, ExpressionState &state, Vector &result) {
	auto &bound_function = state.expr.Cast<BoundFunctionExpression>().Function();
	auto value = bound_function.GetName() == "mutated_name" ? 8 : 7;
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<int32_t>(result)[0] = value;
}

static unique_ptr<FunctionData> DeserializeAndRenameScalar(Deserializer &, BoundScalarFunction &function) {
	function.SetName(Identifier("mutated_name"));
	return nullptr;
}

struct NameSensitiveSumOperation {
	static bool IgnoreNull() {
		return true;
	}

	static void Initialize(int64_t &state) {
		state = 0;
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP>
	static void Operation(STATE_TYPE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state += input;
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP>
	static void ConstantOperation(STATE_TYPE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state += input * static_cast<INPUT_TYPE>(count);
	}

	template <class STATE_TYPE, class OP>
	static void Combine(const STATE_TYPE &source, STATE_TYPE &target, AggregateInputData &) {
		target += source;
	}

	template <class RESULT_TYPE, class STATE_TYPE>
	static void Finalize(STATE_TYPE &state, RESULT_TYPE &target, AggregateFinalizeData &data) {
		auto offset = data.input.function.GetName() == "mutated_sum" ? 1 : 0;
		target = static_cast<RESULT_TYPE>(state + offset);
	}
};

static AggregateFunction NameSensitiveSum(const Identifier &name) {
	auto result = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, NameSensitiveSumOperation>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	result.SetName(name);
	return result;
}

static unique_ptr<FunctionData> DeserializeAndRenameAggregate(Deserializer &, BoundAggregateFunction &function) {
	function.SetName(Identifier("mutated_sum"));
	return nullptr;
}

static idx_t flipping_scalar_bind_count;
static idx_t flipping_aggregate_bind_count;
static idx_t flipping_bind_expression_count;

static unique_ptr<FunctionData> BindFlippingScalar(BindScalarFunctionInput &input) {
	flipping_scalar_bind_count++;
	if (flipping_scalar_bind_count % 2 == 1) {
		input.GetBoundFunction().SetFunctionCallback(ScalarFunction::UnaryFunction<int32_t, int32_t, PlusOneOperation>);
	} else {
		input.GetBoundFunction().SetFunctionCallback(ScalarFunction::NopFunction);
	}
	return nullptr;
}

static unique_ptr<FunctionData> BindFlippingAggregate(BindAggregateFunctionInput &input) {
	flipping_aggregate_bind_count++;
	auto replacement = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<0>>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	if (flipping_aggregate_bind_count % 2 == 1) {
		replacement = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<1>>(
		    LogicalType::INTEGER, LogicalType::BIGINT);
	}
	replacement.SetName(Identifier("flipping_sum"));
	replacement.SetCatalogName(Identifier::SystemCatalog());
	replacement.SetSchemaName(Identifier("synthetic_provenance_schema"));
	input.GetBoundFunction().ReplaceImplementation(replacement);
	return nullptr;
}

static unique_ptr<Expression> BindFlippingExpression(FunctionBindExpressionInput &input) {
	flipping_bind_expression_count++;
	input.children[0] = make_uniq<BoundConstantExpression>(flipping_bind_expression_count % 2 == 1 ? Value::INTEGER(7)
	                                                                                               : Value::INTEGER(8));
	return nullptr;
}

static void SerializeSyntheticScalar(Serializer &, const optional_ptr<FunctionData>, const BoundScalarFunction &) {
}

static unique_ptr<FunctionData> DeserializeSyntheticScalar(Deserializer &, BoundScalarFunction &) {
	return nullptr;
}

static unique_ptr<FunctionData> DeserializeMutatingScalar(Deserializer &, BoundScalarFunction &function) {
	function.SetFunctionCallback(ScalarFunction::UnaryFunction<int32_t, int32_t, PlusOneOperation>);
	return nullptr;
}

static void SerializeSyntheticAggregate(Serializer &, const optional_ptr<FunctionData>,
                                        const BoundAggregateFunction &) {
}

static unique_ptr<FunctionData> DeserializeSyntheticAggregate(Deserializer &, BoundAggregateFunction &) {
	return nullptr;
}

static unique_ptr<FunctionData> DeserializeMutatingAggregate(Deserializer &, BoundAggregateFunction &function) {
	auto replacement = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<1>>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	replacement.SetName(Identifier("deserialize_sum_plus_one"));
	function.ReplaceImplementation(replacement);
	return nullptr;
}

static void RequireInvalidExpressionTypes(const Expression &expression, const BoundExpressionSQLExportContext &context,
                                          const string &label) {
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	for (auto type : {ExpressionType::INVALID, static_cast<ExpressionType>(255)}) {
		INFO("expression class=" << label << " expression type=" << static_cast<uint32_t>(type));
		auto malformed = expression.Copy();
		malformed->SetExpressionTypeUnsafe(type);
		RequireIssue(BoundExpressionSQLExporter::Export(*malformed, context),
		             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
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
	LogicalPlanVerificationPath standalone_path;
	standalone_path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(missing_result, LogicalPlanVerificationIssueCode::INVALID_BINDING, standalone_path);
	REQUIRE(missing_result.GetIssues()[0].facts.size() == 2);

	auto wrong_type = ResolveBinding(left_binding, {Identifier("src"), Identifier("select")}, LogicalType::BIGINT);
	auto mismatch = BoundExpressionSQLExporter::Export(missing, wrong_type);
	RequireIssue(mismatch, LogicalPlanVerificationIssueCode::TYPE_MISMATCH, standalone_path);
	auto &type_mismatch = *mismatch.GetIssues()[0].construct->type_mismatch;
	REQUIRE(type_mismatch.expected_type == LogicalType::BIGINT);
	REQUIRE(type_mismatch.actual_type == LogicalType::INTEGER);

	BoundColumnRefExpression correlated(LogicalType::INTEGER, left_binding, 1);
	auto correlated_result = BoundExpressionSQLExporter::Export(correlated, context);
	RequireIssue(correlated_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, standalone_path);
	REQUIRE(*correlated_result.GetIssues()[0].construct->identifier == "correlated_column_reference");

	BoundColumnRefExpression invalid_binding(LogicalType::INTEGER, ColumnBinding());
	auto invalid_result = BoundExpressionSQLExporter::Export(invalid_binding, context);
	RequireIssue(invalid_result, LogicalPlanVerificationIssueCode::INVALID_BINDING, standalone_path);

	auto invalid_name = ResolveBinding(left_binding, {Identifier(string("\xFF", 1))}, LogicalType::INTEGER);
	auto invalid_name_result = BoundExpressionSQLExporter::Export(missing, invalid_name);
	RequireIssue(invalid_name_result, LogicalPlanVerificationIssueCode::INVALID_BINDING, standalone_path);

	BoundColumnRefExpression incomplete_type(LogicalType::ANY, left_binding);
	auto incomplete_type_result = BoundExpressionSQLExporter::Export(incomplete_type, context);
	RequireIssue(incomplete_type_result, LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, standalone_path);
	auto incomplete_resolution =
	    ResolveBinding(left_binding, {Identifier("src"), Identifier("select")}, LogicalType::ANY);
	auto incomplete_resolution_result = BoundExpressionSQLExporter::Export(missing, incomplete_resolution);
	RequireIssue(incomplete_resolution_result, LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, standalone_path);
}

TEST_CASE("Bound expression SQL export composes deterministic expression paths", "[bound_expression_sql_export]") {
	auto binding = ColumnBinding(TableIndex(20), ProjectionIndex(3));
	auto expression = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding));
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath root;
	root.root = LogicalPlanVerificationPathRoot::LOGICAL_PLAN;
	root.components.push_back(
	    LogicalPlanVerificationPathComponent {LogicalPlanVerificationPathComponentType::OPERATOR_CHILD, 2});
	root.components.push_back(
	    LogicalPlanVerificationPathComponent {LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION, 4});
	auto result = BoundExpressionSQLExporter::ExportAtPath(*expression, context, root);
	REQUIRE(result.IsValid());
	REQUIRE(result.HasError());
	REQUIRE(result.GetIssues().size() == 2);
	for (idx_t child_index = 0; child_index < 2; child_index++) {
		auto expected = root;
		expected.components.push_back(LogicalPlanVerificationPathComponent {
		    LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, child_index});
		REQUIRE(result.GetIssues()[child_index].path == optional<LogicalPlanVerificationPath>(expected));
	}

	LogicalPlanVerificationPath invalid_root;
	invalid_root.root = LogicalPlanVerificationPathRoot::LOGICAL_PLAN;
	auto invalid = BoundExpressionSQLExporter::ExportAtPath(*expression, context, invalid_root);
	REQUIRE(invalid.IsValid());
	REQUIRE(invalid.HasError());
	REQUIRE(invalid.GetIssues().size() == 1);
	REQUIRE(invalid.GetIssues()[0].code == LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT);
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
	REQUIRE(default_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
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

TEST_CASE("Bound expression SQL export validates durable special-function identity", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto callback_mutation = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(7)), Constant(Value::INTEGER(7)));
	callback_mutation->Cast<BoundFunctionExpression>().FunctionMutable().SetFunctionCallback(
	    ScalarFunction::BinaryFunction<int32_t, int32_t, bool, NotEqualOperation>);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *callback_mutation) == Value::BOOLEAN(false));
	RequireIssue(BoundExpressionSQLExporter::Export(*callback_mutation, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto property_mutation = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_DISTINCT_FROM, Constant(Value(LogicalType::INTEGER)), Constant(Value::INTEGER(1)));
	property_mutation->Cast<BoundFunctionExpression>().FunctionMutable().SetNullHandling(
	    FunctionNullHandling::DEFAULT_NULL_HANDLING);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *property_mutation).IsNull());
	RequireIssue(BoundExpressionSQLExporter::Export(*property_mutation, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto cast_mutation =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	cast_mutation->Cast<BoundFunctionExpression>().FunctionMutable().SetFunctionCallback(ScalarFunction::NopFunction);
	RequireIssue(BoundExpressionSQLExporter::Export(*cast_mutation, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto bound_cast_mutation =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	BoundCastExpression::GetBoundCastMutable(bound_cast_mutation->Cast<BoundFunctionExpression>())
	    .SetFunction(DefaultCasts::TryVectorNullCast);
	RequireIssue(BoundExpressionSQLExporter::Export(*bound_cast_mutation, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto between_mutation = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(2)),
	                                                       Constant(Value::INTEGER(9)), true, true);
	between_mutation->Cast<BoundFunctionExpression>().FunctionMutable().SetFunctionCallback(
	    ScalarFunction::TernaryFunction<int32_t, int32_t, int32_t, bool, OutsideRangeOperation>);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *between_mutation) == Value::BOOLEAN(false));
	RequireIssue(BoundExpressionSQLExporter::Export(*between_mutation, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto equivalent_definition = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(7)), Constant(Value::INTEGER(7)));
	auto equivalent_function = BoundScalarFunction(make_shared_ptr<ScalarFunction>(
	    *equivalent_definition->Cast<BoundFunctionExpression>().Function().GetDefinition()));
	equivalent_definition->Cast<BoundFunctionExpression>().FunctionMutable() = std::move(equivalent_function);
	RequireIssue(BoundExpressionSQLExporter::Export(*equivalent_definition, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto equivalent_cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	auto equivalent_cast_function = BoundScalarFunction(
	    make_shared_ptr<ScalarFunction>(*equivalent_cast->Cast<BoundFunctionExpression>().Function().GetDefinition()));
	equivalent_cast->Cast<BoundFunctionExpression>().FunctionMutable() = std::move(equivalent_cast_function);
	RequireIssue(BoundExpressionSQLExporter::Export(*equivalent_cast, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	auto equivalent_between = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(2)),
	                                                         Constant(Value::INTEGER(9)), true, true);
	auto equivalent_between_function = BoundScalarFunction(make_shared_ptr<ScalarFunction>(
	    *equivalent_between->Cast<BoundFunctionExpression>().Function().GetDefinition()));
	equivalent_between->Cast<BoundFunctionExpression>().FunctionMutable() = std::move(equivalent_between_function);
	RequireIssue(BoundExpressionSQLExporter::Export(*equivalent_between, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto comparison = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(7)),
	                                                    Constant(Value::INTEGER(7)));
	auto serialized_comparison = BinaryRoundTrip(*connection.context, *comparison);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *serialized_comparison) == Value::BOOLEAN(true));
	RequireRoundTrip(connection, *serialized_comparison, context, string(), "CAST(7 AS INTEGER) = CAST(7 AS INTEGER)");

	auto cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	auto serialized_cast = BinaryRoundTrip(*connection.context, *cast);
	auto cast_value = ExpressionExecutor::EvaluateScalar(*connection.context, *serialized_cast);
	REQUIRE(cast_value.type() == LogicalType::BIGINT);
	REQUIRE(cast_value == Value::BIGINT(7));
	RequireRoundTrip(connection, *serialized_cast, context, string(), "CAST(7 AS BIGINT)");

	auto between = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(2)),
	                                              Constant(Value::INTEGER(9)), true, true);
	auto serialized_between = BinaryRoundTrip(*connection.context, *between);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *serialized_between) == Value::BOOLEAN(true));
	RequireRoundTrip(connection, *serialized_between, context, string(),
	                 "CAST(2 AS INTEGER) BETWEEN CAST(2 AS INTEGER) AND CAST(9 AS INTEGER)");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export validates generic function provenance", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_provenance_extension");
	loader.UseDedicatedSchemaForExtension(Identifier("synthetic_provenance_schema"));

	ScalarFunction identity(Identifier("synthetic_identity"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                        ScalarFunction::NopFunction);
	loader.RegisterFunction(identity);
	identity.SetCatalogName(Identifier::SystemCatalog());
	identity.SetSchemaName(Identifier("synthetic_provenance_schema"));

	auto sum = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<0>>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	sum.SetName(Identifier("synthetic_sum"));
	loader.RegisterFunction(sum);
	sum.SetCatalogName(Identifier::SystemCatalog());
	sum.SetSchemaName(Identifier("synthetic_provenance_schema"));

	auto sum_plus_one = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<1>>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	sum_plus_one.SetName(Identifier("synthetic_sum_plus_one"));
	loader.RegisterFunction(sum_plus_one);

	ScalarFunction flipping_scalar(Identifier("flipping_bind"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                               ScalarFunction::NopFunction, BindFlippingScalar);
	loader.RegisterFunction(std::move(flipping_scalar));
	ScalarFunction flipping_bind_expression(Identifier("flipping_bind_expression"), {LogicalType::INTEGER},
	                                        LogicalType::INTEGER, ScalarFunction::NopFunction);
	flipping_bind_expression.SetBindExpressionCallback(BindFlippingExpression);
	loader.RegisterFunction(std::move(flipping_bind_expression));
	auto flipping_aggregate = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<0>>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	flipping_aggregate.SetName(Identifier("flipping_sum"));
	flipping_aggregate.SetBindCallback(BindFlippingAggregate);
	loader.RegisterFunction(std::move(flipping_aggregate));
	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();

	BoundExpressionSQLExportContext context;
	auto catalog_plan =
	    BindExportQuery(connection, "SELECT synthetic_provenance_schema.synthetic_identity(CAST(7 AS INTEGER)), "
	                                "synthetic_provenance_schema.synthetic_sum(CAST(7 AS INTEGER))");
	auto catalog_scalar = FindExpression(*catalog_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "synthetic_identity";
	});
	REQUIRE(catalog_scalar);
	REQUIRE(catalog_scalar->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	auto serialized_scalar = BinaryRoundTrip(*connection.context, *catalog_scalar);
	REQUIRE(serialized_scalar->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireRoundTrip(connection, *serialized_scalar, context, string(),
	                 "synthetic_provenance_schema.synthetic_identity(CAST(7 AS INTEGER))");

	auto catalog_aggregate = FindExpression(*catalog_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       expression.Cast<BoundAggregateExpression>().Function().GetName() == "synthetic_sum";
	});
	REQUIRE(catalog_aggregate);
	REQUIRE(catalog_aggregate->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	auto serialized_aggregate = BinaryRoundTrip(*connection.context, *catalog_aggregate);
	REQUIRE(serialized_aggregate->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	RequireRoundTrip(connection, *serialized_aggregate, context, string(),
	                 "synthetic_provenance_schema.synthetic_sum(CAST(7 AS INTEGER))");

	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	auto &catalog = Catalog::GetSystemCatalog(*connection.context);
	auto &schema = catalog.GetSchema(*connection.context, Identifier::DefaultSchema());
	FunctionBinder function_binder(*connection.context);

	flipping_scalar_bind_count = 0;
	auto flipping_scalar_result =
	    connection.Query("SELECT synthetic_provenance_schema.flipping_bind(CAST(7 AS INTEGER))");
	REQUIRE_FALSE(flipping_scalar_result->HasError());
	REQUIRE(flipping_scalar_result->GetValue(0, 0) == Value::INTEGER(8));
	REQUIRE(flipping_scalar_bind_count == 1);
	flipping_scalar_bind_count = 0;
	auto &flipping_scalar_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    *connection.context,
	    QualifiedName(catalog.GetName(), Identifier("synthetic_provenance_schema"), Identifier("flipping_bind")));
	vector<unique_ptr<Expression>> flipping_scalar_children;
	flipping_scalar_children.push_back(Constant(Value::INTEGER(7)));
	ErrorData flipping_scalar_error;
	auto flipping_scalar_expression = function_binder.BindScalarFunction(
	    flipping_scalar_entry, std::move(flipping_scalar_children), flipping_scalar_error);
	REQUIRE(flipping_scalar_expression);
	REQUIRE(flipping_scalar_bind_count == 1);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *flipping_scalar_expression) == Value::INTEGER(8));
	REQUIRE(flipping_scalar_expression->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*flipping_scalar_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto serialized_flipping_scalar = BinaryRoundTrip(*connection.context, *flipping_scalar_expression);
	REQUIRE(serialized_flipping_scalar->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*serialized_flipping_scalar, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	flipping_bind_expression_count = 0;
	auto &flipping_bind_expression_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    *connection.context, QualifiedName(catalog.GetName(), Identifier("synthetic_provenance_schema"),
	                                       Identifier("flipping_bind_expression")));
	vector<unique_ptr<Expression>> flipping_bind_expression_children;
	flipping_bind_expression_children.push_back(Constant(Value::INTEGER(0)));
	ErrorData flipping_bind_expression_error;
	auto flipping_bind_expression_result = function_binder.BindScalarFunction(
	    flipping_bind_expression_entry, std::move(flipping_bind_expression_children), flipping_bind_expression_error);
	REQUIRE(flipping_bind_expression_result);
	REQUIRE(flipping_bind_expression_count == 1);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *flipping_bind_expression_result) ==
	        Value::INTEGER(7));
	REQUIRE(flipping_bind_expression_result->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(*flipping_bind_expression_result, context).IsSuccess());
	auto serialized_flipping_bind_expression = BinaryRoundTrip(*connection.context, *flipping_bind_expression_result);
	REQUIRE(flipping_bind_expression_count == 2);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *serialized_flipping_bind_expression) ==
	        Value::INTEGER(8));
	REQUIRE(serialized_flipping_bind_expression->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(*serialized_flipping_bind_expression, context).IsSuccess());

	flipping_aggregate_bind_count = 0;
	auto flipping_aggregate_result =
	    connection.Query("SELECT synthetic_provenance_schema.flipping_sum(CAST(7 AS INTEGER))");
	REQUIRE_FALSE(flipping_aggregate_result->HasError());
	REQUIRE(flipping_aggregate_result->GetValue(0, 0) == Value::BIGINT(8));
	REQUIRE(flipping_aggregate_bind_count == 1);
	flipping_aggregate_bind_count = 0;
	auto &flipping_aggregate_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    *connection.context,
	    QualifiedName(catalog.GetName(), Identifier("synthetic_provenance_schema"), Identifier("flipping_sum")));
	vector<pair<Identifier, unique_ptr<Expression>>> flipping_aggregate_children;
	flipping_aggregate_children.emplace_back(Identifier(), Constant(Value::INTEGER(7)));
	ErrorData flipping_aggregate_error;
	auto flipping_aggregate_expression = function_binder.BindAggregateFunction(
	    flipping_aggregate_entry, std::move(flipping_aggregate_children), flipping_aggregate_error);
	REQUIRE(flipping_aggregate_expression);
	REQUIRE(flipping_aggregate_bind_count == 1);
	auto plus_one_aggregate = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<1>>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	REQUIRE(flipping_aggregate_expression->Function().GetCallbacks() == plus_one_aggregate.GetCallbacks());
	REQUIRE(flipping_aggregate_expression->GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*flipping_aggregate_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto serialized_flipping_aggregate = BinaryRoundTrip(*connection.context, *flipping_aggregate_expression);
	REQUIRE(serialized_flipping_aggregate->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*serialized_flipping_aggregate, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	ScalarFunction forged_abs(Identifier("abs"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                          ScalarFunction::UnaryFunction<int32_t, int32_t, PlusOneOperation>);
	CreateScalarFunctionInfo forged_abs_info(std::move(forged_abs));
	ScalarFunctionCatalogEntry forged_abs_entry(catalog, schema, forged_abs_info);
	vector<unique_ptr<Expression>> forged_abs_children;
	forged_abs_children.push_back(Constant(Value::INTEGER(7)));
	ErrorData forged_abs_error;
	auto forged_abs_expression =
	    function_binder.BindScalarFunction(forged_abs_entry, std::move(forged_abs_children), forged_abs_error);
	REQUIRE(forged_abs_expression);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *forged_abs_expression) == Value::INTEGER(8));
	REQUIRE_FALSE(forged_abs_expression->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*forged_abs_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto &abs_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    *connection.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("abs")));
	vector<unique_ptr<Expression>> abs_children;
	abs_children.push_back(Constant(Value::INTEGER(7)));
	ErrorData abs_error;
	auto abs_expression = function_binder.BindScalarFunction(abs_entry, std::move(abs_children), abs_error);
	REQUIRE(abs_expression);
	REQUIRE(abs_expression->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireRoundTrip(connection, *abs_expression, context, string(), "abs(CAST(7 AS INTEGER))");
	auto &abs_definition = abs_expression->Cast<BoundFunctionExpression>().Function().GetDefinition();
	auto forged_abs_with_live_overload = ScalarFunction(*abs_definition);
	CreateScalarFunctionInfo forged_abs_with_live_overload_info(std::move(forged_abs_with_live_overload));
	ScalarFunctionCatalogEntry forged_abs_with_live_overload_entry(catalog, schema, forged_abs_with_live_overload_info);
	forged_abs_with_live_overload_entry.functions.functions[0] = abs_definition;
	vector<unique_ptr<Expression>> forged_abs_with_live_overload_children;
	forged_abs_with_live_overload_children.push_back(Constant(Value::INTEGER(7)));
	ErrorData forged_abs_with_live_overload_error;
	auto forged_abs_with_live_overload_expression = function_binder.BindScalarFunction(
	    forged_abs_with_live_overload_entry, std::move(forged_abs_with_live_overload_children),
	    forged_abs_with_live_overload_error);
	REQUIRE(forged_abs_with_live_overload_expression);
	REQUIRE(forged_abs_with_live_overload_expression->Cast<BoundFunctionExpression>().Function().GetDefinition() ==
	        abs_definition);
	REQUIRE_FALSE(forged_abs_with_live_overload_expression->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*forged_abs_with_live_overload_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto &abs_operator_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    *connection.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("@")));
	vector<unique_ptr<Expression>> abs_operator_children;
	abs_operator_children.push_back(Constant(Value::INTEGER(7)));
	ErrorData abs_operator_error;
	auto abs_operator_expression =
	    function_binder.BindScalarFunction(abs_operator_entry, std::move(abs_operator_children), abs_operator_error);
	REQUIRE(abs_operator_expression);
	auto &abs_operator_bound_expression = abs_operator_expression->Cast<BoundFunctionExpression>();
	auto &abs_operator_function = abs_operator_bound_expression.Function();
	REQUIRE(abs_operator_bound_expression.GetSQLExportRecipe());
	REQUIRE(abs_operator_function.GetDefinition()->GetName() == "@");
	REQUIRE(abs_operator_function.GetDefinition() != abs_definition);

	ScalarFunction scalar_impostor(Identifier("synthetic_identity"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                               ScalarFunction::UnaryFunction<int32_t, int32_t, PlusOneOperation>);
	scalar_impostor.SetCatalogName(Identifier::SystemCatalog());
	scalar_impostor.SetSchemaName(Identifier("synthetic_provenance_schema"));
	vector<unique_ptr<Expression>> scalar_children;
	scalar_children.push_back(Constant(Value::INTEGER(7)));
	auto scalar = scalar_impostor.Bind(*connection.context, std::move(scalar_children));
	REQUIRE_FALSE(scalar->GetSQLExportRecipe());
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *scalar) == Value::INTEGER(8));
	RequireIssue(BoundExpressionSQLExporter::Export(*scalar, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto serialized_scalar_impostor = BinaryRoundTrip(*connection.context, *scalar);
	REQUIRE_FALSE(serialized_scalar_impostor->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*serialized_scalar_impostor, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto mutated_scalar_expression = catalog_scalar->Copy();
	auto &mutated_scalar = mutated_scalar_expression->Cast<BoundFunctionExpression>();
	REQUIRE(mutated_scalar.GetSQLExportRecipe());
	mutated_scalar.FunctionMutable().SetFunctionCallback(
	    ScalarFunction::UnaryFunction<int32_t, int32_t, PlusOneOperation>);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, mutated_scalar) == Value::INTEGER(8));
	RequireIssue(BoundExpressionSQLExporter::Export(mutated_scalar, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto changed_definition_expression = catalog_scalar->Copy();
	auto &changed_definition = changed_definition_expression->Cast<BoundFunctionExpression>();
	auto replacement_definition = make_shared_ptr<ScalarFunction>(identity);
	replacement_definition->SetName(Identifier("synthetic_sum_plus_one"));
	changed_definition.FunctionMutable().SetDefinition(std::move(replacement_definition));
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, changed_definition) == Value::INTEGER(7));
	RequireIssue(BoundExpressionSQLExporter::Export(changed_definition, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto ordinary_result = connection.Query("SELECT synthetic_provenance_schema.synthetic_sum(CAST(7 AS INTEGER))");
	REQUIRE_FALSE(ordinary_result->HasError());
	REQUIRE(ordinary_result->GetValue(0, 0) == Value::BIGINT(7));
	auto replacement_result =
	    connection.Query("SELECT synthetic_provenance_schema.synthetic_sum_plus_one(CAST(7 AS INTEGER))");
	REQUIRE_FALSE(replacement_result->HasError());
	REQUIRE(replacement_result->GetValue(0, 0) == Value::BIGINT(8));

	auto forged_sum = sum_plus_one;
	forged_sum.SetName(Identifier("sum"));
	CreateAggregateFunctionInfo forged_sum_info(std::move(forged_sum));
	AggregateFunctionCatalogEntry forged_sum_entry(catalog, schema, forged_sum_info);
	vector<pair<Identifier, unique_ptr<Expression>>> forged_sum_children;
	forged_sum_children.emplace_back(Identifier(), Constant(Value::INTEGER(7)));
	ErrorData forged_sum_error;
	auto forged_sum_expression =
	    function_binder.BindAggregateFunction(forged_sum_entry, std::move(forged_sum_children), forged_sum_error);
	REQUIRE(forged_sum_expression);
	REQUIRE(forged_sum_expression->Function().GetCallbacks() == sum_plus_one.GetCallbacks());
	REQUIRE_FALSE(forged_sum_expression->GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*forged_sum_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto &sum_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    *connection.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("sum")));
	vector<pair<Identifier, unique_ptr<Expression>>> sum_children;
	sum_children.emplace_back(Identifier(), Constant(Value::INTEGER(7)));
	ErrorData sum_error;
	auto sum_expression = function_binder.BindAggregateFunction(sum_entry, std::move(sum_children), sum_error);
	REQUIRE(sum_expression);
	REQUIRE(sum_expression->GetSQLExportRecipe());
	RequireRoundTrip(connection, *sum_expression, context, string(), "sum(CAST(7 AS INTEGER))");
	auto sum_definition = sum_expression->Function().GetDefinition();
	auto forged_sum_with_live_overload = AggregateFunction(*sum_definition);
	CreateAggregateFunctionInfo forged_sum_with_live_overload_info(std::move(forged_sum_with_live_overload));
	AggregateFunctionCatalogEntry forged_sum_with_live_overload_entry(catalog, schema,
	                                                                  forged_sum_with_live_overload_info);
	forged_sum_with_live_overload_entry.functions.functions[0] = sum_definition;
	vector<pair<Identifier, unique_ptr<Expression>>> forged_sum_with_live_overload_children;
	forged_sum_with_live_overload_children.emplace_back(Identifier(), Constant(Value::INTEGER(7)));
	ErrorData forged_sum_with_live_overload_error;
	auto forged_sum_with_live_overload_expression = function_binder.BindAggregateFunction(
	    forged_sum_with_live_overload_entry, std::move(forged_sum_with_live_overload_children),
	    forged_sum_with_live_overload_error);
	REQUIRE(forged_sum_with_live_overload_expression);
	REQUIRE(forged_sum_with_live_overload_expression->Function().GetDefinition() == sum_definition);
	REQUIRE_FALSE(forged_sum_with_live_overload_expression->GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*forged_sum_with_live_overload_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto aggregate_impostor = sum_plus_one;
	aggregate_impostor.SetName(Identifier("synthetic_sum"));
	aggregate_impostor.SetCatalogName(Identifier::SystemCatalog());
	aggregate_impostor.SetSchemaName(Identifier("synthetic_provenance_schema"));
	vector<unique_ptr<Expression>> aggregate_children;
	aggregate_children.push_back(Constant(Value::INTEGER(7)));
	auto aggregate = aggregate_impostor.Bind(*connection.context, std::move(aggregate_children));
	REQUIRE_FALSE(aggregate->GetSQLExportRecipe());
	REQUIRE(aggregate->Function().GetCallbacks() == sum_plus_one.GetCallbacks());
	REQUIRE(aggregate->Function().GetCallbacks() != sum.GetCallbacks());
	RequireIssue(BoundExpressionSQLExporter::Export(*aggregate, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto serialized_aggregate_impostor = BinaryRoundTrip(*connection.context, *aggregate);
	REQUIRE_FALSE(serialized_aggregate_impostor->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*serialized_aggregate_impostor, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto mutated_aggregate_expression = catalog_aggregate->Copy();
	auto &mutated_aggregate = mutated_aggregate_expression->Cast<BoundAggregateExpression>();
	REQUIRE(mutated_aggregate.GetSQLExportRecipe());
	mutated_aggregate.FunctionMutable().SetCallbacks(sum_plus_one.GetCallbacks());
	REQUIRE(mutated_aggregate.Function().GetCallbacks() == sum_plus_one.GetCallbacks());
	REQUIRE(mutated_aggregate.Function().GetCallbacks() !=
	        mutated_aggregate.Function().GetDefinition()->GetCallbacks());
	RequireIssue(BoundExpressionSQLExporter::Export(mutated_aggregate, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export recipes do not transfer through copied functions",
          "[bound_expression_sql_export][function_sql_export_recipe]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_function_copy_extension");
	const Identifier schema_name("synthetic_function_copy_schema");
	loader.UseDedicatedSchemaForExtension(schema_name);
	loader.RegisterFunction(
	    ScalarFunction(Identifier("copy_name_sensitive"), {}, LogicalType::INTEGER, NameSensitiveScalar));
	loader.RegisterFunction(NameSensitiveSum(Identifier("copy_name_sensitive_sum")));
	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();

	auto plan = BindExportQuery(connection, "SELECT " + schema_name.GetIdentifierName() + ".copy_name_sensitive(), " +
	                                            schema_name.GetIdentifierName() +
	                                            ".copy_name_sensitive_sum(CAST(7 AS INTEGER))");
	auto scalar = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
	});
	auto aggregate = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE;
	});
	REQUIRE(scalar);
	REQUIRE(aggregate);
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto scalar_expression_copy = scalar->Copy();
	REQUIRE(scalar_expression_copy->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(*scalar_expression_copy, context).IsSuccess());
	auto copied_bound_scalar = scalar->Cast<BoundFunctionExpression>().Function();
	copied_bound_scalar.SetName(Identifier("mutated_name"));
	BoundFunctionExpression copied_scalar_expression(std::move(copied_bound_scalar), {}, nullptr);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, copied_scalar_expression) == Value::INTEGER(8));
	REQUIRE_FALSE(copied_scalar_expression.GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(copied_scalar_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto copied_scalar_definition = *scalar->Cast<BoundFunctionExpression>().Function().GetDefinition();
	copied_scalar_definition.SetName(Identifier("mutated_name"));
	auto public_scalar_expression = copied_scalar_definition.Bind(*connection.context, {});
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *public_scalar_expression) == Value::INTEGER(8));
	REQUIRE_FALSE(public_scalar_expression->GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*public_scalar_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto aggregate_expression_copy = aggregate->Copy();
	REQUIRE(aggregate_expression_copy->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(*aggregate_expression_copy, context).IsSuccess());
	auto &bound_aggregate = aggregate->Cast<BoundAggregateExpression>();
	auto copied_bound_aggregate = bound_aggregate.Function();
	copied_bound_aggregate.SetName(Identifier("mutated_sum"));
	vector<unique_ptr<Expression>> copied_aggregate_children;
	for (auto &child : bound_aggregate.GetChildren()) {
		copied_aggregate_children.push_back(child->Copy());
	}
	BoundAggregateExpression copied_aggregate_expression(std::move(copied_bound_aggregate),
	                                                     std::move(copied_aggregate_children), nullptr, nullptr,
	                                                     AggregateType::NON_DISTINCT);
	REQUIRE(copied_aggregate_expression.Function().GetName() == "mutated_sum");
	REQUIRE_FALSE(copied_aggregate_expression.GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(copied_aggregate_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto aggregate_mutation = aggregate->Copy();
	auto &aggregate_mutation_bound = aggregate_mutation->Cast<BoundAggregateExpression>();
	REQUIRE(aggregate_mutation_bound.GetSQLExportRecipe());
	aggregate_mutation_bound.FunctionMutable().SetName(Identifier("mutated_sum"));
	REQUIRE_FALSE(aggregate_mutation_bound.GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(aggregate_mutation_bound, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export uses canonical read-only function hooks",
          "[bound_expression_sql_export][function_sql_export_hook]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_sql_export_hook_extension");
	const Identifier schema_name("synthetic_sql_export_hook_schema");
	loader.UseDedicatedSchemaForExtension(schema_name);

	auto register_scalar_arguments = [&](const Identifier &name, vector<LogicalType> arguments,
	                                     scalar_function_sql_export_t callback) {
		ScalarFunction function(name, std::move(arguments), LogicalType::INTEGER, ScalarFunction::NopFunction,
		                        BindOpaqueSQLFunction);
		if (callback) {
			function.SetSQLExportCallback(callback);
		}
		loader.RegisterFunction(std::move(function));
	};
	auto register_scalar = [&](const Identifier &name, scalar_function_sql_export_t callback) {
		register_scalar_arguments(name, {LogicalType::INTEGER, LogicalType::INTEGER}, callback);
	};
	register_scalar(Identifier("hooked_scalar"), ExportScalarFirstChild);
	register_scalar(Identifier("missing_scalar_hook"), nullptr);
	register_scalar(Identifier("malformed_scalar_hook"), ExportMalformedScalar);
	register_scalar(Identifier("throwing_scalar_hook"), ExportThrowingScalar);
	register_scalar(Identifier("bad_alloc_scalar_hook"), ExportBadAllocScalar);
	register_scalar(Identifier("empty_scalar_failure_hook"), ExportEmptyScalarFailure);
	register_scalar(Identifier("invalid_scalar_failure_hook"), ExportInvalidScalarFailure);
	register_scalar(Identifier("verify_scalar_failure_hook"), ExportScalarVerifyFailure);
	register_scalar(Identifier("plan_scalar_failure_hook"), ExportScalarPlanFailure);
	register_scalar(Identifier("wrong_root_scalar_failure_hook"), ExportScalarWrongRootFailure);
	register_scalar(Identifier("sibling_scalar_failure_hook"), ExportScalarSiblingFailure);
	register_scalar(Identifier("descendant_scalar_failure_hook"), ExportScalarDescendantFailure);
	register_scalar_arguments(Identifier("zero_child_zero_scalar_failure_hook"), {}, ExportScalarChildZeroFailure);
	register_scalar_arguments(Identifier("zero_child_99_scalar_failure_hook"), {}, ExportScalarChild99Failure);
	register_scalar_arguments(Identifier("one_child_one_scalar_failure_hook"), {LogicalType::INTEGER},
	                          ExportScalarChildOneFailure);
	register_scalar(Identifier("mixed_scalar_failure_hook"), ExportMixedScalarFailure);
	register_scalar(Identifier("failing_scalar_hook"), ExportScalarFailure);

	auto register_aggregate = [&](const Identifier &name, aggregate_function_sql_export_t callback) {
		auto function = SyntheticSum(name, 0);
		function.SetBindCallback(BindOpaqueSQLAggregate);
		if (callback) {
			function.SetSQLExportCallback(callback);
		}
		loader.RegisterFunction(std::move(function));
	};
	register_aggregate(Identifier("hooked_sum"), ExportAggregateAsSum);
	register_aggregate(Identifier("missing_sum_hook"), nullptr);
	register_aggregate(Identifier("malformed_sum_hook"), ExportMalformedAggregate);
	register_aggregate(Identifier("throwing_sum_hook"), ExportThrowingAggregate);
	register_aggregate(Identifier("empty_sum_failure_hook"), ExportEmptyAggregateFailure);
	register_aggregate(Identifier("invalid_sum_failure_hook"), ExportInvalidAggregateFailure);
	register_aggregate(Identifier("verify_sum_failure_hook"), ExportAggregateVerifyFailure);
	register_aggregate(Identifier("plan_sum_failure_hook"), ExportAggregatePlanFailure);
	register_aggregate(Identifier("wrong_root_sum_failure_hook"), ExportAggregateWrongRootFailure);
	register_aggregate(Identifier("sibling_sum_failure_hook"), ExportAggregateSiblingFailure);
	register_aggregate(Identifier("descendant_sum_failure_hook"), ExportAggregateDescendantFailure);
	register_aggregate(Identifier("past_modifiers_sum_failure_hook"), ExportAggregatePastModifiersFailure);
	register_aggregate(Identifier("mixed_sum_failure_hook"), ExportMixedAggregateFailure);
	register_aggregate(Identifier("failing_sum_hook"), ExportAggregateFailure);
	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();

	auto bind_scalar_arguments = [&](const string &name, const string &arguments) {
		auto plan = BindExportQuery(connection,
		                            "SELECT " + schema_name.GetIdentifierName() + "." + name + "(" + arguments + ")");
		auto expression = FindExpression(*plan, [&](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
			       candidate.Cast<BoundFunctionExpression>().Function().GetName() == name;
		});
		REQUIRE(expression);
		return expression->Copy();
	};
	auto bind_scalar = [&](const string &name) {
		return bind_scalar_arguments(name, "CAST(7 AS INTEGER), CAST(99 AS INTEGER)");
	};
	auto bind_aggregate = [&](const string &name) {
		auto plan = BindExportQuery(connection,
		                            "SELECT " + schema_name.GetIdentifierName() + "." + name + "(CAST(7 AS INTEGER))");
		auto expression = FindExpression(*plan, [&](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
			       candidate.Cast<BoundAggregateExpression>().Function().GetName() == name;
		});
		REQUIRE(expression);
		return expression->Copy();
	};

	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath root_path;
	root_path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	auto hooked_scalar = bind_scalar("hooked_scalar");
	auto &hooked_scalar_bound = hooked_scalar->Cast<BoundFunctionExpression>();
	REQUIRE(hooked_scalar_bound.BindInfo());
	REQUIRE(hooked_scalar_bound.GetSQLExportRecipe());
	auto scalar_copy = hooked_scalar->Copy();
	REQUIRE(scalar_copy->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	auto serialized_hooked_scalar = BinaryRoundTrip(*connection.context, *hooked_scalar);
	REQUIRE(serialized_hooked_scalar->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(*serialized_hooked_scalar, context).IsSuccess());
	auto scalar_result = BoundExpressionSQLExporter::Export(*hooked_scalar, context);
	REQUIRE(scalar_result.IsSuccess());
	REQUIRE(scalar_result.GetValue()->GetExpressionClass() == ExpressionClass::CAST);
	hooked_scalar.reset();
	auto scalar_rebound = connection.Query("SELECT " + scalar_result.GetValue()->ToString());
	REQUIRE_FALSE(scalar_rebound->HasError());
	REQUIRE(scalar_rebound->GetValue(0, 0) == Value::INTEGER(7));

	auto bind_info_mutation = scalar_copy->Copy();
	auto &bind_info_mutation_bound = bind_info_mutation->Cast<BoundFunctionExpression>();
	bind_info_mutation_bound.BindInfoMutable().reset();
	REQUIRE_FALSE(bind_info_mutation_bound.GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(bind_info_mutation_bound, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path);

	auto missing_scalar = bind_scalar("missing_scalar_hook");
	REQUIRE(missing_scalar->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*missing_scalar, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path);
	auto malformed_scalar = bind_scalar("malformed_scalar_hook");
	RequireIssue(BoundExpressionSQLExporter::Export(*malformed_scalar, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, root_path);
	auto throwing_scalar = bind_scalar("throwing_scalar_hook");
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(*throwing_scalar, context), InvalidInputException);
	auto bad_alloc_scalar = bind_scalar("bad_alloc_scalar_hook");
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(*bad_alloc_scalar, context), std::bad_alloc);
	auto empty_scalar_failure = bind_scalar("empty_scalar_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*empty_scalar_failure, context), root_path);
	auto invalid_scalar_failure = bind_scalar("invalid_scalar_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*invalid_scalar_failure, context), root_path);
	auto verify_scalar_failure = bind_scalar("verify_scalar_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*verify_scalar_failure, context), root_path);
	auto plan_scalar_failure = bind_scalar("plan_scalar_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*plan_scalar_failure, context), root_path);
	auto wrong_root_scalar_failure = bind_scalar("wrong_root_scalar_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*wrong_root_scalar_failure, context), root_path);
	auto zero_child_zero_scalar_failure = bind_scalar_arguments("zero_child_zero_scalar_failure_hook", "");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*zero_child_zero_scalar_failure, context), root_path);
	auto zero_child_99_scalar_failure = bind_scalar_arguments("zero_child_99_scalar_failure_hook", "");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*zero_child_99_scalar_failure, context), root_path);
	auto one_child_one_scalar_failure =
	    bind_scalar_arguments("one_child_one_scalar_failure_hook", "CAST(7 AS INTEGER)");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*one_child_one_scalar_failure, context), root_path);
	auto mixed_scalar_failure = bind_scalar("mixed_scalar_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*mixed_scalar_failure, context), root_path);
	auto sibling_scalar_failure = bind_scalar("sibling_scalar_failure_hook");
	auto nested_sibling_scalar_failure = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, std::move(sibling_scalar_failure), Constant(Value::INTEGER(7)));
	auto scalar_hook_path = root_path;
	scalar_hook_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0});
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*nested_sibling_scalar_failure, context),
	                         scalar_hook_path);
	auto descendant_scalar_failure = bind_scalar("descendant_scalar_failure_hook");
	auto scalar_descendant_path = root_path;
	scalar_descendant_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0});
	auto scalar_descendant_result = BoundExpressionSQLExporter::Export(*descendant_scalar_failure, context);
	RequireIssue(scalar_descendant_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION,
	             scalar_descendant_path);
	REQUIRE(scalar_descendant_result.GetIssues()[0].construct);
	REQUIRE(scalar_descendant_result.GetIssues()[0].construct->type == LogicalPlanVerificationConstructType::FUNCTION);
	auto nested_descendant_scalar_failure = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, bind_scalar("descendant_scalar_failure_hook"), Constant(Value::INTEGER(7)));
	auto nested_scalar_descendant_path = scalar_hook_path;
	nested_scalar_descendant_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0});
	auto nested_scalar_descendant_result =
	    BoundExpressionSQLExporter::Export(*nested_descendant_scalar_failure, context);
	RequireIssue(nested_scalar_descendant_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION,
	             nested_scalar_descendant_path);
	auto exact_scalar_failure = bind_scalar("failing_scalar_hook");
	auto exact_scalar_result = BoundExpressionSQLExporter::Export(*exact_scalar_failure, context);
	RequireIssue(exact_scalar_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, root_path);
	REQUIRE(exact_scalar_result.GetIssues()[0].construct);
	REQUIRE(exact_scalar_result.GetIssues()[0].construct->type == LogicalPlanVerificationConstructType::EXPORT_FEATURE);

	auto modified_aggregate_plan =
	    BindExportQuery(connection, "SELECT " + schema_name.GetIdentifierName() +
	                                    ".hooked_sum(DISTINCT i ORDER BY i DESC NULLS FIRST) FILTER (WHERE i > 0) "
	                                    "FROM (VALUES (1), (2), (2)) AS v(i)");
	auto modified_aggregate = FindExpression(*modified_aggregate_plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       candidate.Cast<BoundAggregateExpression>().Function().GetName() == "hooked_sum";
	});
	REQUIRE(modified_aggregate);
	auto modified_source = CreateSyntheticSQLSource(*modified_aggregate);
	auto modified_result = BoundExpressionSQLExporter::Export(*modified_aggregate, modified_source.context);
	REQUIRE(modified_result.IsSuccess());
	auto &modified_function = modified_result.GetValue()->Cast<FunctionExpression>();
	REQUIRE(modified_function.Distinct());
	REQUIRE(modified_function.Filter());
	REQUIRE(modified_function.OrderBy());
	REQUIRE(modified_function.OrderBy()->orders.size() == 1);
	auto modified_rebound =
	    connection.Query("SELECT " + modified_result.GetValue()->ToString() + modified_source.from_clause);
	REQUIRE_FALSE(modified_rebound->HasError());
	REQUIRE(modified_rebound->GetValue(0, 0) == Value::HUGEINT(3));

	auto hooked_aggregate = bind_aggregate("hooked_sum");
	auto &hooked_aggregate_bound = hooked_aggregate->Cast<BoundAggregateExpression>();
	REQUIRE(hooked_aggregate_bound.BindInfo());
	REQUIRE(hooked_aggregate_bound.GetSQLExportRecipe());
	auto aggregate_copy = hooked_aggregate->Copy();
	REQUIRE(aggregate_copy->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	auto serialized_hooked_aggregate = BinaryRoundTrip(*connection.context, *hooked_aggregate);
	REQUIRE(serialized_hooked_aggregate->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(*serialized_hooked_aggregate, context).IsSuccess());
	auto aggregate_result = BoundExpressionSQLExporter::Export(*hooked_aggregate, context);
	REQUIRE(aggregate_result.IsSuccess());
	REQUIRE(aggregate_result.GetValue()->Cast<FunctionExpression>().FunctionName() == "sum");
	hooked_aggregate.reset();
	auto aggregate_rebound = connection.Query("SELECT " + aggregate_result.GetValue()->ToString());
	REQUIRE_FALSE(aggregate_rebound->HasError());
	REQUIRE(aggregate_rebound->GetValue(0, 0) == Value::HUGEINT(7));

	auto missing_aggregate = bind_aggregate("missing_sum_hook");
	REQUIRE(missing_aggregate->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*missing_aggregate, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path);
	auto malformed_aggregate = bind_aggregate("malformed_sum_hook");
	RequireIssue(BoundExpressionSQLExporter::Export(*malformed_aggregate, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, root_path);
	auto throwing_aggregate = bind_aggregate("throwing_sum_hook");
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(*throwing_aggregate, context), InvalidInputException);
	auto empty_aggregate_failure = bind_aggregate("empty_sum_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*empty_aggregate_failure, context), root_path);
	auto invalid_aggregate_failure = bind_aggregate("invalid_sum_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*invalid_aggregate_failure, context), root_path);
	auto verify_aggregate_failure = bind_aggregate("verify_sum_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*verify_aggregate_failure, context), root_path);
	auto plan_aggregate_failure = bind_aggregate("plan_sum_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*plan_aggregate_failure, context), root_path);
	auto wrong_root_aggregate_failure = bind_aggregate("wrong_root_sum_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*wrong_root_aggregate_failure, context), root_path);
	auto mixed_aggregate_failure = bind_aggregate("mixed_sum_failure_hook");
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*mixed_aggregate_failure, context), root_path);
	auto sibling_aggregate_failure = bind_aggregate("sibling_sum_failure_hook");
	auto nested_sibling_aggregate_failure = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, std::move(sibling_aggregate_failure), Constant(Value::BIGINT(7)));
	auto aggregate_hook_path = root_path;
	aggregate_hook_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0});
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*nested_sibling_aggregate_failure, context),
	                         aggregate_hook_path);
	auto descendant_aggregate_failure = bind_aggregate("descendant_sum_failure_hook");
	auto aggregate_descendant_path = root_path;
	aggregate_descendant_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0});
	auto aggregate_descendant_result = BoundExpressionSQLExporter::Export(*descendant_aggregate_failure, context);
	RequireIssue(aggregate_descendant_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION,
	             aggregate_descendant_path);
	REQUIRE(aggregate_descendant_result.GetIssues()[0].construct);
	REQUIRE(aggregate_descendant_result.GetIssues()[0].construct->type ==
	        LogicalPlanVerificationConstructType::FUNCTION);
	auto past_modifiers_plan =
	    BindExportQuery(connection, "SELECT " + schema_name.GetIdentifierName() +
	                                    ".past_modifiers_sum_failure_hook(i ORDER BY i) FILTER (WHERE i > 0) "
	                                    "FROM (VALUES (1), (2)) AS v(i)");
	auto past_modifiers_aggregate = FindExpression(*past_modifiers_plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       candidate.Cast<BoundAggregateExpression>().Function().GetName() == "past_modifiers_sum_failure_hook";
	});
	REQUIRE(past_modifiers_aggregate);
	auto past_modifiers_source = CreateSyntheticSQLSource(*past_modifiers_aggregate);
	RequireInvalidHookResult(
	    BoundExpressionSQLExporter::Export(*past_modifiers_aggregate, past_modifiers_source.context), root_path);
	auto failing_aggregate = bind_aggregate("failing_sum_hook");
	auto aggregate_failure = BoundExpressionSQLExporter::Export(*failing_aggregate, context);
	RequireIssue(aggregate_failure, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, root_path);
	REQUIRE(aggregate_failure.GetIssues()[0].construct ==
	        optional<LogicalPlanVerificationConstructIdentity>(
	            LogicalPlanVerificationConstructIdentity::ExportFeature("synthetic_aggregate_export_failure")));
	REQUIRE(aggregate_failure.GetIssues()[0].message == "Synthetic aggregate SQL export hook rejected the expression");

	auto failing_scalar = bind_scalar("failing_scalar_hook");
	auto first_comparison = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(scalar_copy),
	                                                          Constant(Value::INTEGER(7)));
	auto failing_comparison = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
	                                                            std::move(failing_scalar), Constant(Value::INTEGER(7)));
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	conjunction->GetChildrenMutable().push_back(std::move(first_comparison));
	conjunction->GetChildrenMutable().push_back(std::move(failing_comparison));
	auto failed = BoundExpressionSQLExporter::Export(*conjunction, context);
	REQUIRE(failed.HasError());
	REQUIRE(failed.GetIssues().size() == 1);
	REQUIRE(failed.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(failed.GetIssues()[0].path ==
	        LogicalPlanVerificationPath {LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION,
	                                     {{LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 1},
	                                      {LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0}}});
	REQUIRE(failed.GetIssues()[0].construct ==
	        optional<LogicalPlanVerificationConstructIdentity>(
	            LogicalPlanVerificationConstructIdentity::ExportFeature("synthetic_scalar_export_failure")));
	REQUIRE(failed.GetIssues()[0].message == "Synthetic scalar SQL export hook rejected the expression");
	connection.Rollback();
}

#ifndef DUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED
TEST_CASE("Standalone function binding does not autoload catalog collisions",
          "[bound_expression_sql_export][logical_plan_verification][dont_link]") {
	auto extension_directory = TestJoinPath(TestDirectoryPath(), "stage02_standalone_bind_extensions");
	TestDeleteDirectory(extension_directory);
	TestCreateDirectory(extension_directory);
	DBConfig config;
	config.SetOptionByName("extension_directory", extension_directory);
	config.SetOptionByName("autoload_known_extensions", true);
	config.SetOptionByName("autoinstall_known_extensions", false);
	DuckDB db(nullptr, &config);
	Connection connection(db);

	auto require_core_functions_absent = [&]() {
		auto extension_state = connection.Query(
		    "SELECT installed, loaded FROM duckdb_extensions() WHERE extension_name = 'core_functions'");
		REQUIRE_FALSE(extension_state->HasError());
		REQUIRE(extension_state->RowCount() == 1);
		REQUIRE(extension_state->GetValue(0, 0) == Value::BOOLEAN(false));
		REQUIRE(extension_state->GetValue(1, 0) == Value::BOOLEAN(false));
		auto count_if = connection.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'count_if'");
		REQUIRE_FALSE(count_if->HasError());
		REQUIRE(count_if->GetValue(0, 0) == Value::BIGINT(0));
	};
	require_core_functions_absent();
	connection.BeginTransaction();

	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	ScalarFunction standalone_scalar(Identifier("count_if"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                                 ScalarFunction::NopFunction);
	standalone_scalar.SetCatalogName(Identifier::SystemCatalog());
	standalone_scalar.SetSchemaName(Identifier::DefaultSchema());
	vector<unique_ptr<Expression>> scalar_children;
	scalar_children.push_back(Constant(Value::INTEGER(7)));
	auto scalar = standalone_scalar.Bind(*connection.context, std::move(scalar_children));
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *scalar) == Value::INTEGER(7));
	REQUIRE_FALSE(scalar->GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*scalar, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto standalone_aggregate = SyntheticSum(Identifier("count_if"), 0);
	standalone_aggregate.SetCatalogName(Identifier::SystemCatalog());
	standalone_aggregate.SetSchemaName(Identifier::DefaultSchema());
	vector<unique_ptr<Expression>> aggregate_children;
	aggregate_children.push_back(Constant(Value::INTEGER(7)));
	auto aggregate = standalone_aggregate.Bind(*connection.context, std::move(aggregate_children));
	REQUIRE(aggregate->Function().GetName() == "count_if");
	REQUIRE_FALSE(aggregate->GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(*aggregate, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	connection.Commit();
	require_core_functions_absent();
}
#endif

TEST_CASE("Bound expression SQL export authenticates recipes after deserialization", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_deserializer_provenance_extension");
	loader.UseDedicatedSchemaForExtension(Identifier("synthetic_deserializer_provenance_schema"));

	ScalarFunction stable_scalar(Identifier("deserialize_identity"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                             ScalarFunction::NopFunction);
	stable_scalar.SetSerializeCallback(SerializeSyntheticScalar);
	stable_scalar.SetDeserializeCallback(DeserializeSyntheticScalar);
	auto mutated_scalar = stable_scalar;
	mutated_scalar.SetName(Identifier("deserialize_mutation"));
	mutated_scalar.SetDeserializeCallback(DeserializeMutatingScalar);
	loader.RegisterFunction(std::move(stable_scalar));
	loader.RegisterFunction(std::move(mutated_scalar));

	auto stable_sum = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<0>>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	stable_sum.SetName(Identifier("deserialize_sum"));
	stable_sum.SetSerializeCallback(SerializeSyntheticAggregate);
	stable_sum.SetDeserializeCallback(DeserializeSyntheticAggregate);
	auto mutated_sum = stable_sum;
	mutated_sum.SetName(Identifier("deserialize_mutation_sum"));
	mutated_sum.SetDeserializeCallback(DeserializeMutatingAggregate);
	loader.RegisterFunction(std::move(stable_sum));
	loader.RegisterFunction(std::move(mutated_sum));

	auto plus_one_sum = AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<1>>(
	    LogicalType::INTEGER, LogicalType::BIGINT);
	plus_one_sum.SetName(Identifier("deserialize_sum_plus_one"));
	loader.RegisterFunction(plus_one_sum);
	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();

	auto plan = BindExportQuery(
	    connection, "SELECT synthetic_deserializer_provenance_schema.deserialize_identity(CAST(7 AS INTEGER)), "
	                "synthetic_deserializer_provenance_schema.deserialize_mutation(CAST(7 AS INTEGER)), "
	                "synthetic_deserializer_provenance_schema.deserialize_sum(CAST(7 AS INTEGER)), "
	                "synthetic_deserializer_provenance_schema.deserialize_mutation_sum(CAST(7 AS INTEGER))");
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto stable_scalar_expression = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "deserialize_identity";
	});
	REQUIRE(stable_scalar_expression);
	auto serialized_stable_scalar = BinaryRoundTrip(*connection.context, *stable_scalar_expression);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *serialized_stable_scalar) == Value::INTEGER(7));
	auto &serialized_stable_function = serialized_stable_scalar->Cast<BoundFunctionExpression>();
	REQUIRE(serialized_stable_function.GetSQLExportRecipe());
	RequireRoundTrip(connection, *serialized_stable_scalar, context, string(),
	                 "synthetic_deserializer_provenance_schema.deserialize_identity(CAST(7 AS INTEGER))");

	auto mutated_scalar_expression = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "deserialize_mutation";
	});
	REQUIRE(mutated_scalar_expression);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *mutated_scalar_expression) == Value::INTEGER(7));
	auto serialized_mutated_scalar = BinaryRoundTrip(*connection.context, *mutated_scalar_expression);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *serialized_mutated_scalar) == Value::INTEGER(8));
	REQUIRE(serialized_mutated_scalar->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(*serialized_mutated_scalar, context).IsSuccess());

	auto stable_aggregate_expression = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       expression.Cast<BoundAggregateExpression>().Function().GetName() == "deserialize_sum";
	});
	REQUIRE(stable_aggregate_expression);
	auto serialized_stable_aggregate = BinaryRoundTrip(*connection.context, *stable_aggregate_expression);
	REQUIRE(serialized_stable_aggregate->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	RequireRoundTrip(connection, *serialized_stable_aggregate, context, string(),
	                 "synthetic_deserializer_provenance_schema.deserialize_sum(CAST(7 AS INTEGER))");

	auto mutated_aggregate_expression = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       expression.Cast<BoundAggregateExpression>().Function().GetName() == "deserialize_mutation_sum";
	});
	REQUIRE(mutated_aggregate_expression);
	auto serialized_mutated_aggregate = BinaryRoundTrip(*connection.context, *mutated_aggregate_expression);
	auto &serialized_mutated_sum = serialized_mutated_aggregate->Cast<BoundAggregateExpression>();
	REQUIRE(serialized_mutated_sum.Function().GetCallbacks() == plus_one_sum.GetCallbacks());
	auto callback_oracle = connection.Query(
	    "SELECT synthetic_deserializer_provenance_schema.deserialize_sum_plus_one(CAST(7 AS INTEGER))");
	REQUIRE_FALSE(callback_oracle->HasError());
	REQUIRE(callback_oracle->GetValue(0, 0) == Value::BIGINT(8));
	REQUIRE(serialized_mutated_sum.GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(serialized_mutated_sum, context).IsSuccess());
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export consumes recipes around broad statistics mutation",
          "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_statistics_provenance_extension");
	loader.UseDedicatedSchemaForExtension(Identifier("synthetic_statistics_provenance_schema"));

	ScalarFunction stable_scalar(Identifier("statistics_stable"), {}, LogicalType::INTEGER, ReturnSeven);
	stable_scalar.SetStatisticsCallback(PreserveScalarDuringStatistics);
	stable_scalar.SetVolatile();
	auto scalar = stable_scalar;
	scalar.SetName(Identifier("statistics_mutation"));
	scalar.SetStatisticsCallback(MutateScalarDuringStatistics);
	loader.RegisterFunction(std::move(stable_scalar));
	loader.RegisterFunction(std::move(scalar));

	auto stable_aggregate = SyntheticSum(Identifier("statistics_sum_stable"), 0);
	stable_aggregate.SetStatisticsCallback(PreserveAggregateDuringStatistics);
	auto aggregate = stable_aggregate;
	aggregate.SetName(Identifier("statistics_sum"));
	aggregate.SetStatisticsCallback(MutateAggregateDuringStatistics);
	loader.RegisterFunction(std::move(stable_aggregate));
	loader.RegisterFunction(std::move(aggregate));
	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();

	const string scalar_query = "SELECT synthetic_statistics_provenance_schema.statistics_mutation()";
	auto scalar_plan = BindExportQuery(connection, scalar_query);
	auto scalar_expression = FindExpression(*scalar_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
	});
	REQUIRE(scalar_expression);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *scalar_expression) == Value::INTEGER(7));

	auto optimized_scalar_plan = OptimizeExportQuery(connection, scalar_query);
	auto optimized_scalar = FindExpression(*optimized_scalar_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
	});
	REQUIRE(optimized_scalar);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *optimized_scalar) == Value::INTEGER(8));
	REQUIRE_FALSE(optimized_scalar->Cast<BoundFunctionExpression>().GetSQLExportRecipe());

	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(BoundExpressionSQLExporter::Export(*optimized_scalar, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto stable_scalar_plan =
	    OptimizeExportQuery(connection, "SELECT synthetic_statistics_provenance_schema.statistics_stable()");
	auto stable_scalar_expression = FindExpression(*stable_scalar_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
	});
	REQUIRE(stable_scalar_expression);
	REQUIRE(stable_scalar_expression->Cast<BoundFunctionExpression>().GetSQLExportRecipe());
	RequireRoundTrip(connection, *stable_scalar_expression, context, string(),
	                 "synthetic_statistics_provenance_schema.statistics_stable()");

	const string aggregate_query = "SELECT synthetic_statistics_provenance_schema.statistics_sum(CAST(7 AS INTEGER))";
	auto optimized_aggregate_plan = OptimizeExportQuery(connection, aggregate_query);
	auto optimized_aggregate = FindExpression(*optimized_aggregate_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE;
	});
	REQUIRE(optimized_aggregate);
	auto &bound_aggregate = optimized_aggregate->Cast<BoundAggregateExpression>();
	REQUIRE(bound_aggregate.Function().GetName() == "statistics_sum_plus_one");
	REQUIRE_FALSE(bound_aggregate.GetSQLExportRecipe());
	RequireIssue(BoundExpressionSQLExporter::Export(bound_aggregate, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto optimized_result = connection.Query(aggregate_query);
	REQUIRE_FALSE(optimized_result->HasError());
	REQUIRE(optimized_result->GetValue(0, 0) == Value::BIGINT(8));
	REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='statistics_propagation'"));
	auto baseline_result = connection.Query(aggregate_query);
	REQUIRE_FALSE(baseline_result->HasError());
	REQUIRE(baseline_result->GetValue(0, 0) == Value::BIGINT(7));
	REQUIRE_NO_FAIL(connection.Query("RESET disabled_optimizers"));
	auto stable_aggregate_plan = OptimizeExportQuery(
	    connection, "SELECT synthetic_statistics_provenance_schema.statistics_sum_stable(CAST(7 AS INTEGER))");
	auto stable_aggregate_expression = FindExpression(*stable_aggregate_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE;
	});
	REQUIRE(stable_aggregate_expression);
	REQUIRE(stable_aggregate_expression->Cast<BoundAggregateExpression>().GetSQLExportRecipe());
	RequireRoundTrip(connection, *stable_aggregate_expression, context, string(),
	                 "synthetic_statistics_provenance_schema.statistics_sum_stable(CAST(7 AS INTEGER))");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export keeps authenticated recipes separate from deserialized execution",
          "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_deserializer_name_extension");
	loader.UseDedicatedSchemaForExtension(Identifier("synthetic_deserializer_name_schema"));

	ScalarFunction scalar(Identifier("name_sensitive"), {}, LogicalType::INTEGER, NameSensitiveScalar);
	scalar.SetSerializeCallback(SerializeSyntheticScalar);
	scalar.SetDeserializeCallback(DeserializeAndRenameScalar);
	loader.RegisterFunction(std::move(scalar));

	auto aggregate = NameSensitiveSum(Identifier("name_sensitive_sum"));
	aggregate.SetSerializeCallback(SerializeSyntheticAggregate);
	aggregate.SetDeserializeCallback(DeserializeAndRenameAggregate);
	loader.RegisterFunction(std::move(aggregate));
	auto mutated_aggregate = NameSensitiveSum(Identifier("mutated_sum"));
	mutated_aggregate.SetSerializeCallback(SerializeSyntheticAggregate);
	mutated_aggregate.SetDeserializeCallback(DeserializeAndRenameAggregate);
	loader.RegisterFunction(mutated_aggregate);
	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();

	auto plan =
	    BindExportQuery(connection, "SELECT synthetic_deserializer_name_schema.name_sensitive(), "
	                                "synthetic_deserializer_name_schema.name_sensitive_sum(CAST(7 AS INTEGER))");
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto scalar_expression = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
	});
	REQUIRE(scalar_expression);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *scalar_expression) == Value::INTEGER(7));
	auto serialized_scalar = BinaryRoundTrip(*connection.context, *scalar_expression);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *serialized_scalar) == Value::INTEGER(8));
	auto &bound_scalar = serialized_scalar->Cast<BoundFunctionExpression>();
	REQUIRE(bound_scalar.Function().GetName() == "mutated_name");
	REQUIRE(bound_scalar.Function().GetDefinition()->GetName() == "name_sensitive");
	REQUIRE(bound_scalar.GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(bound_scalar, context).IsSuccess());

	auto aggregate_expression = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE;
	});
	REQUIRE(aggregate_expression);
	auto serialized_aggregate = BinaryRoundTrip(*connection.context, *aggregate_expression);
	auto &bound_aggregate = serialized_aggregate->Cast<BoundAggregateExpression>();
	REQUIRE(bound_aggregate.Function().GetName() == "mutated_sum");
	REQUIRE(bound_aggregate.Function().GetDefinition()->GetName() == "name_sensitive_sum");
	REQUIRE(bound_aggregate.Function().GetCallbacks() == mutated_aggregate.GetCallbacks());
	REQUIRE(bound_aggregate.GetSQLExportRecipe());
	REQUIRE(BoundExpressionSQLExporter::Export(bound_aggregate, context).IsSuccess());

	auto original_result =
	    connection.Query("SELECT synthetic_deserializer_name_schema.name_sensitive_sum(CAST(7 AS INTEGER))");
	REQUIRE_FALSE(original_result->HasError());
	REQUIRE(original_result->GetValue(0, 0) == Value::BIGINT(7));
	auto mutated_result = connection.Query("SELECT synthetic_deserializer_name_schema.mutated_sum(CAST(7 AS INTEGER))");
	REQUIRE_FALSE(mutated_result->HasError());
	REQUIRE(mutated_result->GetValue(0, 0) == Value::BIGINT(8));
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export admits only context-rebindable casts", "[bound_expression_sql_export]") {
	DuckDB unregistered_db;
	Connection unregistered_connection(unregistered_db);
	auto unregistered = BoundCastExpression::Create(Constant(Value::INTEGER(7)), LogicalType::BIGINT,
	                                                BoundCastInfo(IntegerToBigintPlusOne));
	REQUIRE(ExpressionExecutor::EvaluateScalar(*unregistered_connection.context, *unregistered) == Value::BIGINT(8));
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(BoundExpressionSQLExporter::Export(*unregistered, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	DuckDB registered_db;
	Connection registered_connection(registered_db);
	ExtensionLoader loader(*registered_db.instance, "synthetic_cast_extension");
	loader.RegisterCastFunction(LogicalType::INTEGER, LogicalType::BIGINT, BoundCastInfo(IntegerToBigintPlusOne), 0);
	auto registered = BoundCastExpression::AddCastToType(*registered_connection.context, Constant(Value::INTEGER(7)),
	                                                     LogicalType::BIGINT);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*registered_connection.context, *registered) == Value::BIGINT(8));
	auto exported = BoundExpressionSQLExporter::Export(*registered, context);
	REQUIRE(exported.IsSuccess());
	auto rebound = registered_connection.Query("SELECT " + exported.GetValue()->ToString());
	REQUIRE_FALSE(rebound->HasError());
	REQUIRE(rebound->GetTypes() == vector<LogicalType> {LogicalType::BIGINT});
	REQUIRE(rebound->GetValue(0, 0) == Value::BIGINT(8));
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
	REQUIRE(invalid_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);

	BoundOperatorExpression invalid_arity(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
	auto arity_result = BoundExpressionSQLExporter::Export(invalid_arity, context);
	REQUIRE(arity_result.HasError());
	REQUIRE(arity_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT);
}

TEST_CASE("Bound expression SQL export rejects TRY around volatile children", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

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
	RequireIssue(volatile_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
	REQUIRE(*volatile_result.GetIssues()[0].construct->identifier == "try_volatile_child");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export reconstructs optimizer-produced scalar functions",
          "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE scalar_values(i INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO scalar_values VALUES (-7, 'AbC'), (7, 'Def')"));
	connection.BeginTransaction();

	auto plan = OptimizeExportQuery(connection,
	                                "SELECT abs(i), i + 2, struct_pack(value := i), NOT (i = 7) FROM scalar_values");
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
	REQUIRE(addition_function.GetSQLExportRecipe());
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

	auto negated_comparison = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.GetExpressionType() == ExpressionType::COMPARE_NOTEQUAL;
	});
	REQUIRE(negated_comparison);
	auto &comparison_column =
	    negated_comparison->Cast<BoundFunctionExpression>().GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto comparison_context =
	    ResolveBinding(comparison_column.Binding(), {Identifier("v"), Identifier("i")}, LogicalType::INTEGER);
	RequireRoundTrip(connection, *negated_comparison, comparison_context, " FROM scalar_values AS v", "v.i != 7");

	auto struct_pack = FindExpression(*plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "struct_pack";
	});
	REQUIRE(struct_pack);
	auto struct_result = BoundExpressionSQLExporter::Export(*struct_pack, abs_context);
	REQUIRE(struct_result.IsValid());
	REQUIRE(struct_result.HasError());
	INFO(struct_result.GetIssues()[0].message);
	REQUIRE(struct_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION);

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
	REQUIRE(opaque_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION);

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
	REQUIRE(volatile_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
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
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto callback_spoof = OperatorEqualFun::GetFunction();
	callback_spoof.SetFunctionCallback(ScalarFunction::BinaryFunction<int32_t, int32_t, bool, NotEqualOperation>);
	vector<unique_ptr<Expression>> callback_children;
	callback_children.push_back(Constant(Value::INTEGER(7)));
	callback_children.push_back(Constant(Value::INTEGER(7)));
	auto callback_expression = callback_spoof.Bind(*connection.context, std::move(callback_children));
	REQUIRE(callback_expression->GetExpressionType() == ExpressionType::COMPARE_EQUAL);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *callback_expression) == Value::BOOLEAN(false));
	RequireIssue(BoundExpressionSQLExporter::Export(*callback_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto property_spoof = IsDistinctFromFun::GetFunction();
	property_spoof.SetNullHandling(FunctionNullHandling::DEFAULT_NULL_HANDLING);
	vector<unique_ptr<Expression>> property_children;
	property_children.push_back(Constant(Value(LogicalType::INTEGER)));
	property_children.push_back(Constant(Value::INTEGER(1)));
	auto property_expression = property_spoof.Bind(*connection.context, std::move(property_children));
	REQUIRE(property_expression->GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *property_expression).IsNull());
	RequireIssue(BoundExpressionSQLExporter::Export(*property_expression, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

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
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	}

	auto malformed_cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	malformed_cast->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<OpaqueSQLFunctionData>();
	RequireIssue(BoundExpressionSQLExporter::Export(*malformed_cast, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto spoofed_cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	SpoofCastFunctionData spoof_cast_source;
	spoof_cast_source.value = 42;
	SpoofCastFunctionData spoof_cast_copy(spoof_cast_source);
	REQUIRE(spoof_cast_copy.value == 42);
	SpoofCastFunctionData spoof_cast_assignment;
	spoof_cast_assignment = spoof_cast_source;
	REQUIRE(spoof_cast_assignment.value == 42);
	spoofed_cast->Cast<BoundFunctionExpression>().BindInfoMutable() =
	    make_uniq<SpoofCastFunctionData>(spoof_cast_assignment);
	RequireIssue(BoundExpressionSQLExporter::Export(*spoofed_cast, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto mismatched_cast_data =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	auto varchar_cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::VARCHAR);
	mismatched_cast_data->Cast<BoundFunctionExpression>().BindInfoMutable() =
	    varchar_cast->Cast<BoundFunctionExpression>().BindInfo()->Copy();
	RequireIssue(BoundExpressionSQLExporter::Export(*mismatched_cast_data, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);

	auto malformed_between = BoundBetweenExpression::Create(Constant(Value::INTEGER(7)), Constant(Value::INTEGER(2)),
	                                                        Constant(Value::INTEGER(9)), true, true);
	malformed_between->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<OpaqueSQLFunctionData>();
	RequireIssue(BoundExpressionSQLExporter::Export(*malformed_between, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	auto spoofed_between = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(2)),
	                                                      Constant(Value::INTEGER(9)), true, true);
	SpoofBetweenFunctionData spoof_between_source;
	spoof_between_source.value = 84;
	SpoofBetweenFunctionData spoof_between_copy(spoof_between_source);
	REQUIRE(spoof_between_copy.value == 84);
	SpoofBetweenFunctionData spoof_between_assignment;
	spoof_between_assignment = spoof_between_source;
	REQUIRE(spoof_between_assignment.value == 84);
	spoofed_between->Cast<BoundFunctionExpression>().BindInfoMutable() =
	    make_uniq<SpoofBetweenFunctionData>(spoof_between_copy);
	RequireIssue(BoundExpressionSQLExporter::Export(*spoofed_between, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export preserves aggregate modifiers", "[bound_expression_sql_export][optimizer]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE aggregate_values(i INTEGER)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO aggregate_values VALUES (1), (2), (2), (NULL)"));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE count_values(i INTEGER NOT NULL)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO count_values VALUES (1), (2)"));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE large_sum_values(i BIGINT)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO large_sum_values SELECT 4000000000000000000 FROM range(5)"));
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
	auto avg_sum = check_aggregate("SELECT avg(i) FROM aggregate_values", "sum(v.i)", Identifier("sum"), true);
	REQUIRE(avg_sum.GetValue()->Cast<FunctionExpression>().FunctionName() == "sum");
	auto optimized_sum_plan = OptimizeExportQuery(connection, "SELECT sum(i) FROM aggregate_values");
	auto optimized_sum = FindExpression(*optimized_sum_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       expression.Cast<BoundAggregateExpression>().Function().GetName() == "sum";
	});
	REQUIRE(optimized_sum);
	auto &optimized_sum_bound = optimized_sum->Cast<BoundAggregateExpression>();
	REQUIRE(optimized_sum_bound.Function().GetDefinition());
	REQUIRE(optimized_sum_bound.Function().GetDefinition()->GetName() == "sum");
	REQUIRE(optimized_sum_bound.GetSQLExportRecipe());
	auto &catalog = Catalog::GetSystemCatalog(*connection.context);
	auto &sum_no_overflow_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    *connection.context,
	    QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("sum_no_overflow")));
	auto sum_no_overflow =
	    sum_no_overflow_entry.functions.GetFunctionByArguments(*connection.context, {LogicalType::INTEGER});
	auto optimized_execution = optimized_sum_bound.Function().GetExecutionCallbacks();
	auto no_overflow_execution = sum_no_overflow->GetExecutionCallbacks();
	REQUIRE(optimized_execution.state_size == no_overflow_execution.state_size);
	REQUIRE(optimized_execution.initialize == no_overflow_execution.initialize);
	REQUIRE(optimized_execution.update == no_overflow_execution.update);
	REQUIRE(optimized_execution.combine == no_overflow_execution.combine);
	REQUIRE(optimized_execution.finalize == no_overflow_execution.finalize);
	REQUIRE(optimized_sum_bound.Function().GetCallbacks() !=
	        optimized_sum_bound.Function().GetDefinition()->GetCallbacks());
	auto &optimized_sum_child = optimized_sum_bound.GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto optimized_sum_context =
	    ResolveBinding(optimized_sum_child.Binding(), {Identifier("v"), Identifier("i")}, LogicalType::INTEGER);
	auto pre_serialization = BoundExpressionSQLExporter::Export(optimized_sum_bound, optimized_sum_context);
	REQUIRE(pre_serialization.IsSuccess());
	REQUIRE(pre_serialization.GetValue()->Cast<FunctionExpression>().FunctionName() == "sum");

	auto serialized_sum = BinaryRoundTrip(*connection.context, optimized_sum_bound);
	auto &serialized_sum_bound = serialized_sum->Cast<BoundAggregateExpression>();
	REQUIRE(serialized_sum_bound.Function().GetName() == "sum");
	REQUIRE(serialized_sum_bound.Function().GetDefinition());
	REQUIRE(serialized_sum_bound.Function().GetDefinition()->GetName() == "sum");
	REQUIRE(serialized_sum_bound.GetSQLExportRecipe());
	REQUIRE(serialized_sum_bound.Function().GetCallbacks() != optimized_sum_bound.Function().GetCallbacks());
	RequireRoundTrip(connection, serialized_sum_bound, optimized_sum_context, " FROM aggregate_values AS v",
	                 "sum(v.i)");

	auto large_sum_plan = OptimizeExportQuery(connection, "SELECT sum(i) FROM large_sum_values");
	auto large_sum = FindExpression(*large_sum_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       expression.Cast<BoundAggregateExpression>().Function().GetName() == "sum";
	});
	REQUIRE(large_sum);
	auto &large_sum_bound = large_sum->Cast<BoundAggregateExpression>();
	REQUIRE(large_sum_bound.GetSQLExportRecipe());
	REQUIRE(large_sum_bound.Function().GetDefinition());
	REQUIRE(large_sum_bound.Function().GetCallbacks() == large_sum_bound.Function().GetDefinition()->GetCallbacks());

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
	REQUIRE(count_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION);
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export preserves aggregate rewrite provenance",
          "[bound_expression_sql_export][aggregate_rewrite_provenance]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	LogicalPlanVerificationPath root_path;
	root_path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto histogram_plan = OptimizeExportQuery(connection, "SELECT histogram(i) FROM (VALUES (1), (2), (2)) t(i)");
	vector<reference<const BoundAggregateExpression>> histogram_aggregates;
	CollectAggregateExpressions(*histogram_plan, histogram_aggregates);
	idx_t list_count = 0;
	for (auto &aggregate_ref : histogram_aggregates) {
		auto &aggregate = aggregate_ref.get();
		auto &definition = aggregate.Function().GetDefinition();
		if (!definition || definition->GetName() != "list") {
			continue;
		}
		list_count++;
		REQUIRE(definition->GetCatalogName() == Identifier::SystemCatalog());
		REQUIRE(definition->GetSchemaName() == Identifier::DefaultSchema());
		RequireLiveCatalogDefinition(*connection.context, aggregate);
		REQUIRE(aggregate.GetSQLExportRecipe());
		REQUIRE(TypeVisitor::Contains(aggregate.GetReturnType(),
		                              [](const LogicalType &type) { return type.id() == LogicalTypeId::TUPLE; }));
		auto source = CreateSyntheticSQLSource(aggregate);
		RequireFunctionIssue(BoundExpressionSQLExporter::Export(aggregate, source.context),
		                     LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path,
		                     definition->GetCatalogName(), definition->GetSchemaName(), definition->GetName(),
		                     aggregate.Function().GetArguments(), aggregate.GetReturnType());
	}
	REQUIRE(list_count == 1);

	auto entropy_plan = OptimizeExportQuery(connection, "SELECT entropy(i) FROM (VALUES (1), (2), (2)) t(i)");
	vector<reference<const BoundAggregateExpression>> entropy_aggregates;
	CollectAggregateExpressions(*entropy_plan, entropy_aggregates);
	idx_t sum_count = 0;
	idx_t simple_sum_count = 0;
	idx_t weighted_sum_count = 0;
	for (auto &aggregate_ref : entropy_aggregates) {
		auto &aggregate = aggregate_ref.get();
		auto &definition = aggregate.Function().GetDefinition();
		if (!definition || definition->GetName() != "sum") {
			continue;
		}
		sum_count++;
		REQUIRE(definition->GetCatalogName() == Identifier::SystemCatalog());
		REQUIRE(definition->GetSchemaName() == Identifier::DefaultSchema());
		RequireLiveCatalogDefinition(*connection.context, aggregate);
		REQUIRE(aggregate.GetSQLExportRecipe());
		REQUIRE(aggregate.GetChildren().size() == 1);
		if (aggregate.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			simple_sum_count++;
			RequireAggregateRewriteRoundTrip(connection, aggregate);
			continue;
		}

		weighted_sum_count++;
		REQUIRE(aggregate.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION);
		auto &multiply = aggregate.GetChildren()[0]->Cast<BoundFunctionExpression>();
		REQUIRE(multiply.Function().GetName() == "*");
		REQUIRE(multiply.GetChildren().size() == 2);
		REQUIRE(multiply.GetChildren()[1]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION);
		auto &log2 = multiply.GetChildren()[1]->Cast<BoundFunctionExpression>();
		REQUIRE(log2.Function().GetName() == "log2");
		REQUIRE(log2.Function().GetDefinition());
		REQUIRE(log2.GetSQLExportRecipe());
		auto source = CreateSyntheticSQLSource(aggregate);
		auto log2_path = root_path;
		log2_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0});
		log2_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 1});
		auto &log2_definition = log2.Function().GetDefinition();
		RequireFunctionIssue(BoundExpressionSQLExporter::Export(aggregate, source.context),
		                     LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, log2_path,
		                     log2_definition->GetCatalogName(), log2_definition->GetSchemaName(),
		                     log2_definition->GetName(), log2.Function().GetArguments(), log2.GetReturnType());
	}
	REQUIRE(sum_count == 2);
	REQUIRE(simple_sum_count == 1);
	REQUIRE(weighted_sum_count == 1);

	auto mode_plan = OptimizeExportQuery(connection, "SELECT mode(i) FROM (VALUES (1), (2), (2)) t(i)");
	vector<reference<const BoundAggregateExpression>> mode_aggregates;
	CollectAggregateExpressions(*mode_plan, mode_aggregates);
	idx_t arg_max_count = 0;
	for (auto &aggregate_ref : mode_aggregates) {
		auto &aggregate = aggregate_ref.get();
		auto &definition = aggregate.Function().GetDefinition();
		if (!definition || definition->GetName() != "arg_max") {
			continue;
		}
		arg_max_count++;
		RequireLiveCatalogDefinition(*connection.context, aggregate);
		REQUIRE(definition->GetCallbacks().HasBindCallback());
		REQUIRE(aggregate.GetSQLExportRecipe());
		auto source = CreateSyntheticSQLSource(aggregate);
		RequireFunctionIssue(BoundExpressionSQLExporter::Export(aggregate, source.context),
		                     LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path,
		                     definition->GetCatalogName(), definition->GetSchemaName(), definition->GetName(),
		                     aggregate.Function().GetArguments(), aggregate.GetReturnType());
	}
	REQUIRE(arg_max_count == 1);
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export fails closed for deferred and malformed inputs",
          "[bound_expression_sql_export]") {
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
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
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION, path);
	}

	vector<unique_ptr<Expression>> expanded_children;
	expanded_children.push_back(Constant(Value::INTEGER(1)));
	BoundExpandedExpression expanded(std::move(expanded_children));
	RequireIssue(BoundExpressionSQLExporter::Export(expanded, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	SyntheticExpression parsed(ExpressionClass::CONSTANT, ExpressionType::VALUE_CONSTANT, LogicalType::INTEGER);
	RequireIssue(BoundExpressionSQLExporter::Export(parsed, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	SyntheticExpression invalid(ExpressionClass::INVALID, ExpressionType::INVALID, LogicalType::INTEGER);
	RequireIssue(BoundExpressionSQLExporter::Export(invalid, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	SyntheticExpression unknown(static_cast<ExpressionClass>(255), ExpressionType::INVALID, LogicalType::INTEGER);
	RequireIssue(BoundExpressionSQLExporter::Export(unknown, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);

	auto malformed = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	malformed->GetChildrenMutable().push_back(nullptr);
	malformed->GetChildrenMutable().push_back(make_uniq<BoundDefaultExpression>(LogicalType::BOOLEAN));
	auto malformed_result = BoundExpressionSQLExporter::Export(*malformed, context);
	REQUIRE(malformed_result.IsValid());
	REQUIRE(malformed_result.HasError());
	REQUIRE(malformed_result.GetIssues().size() == 2);
	REQUIRE(malformed_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT);
	REQUIRE(malformed_result.GetIssues()[1].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION);
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
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(BoundExpressionSQLExporter::Export(deferred, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
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
