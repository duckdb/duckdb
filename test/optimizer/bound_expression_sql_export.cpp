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

	int32_t value = 0;
};

static unique_ptr<FunctionData> BindOpaqueSQLFunction(BindScalarFunctionInput &) {
	return make_uniq<OpaqueSQLFunctionData>();
}

static unique_ptr<FunctionData> BindOpaqueSQLAggregate(BindAggregateFunctionInput &) {
	return make_uniq<OpaqueSQLFunctionData>();
}

static unique_ptr<FunctionData> BindDropLastScalarArgument(BindScalarFunctionInput &input) {
	input.GetBoundFunction().GetArguments().pop_back();
	input.GetArguments().pop_back();
	return make_uniq<OpaqueSQLFunctionData>();
}

static unique_ptr<FunctionData> BindDropLastAggregateArgument(BindAggregateFunctionInput &input) {
	input.GetBoundFunction().GetArguments().pop_back();
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
	auto sum =
	    make_uniq<FunctionExpression>(name, std::move(input.children), std::move(input.filter),
	                                  std::move(input.order_bys), input.aggregate_type == AggregateType::DISTINCT,
	                                  false, input.state_export_mode == AggregateStateExportMode::STATE_EXPORT);
	return FunctionSQLExportResult::Success(make_uniq<CastExpression>(LogicalType::BIGINT, std::move(sum)));
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

static FunctionSQLExportResult ExportBadAllocAggregate(AggregateFunctionSQLExportInput &) {
	throw std::bad_alloc();
}

static FunctionSQLExportResult EmptySQLExportFailure() {
	return FunctionSQLExportResult::Failure(string());
}

static FunctionSQLExportResult ExportEmptyScalarFailure(ScalarFunctionSQLExportInput &) {
	return EmptySQLExportFailure();
}

static FunctionSQLExportResult ExportEmptyAggregateFailure(AggregateFunctionSQLExportInput &) {
	return EmptySQLExportFailure();
}

static FunctionSQLExportResult ExportScalarFailure(ScalarFunctionSQLExportInput &) {
	return FunctionSQLExportResult::Failure("Synthetic scalar SQL export hook rejected the expression");
}

static FunctionSQLExportResult ExportAggregateFailure(AggregateFunctionSQLExportInput &) {
	return FunctionSQLExportResult::Failure("Synthetic aggregate SQL export hook rejected the expression");
}

struct SubtractOperation {
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return left - right;
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

static AggregateFunction SyntheticSum(const Identifier &name, int64_t offset) {
	auto result = offset == 0 ? AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<0>>(
	                                LogicalType::INTEGER, LogicalType::BIGINT)
	                          : AggregateFunction::UnaryAggregate<int64_t, int32_t, int64_t, SyntheticSumOperation<1>>(
	                                LogicalType::INTEGER, LogicalType::BIGINT);
	result.SetName(name);
	return result;
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
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;

	auto cast = BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(42)),
	                                               LogicalType::BIGINT, true);
	auto cast_result = BoundExpressionSQLExporter::Export(*cast, context);
	REQUIRE(cast_result.IsSuccess());
	REQUIRE(cast_result.GetValue()->Cast<CastExpression>().IsTryCast());
	REQUIRE(cast_result.GetValue()->Cast<CastExpression>().TargetType() == LogicalType::BIGINT);
	auto serialized_cast = BinaryRoundTrip(*connection.context, *cast);
	RequireRoundTrip(connection, *serialized_cast, context, string(), "TRY_CAST(CAST(42 AS INTEGER) AS BIGINT)");

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
		auto copied = comparison->Copy();
		REQUIRE(BoundExpressionSQLExporter::Export(*copied, context).IsSuccess());
	}

	auto inclusive = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(1)),
	                                                Constant(Value::INTEGER(3)), true, true);
	auto inclusive_result = BoundExpressionSQLExporter::Export(*inclusive, context);
	REQUIRE(inclusive_result.IsSuccess());
	REQUIRE(inclusive_result.GetValue()->GetExpressionClass() == ExpressionClass::BETWEEN);
	auto serialized_between = BinaryRoundTrip(*connection.context, *inclusive);
	RequireRoundTrip(connection, *serialized_between, context, string(),
	                 "CAST(2 AS INTEGER) BETWEEN CAST(1 AS INTEGER) AND CAST(3 AS INTEGER)");

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
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export uses local function syntax hooks",
          "[bound_expression_sql_export][function_sql_export_hook]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_sql_export_hook_extension");
	const Identifier schema_name("synthetic_sql_export_hook_schema");
	loader.UseDedicatedSchemaForExtension(schema_name);
	ScalarFunction scalar_without_hook(Identifier("scalar_equality"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                                   ScalarFunction::NopFunction);
	auto scalar_with_hook = scalar_without_hook;
	scalar_with_hook.SetSQLExportCallback(ExportScalarFirstChild);
	REQUIRE(scalar_without_hook == scalar_with_hook);
	auto aggregate_without_hook = SyntheticSum(Identifier("aggregate_equality"), 0);
	auto aggregate_with_hook = aggregate_without_hook;
	aggregate_with_hook.SetSQLExportCallback(ExportAggregateAsSum);
	REQUIRE(aggregate_without_hook == aggregate_with_hook);

	auto register_scalar = [&](const Identifier &name, scalar_function_sql_export_t callback,
	                           bind_scalar_function_t bind = BindOpaqueSQLFunction) {
		ScalarFunction function(name, {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
		                        ScalarFunction::NopFunction, bind);
		if (callback) {
			function.SetSQLExportCallback(callback);
		}
		loader.RegisterFunction(std::move(function));
	};
	register_scalar(Identifier("ordinary_bound_scalar"), nullptr);
	register_scalar(Identifier("hooked_scalar"), ExportScalarFirstChild);
	register_scalar(Identifier("malformed_scalar_hook"), ExportMalformedScalar);
	register_scalar(Identifier("throwing_scalar_hook"), ExportThrowingScalar);
	register_scalar(Identifier("bad_alloc_scalar_hook"), ExportBadAllocScalar);
	register_scalar(Identifier("empty_scalar_failure_hook"), ExportEmptyScalarFailure);
	register_scalar(Identifier("failing_scalar_hook"), ExportScalarFailure);
	register_scalar(Identifier("lost_scalar_argument"), nullptr, BindDropLastScalarArgument);

	auto register_aggregate = [&](const Identifier &name, aggregate_function_sql_export_t callback,
	                              bind_aggregate_function_t bind = BindOpaqueSQLAggregate) {
		auto function = SyntheticSum(name, 0);
		function.SetBindCallback(bind);
		if (callback) {
			function.SetSQLExportCallback(callback);
		}
		loader.RegisterFunction(std::move(function));
	};
	register_aggregate(Identifier("ordinary_bound_sum"), nullptr);
	register_aggregate(Identifier("hooked_sum"), ExportAggregateAsSum);
	register_aggregate(Identifier("malformed_sum_hook"), ExportMalformedAggregate);
	register_aggregate(Identifier("throwing_sum_hook"), ExportThrowingAggregate);
	register_aggregate(Identifier("bad_alloc_sum_hook"), ExportBadAllocAggregate);
	register_aggregate(Identifier("empty_sum_failure_hook"), ExportEmptyAggregateFailure);
	register_aggregate(Identifier("failing_sum_hook"), ExportAggregateFailure);
	auto lost_sum = SyntheticSum(Identifier("lost_sum_argument"), 0);
	lost_sum.GetSignature().AddParameter(LogicalType::INTEGER);
	lost_sum.SetBindCallback(BindDropLastAggregateArgument);
	loader.RegisterFunction(std::move(lost_sum));
	loader.RefreshSearchPath(*connection.context);
	connection.BeginTransaction();

	auto bind_scalar = [&](const string &name) {
		auto plan = BindExportQuery(connection, "SELECT " + schema_name.GetIdentifierName() + "." + name +
		                                            "(CAST(7 AS INTEGER), CAST(99 AS INTEGER))");
		auto expression = FindExpression(*plan, [&](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
			       candidate.Cast<BoundFunctionExpression>().Function().GetName() == name;
		});
		REQUIRE(expression);
		return expression->Copy();
	};
	auto bind_aggregate = [&](const string &name, const string &arguments = "CAST(7 AS INTEGER)") {
		auto plan = BindExportQuery(connection,
		                            "SELECT " + schema_name.GetIdentifierName() + "." + name + "(" + arguments + ")");
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

	auto ordinary_scalar = bind_scalar("ordinary_bound_scalar");
	REQUIRE(ordinary_scalar->Cast<BoundFunctionExpression>().BindInfo());
	auto ordinary_scalar_result = BoundExpressionSQLExporter::Export(*ordinary_scalar, context);
	REQUIRE(ordinary_scalar_result.IsSuccess());
	REQUIRE(ordinary_scalar_result.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
	        QualifiedName(Identifier::SystemCatalog(), schema_name, Identifier("ordinary_bound_scalar")));

	auto hooked_scalar = bind_scalar("hooked_scalar");
	REQUIRE(hooked_scalar->Cast<BoundFunctionExpression>().BindInfo());
	REQUIRE(BoundExpressionSQLExporter::Export(*hooked_scalar->Copy(), context).IsSuccess());
	auto serialized_hooked_scalar = BinaryRoundTrip(*connection.context, *hooked_scalar);
	REQUIRE(BoundExpressionSQLExporter::Export(*serialized_hooked_scalar, context).IsSuccess());
	auto scalar_result = BoundExpressionSQLExporter::Export(*hooked_scalar, context);
	REQUIRE(scalar_result.IsSuccess());
	REQUIRE(scalar_result.GetValue()->GetExpressionClass() == ExpressionClass::CAST);
	hooked_scalar.reset();
	auto scalar_rebound = connection.Query("SELECT " + scalar_result.GetValue()->ToString());
	REQUIRE_FALSE(scalar_rebound->HasError());
	REQUIRE(scalar_rebound->GetValue(0, 0) == Value::INTEGER(7));

	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*bind_scalar("malformed_scalar_hook"), context),
	                         root_path);
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*bind_scalar("empty_scalar_failure_hook"), context),
	                         root_path);
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(*bind_scalar("throwing_scalar_hook"), context),
	                  InvalidInputException);
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(*bind_scalar("bad_alloc_scalar_hook"), context),
	                  std::bad_alloc);
	auto scalar_failure = bind_scalar("failing_scalar_hook");
	auto nested_failure = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(scalar_failure),
	                                                        Constant(Value::INTEGER(7)));
	auto scalar_hook_path = root_path;
	scalar_hook_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0});
	auto failed = BoundExpressionSQLExporter::Export(*nested_failure, context);
	RequireIssue(failed, LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, scalar_hook_path);
	REQUIRE(failed.GetIssues()[0].construct);
	REQUIRE(failed.GetIssues()[0].construct->type == LogicalPlanVerificationConstructType::FUNCTION);

	auto lost_scalar = bind_scalar("lost_scalar_argument");
	REQUIRE(lost_scalar->Cast<BoundFunctionExpression>().GetChildren().size() == 1);
	RequireIssue(BoundExpressionSQLExporter::Export(*lost_scalar, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path);

	auto modified_plan =
	    BindExportQuery(connection, "SELECT " + schema_name.GetIdentifierName() +
	                                    ".hooked_sum(DISTINCT i ORDER BY i DESC NULLS FIRST) FILTER (WHERE i > 0) "
	                                    "FROM (VALUES (1), (2), (2)) AS v(i)");
	auto modified_aggregate = FindExpression(*modified_plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       candidate.Cast<BoundAggregateExpression>().Function().GetName() == "hooked_sum";
	});
	REQUIRE(modified_aggregate);
	auto modified_source = CreateSyntheticSQLSource(*modified_aggregate);
	auto modified_result = BoundExpressionSQLExporter::Export(*modified_aggregate, modified_source.context);
	REQUIRE(modified_result.IsSuccess());
	REQUIRE(modified_result.GetValue()->GetExpressionClass() == ExpressionClass::CAST);
	auto &modified_function = modified_result.GetValue()->Cast<CastExpression>().Child().Cast<FunctionExpression>();
	REQUIRE(modified_function.Distinct());
	REQUIRE(modified_function.Filter());
	REQUIRE(modified_function.OrderBy());
	REQUIRE(modified_function.OrderBy()->orders.size() == 1);
	auto modified_rebound =
	    connection.Query("SELECT " + modified_result.GetValue()->ToString() + modified_source.from_clause);
	REQUIRE_FALSE(modified_rebound->HasError());
	REQUIRE(modified_rebound->GetTypes() == vector<LogicalType> {LogicalType::BIGINT});
	REQUIRE(modified_rebound->GetValue(0, 0) == Value::BIGINT(3));

	auto ordinary_aggregate = bind_aggregate("ordinary_bound_sum");
	REQUIRE(ordinary_aggregate->Cast<BoundAggregateExpression>().BindInfo());
	auto ordinary_aggregate_result = BoundExpressionSQLExporter::Export(*ordinary_aggregate, context);
	REQUIRE(ordinary_aggregate_result.IsSuccess());
	REQUIRE(ordinary_aggregate_result.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
	        QualifiedName(Identifier::SystemCatalog(), schema_name, Identifier("ordinary_bound_sum")));

	auto hooked_aggregate = bind_aggregate("hooked_sum");
	REQUIRE(BoundExpressionSQLExporter::Export(*hooked_aggregate->Copy(), context).IsSuccess());
	auto serialized_hooked_aggregate = BinaryRoundTrip(*connection.context, *hooked_aggregate);
	REQUIRE(BoundExpressionSQLExporter::Export(*serialized_hooked_aggregate, context).IsSuccess());
	auto aggregate_result = BoundExpressionSQLExporter::Export(*hooked_aggregate, context);
	REQUIRE(aggregate_result.IsSuccess());
	REQUIRE(aggregate_result.GetValue()->GetExpressionClass() == ExpressionClass::CAST);
	hooked_aggregate.reset();
	auto aggregate_rebound = connection.Query("SELECT " + aggregate_result.GetValue()->ToString());
	REQUIRE_FALSE(aggregate_rebound->HasError());
	REQUIRE(aggregate_rebound->GetTypes() == vector<LogicalType> {LogicalType::BIGINT});
	REQUIRE(aggregate_rebound->GetValue(0, 0) == Value::BIGINT(7));

	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*bind_aggregate("malformed_sum_hook"), context),
	                         root_path);
	RequireInvalidHookResult(BoundExpressionSQLExporter::Export(*bind_aggregate("empty_sum_failure_hook"), context),
	                         root_path);
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(*bind_aggregate("throwing_sum_hook"), context),
	                  InvalidInputException);
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(*bind_aggregate("bad_alloc_sum_hook"), context),
	                  std::bad_alloc);
	RequireIssue(BoundExpressionSQLExporter::Export(*bind_aggregate("failing_sum_hook"), context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path);

	auto lost_aggregate = bind_aggregate("lost_sum_argument", "CAST(7 AS INTEGER), CAST(99 AS INTEGER)");
	REQUIRE(lost_aggregate->Cast<BoundAggregateExpression>().GetChildren().size() == 1);
	RequireIssue(BoundExpressionSQLExporter::Export(*lost_aggregate, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path);
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

	ScalarFunction standalone_scalar(Identifier("count_if"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                                 ScalarFunction::NopFunction);
	standalone_scalar.SetCatalogName(Identifier::SystemCatalog());
	standalone_scalar.SetSchemaName(Identifier::DefaultSchema());
	vector<unique_ptr<Expression>> scalar_children;
	scalar_children.push_back(Constant(Value::INTEGER(7)));
	auto scalar = standalone_scalar.Bind(*connection.context, std::move(scalar_children));
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *scalar) == Value::INTEGER(7));
	auto scalar_export = BoundExpressionSQLExporter::Export(*scalar, context);
	REQUIRE(scalar_export.IsSuccess());
	REQUIRE(scalar_export.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
	        QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(), Identifier("count_if")));

	auto standalone_aggregate = SyntheticSum(Identifier("count_if"), 0);
	standalone_aggregate.SetCatalogName(Identifier::SystemCatalog());
	standalone_aggregate.SetSchemaName(Identifier::DefaultSchema());
	vector<unique_ptr<Expression>> aggregate_children;
	aggregate_children.push_back(Constant(Value::INTEGER(7)));
	auto aggregate = standalone_aggregate.Bind(*connection.context, std::move(aggregate_children));
	REQUIRE(aggregate->Function().GetName() == "count_if");
	auto aggregate_export = BoundExpressionSQLExporter::Export(*aggregate, context);
	REQUIRE(aggregate_export.IsSuccess());
	REQUIRE(aggregate_export.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
	        QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(), Identifier("count_if")));

	connection.Commit();
	require_core_functions_absent();
}
#endif

TEST_CASE("Bound expression SQL export reconstructs registered casts", "[bound_expression_sql_export]") {
	BoundExpressionSQLExportContext context;
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
	REQUIRE(abs_function.Function().GetDefinition());
	REQUIRE(abs_function.Function().GetDefinition()->GetName() == "abs");
	auto &abs_column = abs_function.GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto abs_context = ResolveBinding(abs_column.Binding(), {Identifier("v"), Identifier("i")}, LogicalType::INTEGER);
	RequireRoundTrip(connection, *abs_expression, abs_context, " FROM scalar_values AS v", "abs(v.i)");

	auto log2_plan = OptimizeExportQuery(connection, "SELECT log2(CAST(i AS DOUBLE)) FROM scalar_values");
	auto log2_expression = FindExpression(*log2_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "log2";
	});
	REQUIRE(log2_expression);
	auto &log2_function = log2_expression->Cast<BoundFunctionExpression>();
	REQUIRE(log2_function.Function().GetDefinition());
	REQUIRE(log2_function.Function().GetDefinition()->HasBindCallback());
	auto log2_source = CreateSyntheticSQLSource(log2_function);
	RequireRoundTrip(connection, log2_function, log2_source.context, log2_source.from_clause,
	                 "log2(CAST(v.exported_0 AS DOUBLE))");

	auto power_plan = BindExportQuery(connection, "SELECT power(CAST(i AS DOUBLE), 2.0) FROM scalar_values");
	auto power_expression = FindExpression(*power_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "power";
	});
	REQUIRE(power_expression);
	auto &power_function = power_expression->Cast<BoundFunctionExpression>();
	REQUIRE(power_function.Function().GetDefinition());
	REQUIRE(power_function.Function().GetDefinition()->GetName() == "power");
	auto power_source = CreateSyntheticSQLSource(power_function);
	auto power_result = BoundExpressionSQLExporter::Export(power_function, power_source.context);
	REQUIRE(power_result.IsSuccess());
	REQUIRE(power_result.GetValue()->Cast<FunctionExpression>().FunctionName() == "power");
	RequireRoundTrip(connection, power_function, power_source.context, power_source.from_clause,
	                 "power(CAST(v.exported_0 AS DOUBLE), 2.0)");

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
	REQUIRE(opaque_result.IsSuccess());
	REQUIRE(
	    opaque_result.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
	    QualifiedName(Identifier("synthetic_catalog"), Identifier("synthetic_schema"), Identifier("opaque_scalar")));

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

TEST_CASE("Bound expression SQL export validates structural expression state", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto comparison = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(7)),
	                                                    Constant(Value::INTEGER(7)));
	comparison->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<OpaqueSQLFunctionData>();
	RequireIssue(BoundExpressionSQLExporter::Export(*comparison, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);

	auto malformed_cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	malformed_cast->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<OpaqueSQLFunctionData>();
	RequireIssue(BoundExpressionSQLExporter::Export(*malformed_cast, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	auto spoofed_cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	SpoofCastFunctionData spoof_cast_source;
	spoof_cast_source.value = 42;
	SpoofCastFunctionData spoof_cast_copy(spoof_cast_source);
	REQUIRE(spoof_cast_copy.value == 42);
	spoofed_cast->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<SpoofCastFunctionData>(spoof_cast_copy);
	RequireIssue(BoundExpressionSQLExporter::Export(*spoofed_cast, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	auto mismatched_cast_data =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	auto varchar_cast =
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::VARCHAR);
	mismatched_cast_data->Cast<BoundFunctionExpression>().BindInfoMutable() =
	    varchar_cast->Cast<BoundFunctionExpression>().BindInfo()->Copy();
	RequireIssue(BoundExpressionSQLExporter::Export(*mismatched_cast_data, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);

	auto malformed_between = BoundBetweenExpression::Create(Constant(Value::INTEGER(7)), Constant(Value::INTEGER(2)),
	                                                        Constant(Value::INTEGER(9)), true, true);
	malformed_between->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<OpaqueSQLFunctionData>();
	RequireIssue(BoundExpressionSQLExporter::Export(*malformed_between, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	auto spoofed_between = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(2)),
	                                                      Constant(Value::INTEGER(9)), true, true);
	SpoofBetweenFunctionData spoof_between_source;
	spoof_between_source.value = 84;
	SpoofBetweenFunctionData spoof_between_copy(spoof_between_source);
	REQUIRE(spoof_between_copy.value == 84);
	spoofed_between->Cast<BoundFunctionExpression>().BindInfoMutable() =
	    make_uniq<SpoofBetweenFunctionData>(spoof_between_copy);
	RequireIssue(BoundExpressionSQLExporter::Export(*spoofed_between, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);

	auto wrong_arity = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(2)),
	                                                  Constant(Value::INTEGER(9)), true, true);
	wrong_arity->Cast<BoundFunctionExpression>().GetChildrenMutable().pop_back();
	RequireIssue(BoundExpressionSQLExporter::Export(*wrong_arity, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
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
	auto mean = check_aggregate("SELECT mean(i) FROM aggregate_values", "mean(v.i)", Identifier("mean"), false);
	REQUIRE(mean.GetValue()->Cast<FunctionExpression>().FunctionName() == "mean");
	auto optimized_sum_plan = OptimizeExportQuery(connection, "SELECT sum(i) FROM aggregate_values");
	auto optimized_sum = FindExpression(*optimized_sum_plan, [](const Expression &expression) {
		if (expression.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &definition = expression.Cast<BoundAggregateExpression>().Function().GetDefinition();
		return definition && definition->GetName() == "sum";
	});
	REQUIRE(optimized_sum);
	auto &optimized_sum_bound = optimized_sum->Cast<BoundAggregateExpression>();
	REQUIRE(optimized_sum_bound.Function().GetDefinition());
	REQUIRE(optimized_sum_bound.Function().GetDefinition()->GetName() == "sum");
	REQUIRE(optimized_sum_bound.Function().GetName() == "sum_no_overflow");
	auto &catalog = Catalog::GetSystemCatalog(*connection.context);
	auto &sum_no_overflow_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    *connection.context,
	    QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("sum_no_overflow")));
	auto sum_no_overflow =
	    sum_no_overflow_entry.functions.GetFunctionByArguments(*connection.context, {LogicalType::INTEGER});
	REQUIRE(optimized_sum_bound.Function().GetCallbacks() == sum_no_overflow->GetCallbacks());
	REQUIRE(optimized_sum_bound.Function().GetCallbacks() !=
	        optimized_sum_bound.Function().GetDefinition()->GetCallbacks());
	auto &optimized_sum_child = optimized_sum_bound.GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto optimized_sum_context =
	    ResolveBinding(optimized_sum_child.Binding(), {Identifier("v"), Identifier("i")}, LogicalType::INTEGER);
	auto pre_serialization = BoundExpressionSQLExporter::Export(optimized_sum_bound, optimized_sum_context);
	REQUIRE(pre_serialization.IsSuccess());
	REQUIRE(pre_serialization.GetValue()->Cast<FunctionExpression>().FunctionName() == "sum");

	auto large_sum_plan = OptimizeExportQuery(connection, "SELECT sum(i) FROM large_sum_values");
	auto large_sum = FindExpression(*large_sum_plan, [](const Expression &expression) {
		if (expression.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &definition = expression.Cast<BoundAggregateExpression>().Function().GetDefinition();
		return definition && definition->GetName() == "sum";
	});
	REQUIRE(large_sum);
	auto &large_sum_bound = large_sum->Cast<BoundAggregateExpression>();
	REQUIRE(large_sum_bound.Function().GetName() == "sum");
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

	auto quantile_plan = BindExportQuery(connection, "SELECT quantile_cont(i, 0.5) FROM aggregate_values");
	auto quantile = FindExpression(*quantile_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
		       expression.Cast<BoundAggregateExpression>().Function().GetName() == "quantile_cont";
	});
	REQUIRE(quantile);
	auto &quantile_bound = quantile->Cast<BoundAggregateExpression>();
	REQUIRE(quantile_bound.BindInfo());
	REQUIRE(quantile_bound.GetChildren().size() == 2);
	auto quantile_source = CreateSyntheticSQLSource(quantile_bound);
	RequireRoundTrip(connection, quantile_bound, quantile_source.context, quantile_source.from_clause,
	                 "quantile_cont(v.exported_0, 0.5)");

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

TEST_CASE("Bound expression SQL export uses retained definitions after aggregate rewrites",
          "[bound_expression_sql_export][aggregate_rewrite]") {
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
		REQUIRE(log2.Function().HasBindCallback());
		RequireAggregateRewriteRoundTrip(connection, aggregate);
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
		RequireAggregateRewriteRoundTrip(connection, aggregate);
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
