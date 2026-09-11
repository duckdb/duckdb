#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

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
#include "duckdb/function/scalar/generic_common.hpp"
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
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include <cstring>

using namespace duckdb;

namespace {

using ExportResult = LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>;

static void RequireCastTarget(const ParsedExpression &expression, const LogicalType &type) {
	auto &cast = expression.Cast<CastExpression>();
	CastExpression expected(type, cast.Child().Copy(), cast.IsTryCast());
	REQUIRE(cast.Equals(expected));
}

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
	auto export_error = exported_result->HasError() ? exported_result->GetError() : string();
	INFO("exported SQL=" << exported_sql);
	INFO("export error=" << export_error);
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

static unique_ptr<Expression>
BinaryRoundTrip(ClientContext &context, const Expression &expression,
                const StorageCompatibility &compatibility = StorageCompatibility::Latest()) {
	MemoryStream stream(Allocator::Get(context));
	SerializationOptions options;
	options.storage_compatibility = compatibility;
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
		} else if (entry.type.id() == LogicalTypeId::VARCHAR) {
			first_row += "CAST('a' AS VARCHAR)";
			second_row += "CAST('b' AS VARCHAR)";
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
	uint8_t GetInternalKind() const {
		return 1;
	}

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
	uint8_t GetInternalKind() const {
		return 2;
	}

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

static Value EvaluateAggregate(const BoundAggregateExpression &expression,
                               const Value &input_value = Value::INTEGER(7)) {
	auto &function = expression.Function();
	AggregateStateInput state_input(function, expression.BindInfo().get());
	auto state = make_unsafe_uniq_array_uninitialized<data_t>(function.GetStateSizeCallback()(state_input));
	data_ptr_t state_ptr = state.get();
	function.GetStateInitCallback()(state_input, &state_ptr, 1);
	Vector input(input_value, count_t(1));
	Vector state_vector(Value::POINTER(CastPointerToValue(state.get())), count_t(1));
	ArenaAllocator allocator(Allocator::DefaultAllocator());
	AggregateInputData update_input(function, expression.BindInfo().get(), allocator);
	function.GetStateUpdateCallback()(&input, update_input, 1, state_vector, 1);
	Vector result(expression.GetReturnType());
	AggregateFinalizeInputData finalize_input(function, expression.BindInfo().get(), allocator);
	function.GetStateFinalizeCallback()(state_vector, finalize_input, result, 1, 0);
	return result.GetValue(0);
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
			RequireCastTarget(*result.GetValue(), value.type());
		}
		auto rebound = connection.Query("SELECT " + result.GetValue()->ToString());
		INFO("type=" << value.type().ToString());
		INFO("sql=" << result.GetValue()->ToString());
		if (rebound->HasError()) {
			INFO("error=" << rebound->GetError());
		}
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

TEST_CASE("Bound expression SQL export reconstructs VARIANT literals", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	for (auto &sql : {"'hello'::VARIANT", "NULL::VARIANT", "7::SMALLINT::VARIANT", "12.34::DECIMAL(9,2)::VARIANT",
	                  "'a''b'::BLOB::VARIANT", "{'slash\\key': {'🦆': 12.34::DECIMAL(9,2)}}::VARIANT",
	                  "{'quoted''key': [1::SMALLINT::VARIANT, 'x'::VARIANT, NULL::VARIANT]}::VARIANT",
	                  "[7::SMALLINT::VARIANT, 'x'::VARIANT]", "{'v': 7::SMALLINT::VARIANT}",
	                  "[7::SMALLINT::VARIANT]::VARIANT[1]"}) {
		INFO(sql);
		auto original = connection.Query("SELECT " + string(sql));
		REQUIRE_NO_FAIL(*original);
		BoundConstantExpression expression(original->GetValue(0, 0));
		RequireRoundTrip(connection, expression, {}, string(), sql);
		auto copied = expression.Copy();
		RequireRoundTrip(connection, *copied, {}, string(), sql);
		auto restored = BinaryRoundTrip(*connection.context, expression);
		RequireRoundTrip(connection, *restored, {}, string(), sql);
	}
}

TEST_CASE("Bound expression SQL export preserves negative floating zero", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	for (auto type : {"FLOAT", "DOUBLE"}) {
		auto sql = "'-0.0'::" + string(type) + "::VARIANT";
		auto original = connection.Query("SELECT " + sql);
		REQUIRE_NO_FAIL(*original);
		BoundConstantExpression expression(original->GetValue(0, 0));
		for (idx_t lifecycle = 0; lifecycle < 3; lifecycle++) {
			auto candidate = lifecycle == 2 ? BinaryRoundTrip(*connection.context, expression) : expression.Copy();
			auto exported = BoundExpressionSQLExporter::Export(lifecycle == 0 ? expression : *candidate, {});
			REQUIRE(exported.IsSuccess());
			auto result = connection.Query("SELECT 1.0 / (" + exported.GetValue()->ToString() + ")::DOUBLE");
			REQUIRE_NO_FAIL(*result);
			REQUIRE(result->GetValue(0, 0) == Value("-Infinity").DefaultCastAs(LogicalType::DOUBLE));
		}
	}
}

TEST_CASE("Bound expression SQL export rejects opaque aggregate-state literals", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	auto result = connection.Query("SELECT sum(i) EXPORT_STATE FROM (VALUES (1),(2)) t(i)");
	REQUIRE_NO_FAIL(*result);
	auto state = result->GetValue(0, 0);
	REQUIRE(state.type().IsAggregateState());
	vector<Value> values {state, Value(state.type()), Value::LIST(state.type(), {state}),
	                      Value::STRUCT({{"state", state}}), Value(LogicalType::LIST(state.type()))};
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 2});
	for (auto &value : values) {
		BoundConstantExpression expression(value);
		RequireIssue(BoundExpressionSQLExporter::ExportAtPath(expression, {}, path),
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
	}
}

TEST_CASE("Bound expression SQL export rejects unrepresentable VARIANT object keys", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	if (!db.instance->ExtensionIsLoaded("json")) {
		WARN("JSON extension required for empty VARIANT key coverage");
		return;
	}
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	for (auto json : {"{\"\":1}", "{\"\":3,\"a\":1}", "{\"a\":1,\"\":2}", "{\"outer\":{\"\":1,\"x\":2}}", "[{\"\":1}]",
	                  "{\"a\":1,\"A\":2}", "{\"outer\":{\"a\":1,\"A\":2}}", "[{\"a\":1,\"A\":2}]"}) {
		auto original = connection.Query("SELECT j::JSON::VARIANT FROM (VALUES ('" + string(json) + "')) t(j)");
		REQUIRE_NO_FAIL(*original);
		auto value = original->GetValue(0, 0);
		for (auto &nested : {value, Value::LIST(LogicalType::VARIANT(), {value}), Value::STRUCT({{"v", value}}),
		                     Value::ARRAY(LogicalType::VARIANT(), {value}),
		                     Value::MAP(LogicalType::VARCHAR, LogicalType::VARIANT(), {Value("v")}, {value}),
		                     Value::UNION({{"v", LogicalType::VARIANT()}}, 0, value)}) {
			BoundConstantExpression expression(nested);
			for (idx_t lifecycle = 0; lifecycle < 3; lifecycle++) {
				auto candidate = lifecycle == 2 ? BinaryRoundTrip(*connection.context, expression) : expression.Copy();
				RequireIssue(BoundExpressionSQLExporter::Export(lifecycle == 0 ? expression : *candidate, {}),
				             LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
			}
		}
	}
}

TEST_CASE("Bound expression SQL export rejects alias-sensitive binders", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	vector<pair<string, string>> cases {{"struct_insert", "struct_insert({'a': i}, b := i)"},
	                                    {"struct_update", "struct_update({'a': i}, a := i + 1)"},
	                                    {"union_value", "union_value(a := i)"},
	                                    {"write_log", "write_log('test', disable_logging := true, return_value := i)"}};
	if (db.instance->ExtensionIsLoaded("json")) {
		cases.emplace_back("json_serialize_sql", "json_serialize_sql('SELECT 1', format := true)");
		cases.emplace_back("json_serialize_plan", "json_serialize_plan('SELECT 1', optimize := false)");
	}
	for (auto &entry : cases) {
		INFO(entry.first);
		auto sql = "SELECT " + entry.second + " FROM (VALUES (1),(2)) t(i)";
		auto original = connection.Query(sql);
		REQUIRE_NO_FAIL(*original);
		auto plan = BindExportQuery(connection, sql);
		auto expression = FindExpression(*plan, [&](const Expression &candidate) {
			return candidate.GetExpressionType() == ExpressionType::BOUND_FUNCTION &&
			       candidate.Cast<BoundFunctionExpression>().Function().GetDefinition()->GetName() == entry.first;
		});
		REQUIRE(expression);
		REQUIRE(expression->Cast<BoundFunctionExpression>()
		            .Function()
		            .GetDefinition()
		            ->GetProperties()
		            .RequiresExpressionNames());
		RequireIssue(BoundExpressionSQLExporter::Export(*expression, {}),
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
		auto copied = expression->Copy();
		RequireIssue(BoundExpressionSQLExporter::Export(*copied, {}),
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
		if (entry.first == "struct_insert" || entry.first == "struct_update" || entry.first == "union_value") {
			auto restored = BinaryRoundTrip(*connection.context, *expression);
			RequireIssue(BoundExpressionSQLExporter::Export(*restored, {}),
			             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
		}
	}
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export rejects functions that observe expression names",
          "[bound_expression_sql_export][serialization]") {
	FunctionProperties properties;
	REQUIRE_FALSE(properties.RequiresExpressionNames());
	auto named_properties = properties;
	named_properties.SetRequiresExpressionNames(true);
	REQUIRE(properties != named_properties);
	properties = named_properties;
	REQUIRE(properties.RequiresExpressionNames());
	REQUIRE(properties == named_properties);
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	struct Case {
		string sql;
		string expected;
		bool nested;
	};
	vector<Case> cases {{"SELECT alias(i) AS renamed FROM (VALUES (1),(2),(NULL)) t(i)", "renamed", false},
	                    {"SELECT alias(i + 1) FROM (VALUES (1),(2),(NULL)) t(i)", "(i + 1)", false},
	                    {"SELECT upper(alias(i)) FROM (VALUES (1),(2),(NULL)) t(i)", "I", true}};
	for (auto &entry : cases) {
		INFO(entry.sql);
		auto original = connection.Query(entry.sql);
		REQUIRE_NO_FAIL(*original);
		for (idx_t row = 0; row < 3; row++) {
			REQUIRE(original->GetValue(0, row) == Value(entry.expected));
		}
		auto plan = BindExportQuery(connection, entry.sql);
		auto alias = FindExpression(*plan, [](const Expression &expression) {
			return expression.GetExpressionType() == ExpressionType::BOUND_FUNCTION &&
			       expression.Cast<BoundFunctionExpression>().Function().GetDefinition()->GetName() == "alias";
		});
		REQUIRE(alias);
		REQUIRE(alias->Cast<BoundFunctionExpression>()
		            .Function()
		            .GetDefinition()
		            ->GetProperties()
		            .RequiresExpressionNames());
		RequireIssue(BoundExpressionSQLExporter::Export(*alias, {}),
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
		auto copied = alias->Copy();
		RequireIssue(BoundExpressionSQLExporter::Export(*copied, {}),
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
		auto restored = BinaryRoundTrip(*connection.context, *alias);
		RequireIssue(BoundExpressionSQLExporter::Export(*restored, {}),
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
		if (entry.nested) {
			auto parent = FindExpression(*plan, [](const Expression &expression) {
				return expression.GetExpressionType() == ExpressionType::BOUND_FUNCTION &&
				       expression.Cast<BoundFunctionExpression>().Function().GetDefinition()->GetName() == "upper";
			});
			REQUIRE(parent);
			auto child_path = path;
			child_path.components.push_back({LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, 0});
			RequireIssue(BoundExpressionSQLExporter::Export(*parent, {}),
			             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, child_path);
		}
	}
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export preserves ordering before physical aggregate lowering",
          "[bound_expression_sql_export][serialization]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	const string from = " FROM (VALUES (1),(2)) t(i)";
	auto plan = BindExportQuery(connection, "SELECT first(i ORDER BY i DESC)" + from);
	auto expression = FindExpression(*plan, [](const Expression &candidate) {
		return candidate.GetExpressionType() == ExpressionType::BOUND_AGGREGATE;
	});
	REQUIRE(expression);
	for (idx_t lifecycle = 0; lifecycle < 3; lifecycle++) {
		auto candidate = lifecycle == 2 ? BinaryRoundTrip(*connection.context, *expression) : expression->Copy();
		auto &aggregate = candidate->Cast<BoundAggregateExpression>();
		REQUIRE(aggregate.GetOrderBys());
		REQUIRE(aggregate.GetOrderBys()->orders.size() == 1);
		vector<SQLBindingEntry> bindings;
		CollectSQLBindings(*candidate, bindings);
		REQUIRE(bindings.size() == 1);
		auto context = ResolveBinding(bindings[0].binding, {Identifier("i")}, LogicalType::INTEGER);
		RequireRoundTrip(connection, lifecycle == 0 ? *expression : *candidate, context, from,
		                 "first(i ORDER BY i DESC)");
	}
	auto expected = connection.Query("SELECT first(i ORDER BY i DESC)" + from);
	REQUIRE_NO_FAIL(*expected);
	REQUIRE(expected->GetValue(0, 0) == Value::INTEGER(2));
	// Physical lowering consumes the ordering; such execution expressions are outside the export API contract.
	auto lowered = expression->Copy();
	vector<unique_ptr<Expression>> groups;
	FunctionBinder::BindSortedAggregate(*connection.context, lowered->Cast<BoundAggregateExpression>(), groups,
	                                    nullptr);
	REQUIRE_FALSE(lowered->Cast<BoundAggregateExpression>().GetOrderBys());
	connection.Rollback();
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
	RequireCastTarget(*cast_result.GetValue(), LogicalType::BIGINT);
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

TEST_CASE("Incremental scalar registration preserves live SQL identity", "[bound_expression_sql_export][extension]") {
	DuckDB source_db;
	DuckDB target_db;
	Connection source(source_db);
	Connection target(target_db);
	const Identifier name("incremental_sql_identity");
	auto make_function = [&](const LogicalType &type) {
		auto function = ScalarFunction(name, {type}, type, ScalarFunction::NopFunction);
		function.SetCatalogName(Identifier("unowned_catalog"));
		function.SetSchemaName(Identifier("unowned_schema"));
		return function;
	};
	auto register_functions = [&](DuckDB &db) {
		ExtensionLoader loader(*db.instance, "incremental_sql_identity_extension");
		loader.RegisterFunction(make_function(LogicalType::INTEGER));
		auto initial = loader.GetFunction(name).functions.GetFunctionByOffset(0);
		loader.AddFunctionOverload(make_function(LogicalType::BIGINT));
		ScalarFunctionSet additions(name);
		additions.AddFunction(make_function(LogicalType::SMALLINT));
		auto renamed = make_function(LogicalType::VARCHAR);
		renamed.SetName(Identifier("noncanonical_name"));
		additions.AddFunction(std::move(renamed));
		loader.AddFunctionOverload(std::move(additions));
		auto &functions = loader.GetFunction(name).functions;
		REQUIRE(functions.Size() == 4);
		REQUIRE(functions.GetFunctionByOffset(0) == initial);
		idx_t index = 0;
		for (auto &type : {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::SMALLINT, LogicalType::VARCHAR}) {
			auto &definition = functions.GetFunctionByOffset(index++);
			REQUIRE(definition->GetSignature().GetParameter(0).GetType() == type);
			REQUIRE(definition->GetReturnType() == type);
			REQUIRE(definition->GetName() == name);
			REQUIRE(definition->GetCatalogName() == Identifier::SystemCatalog());
			REQUIRE(definition->GetSchemaName() == Identifier::DefaultSchema());
		}
	};
	register_functions(source_db);
	register_functions(target_db);
	source.BeginTransaction();
	BoundExpressionSQLExportContext context;
	for (auto &value : {Value::INTEGER(7), Value::BIGINT(8), Value::SMALLINT(9), Value("ten")}) {
		auto sql =
		    "SELECT " + name.GetIdentifierName() + "(" + value.ToSQLString() + "::" + value.type().ToString() + ")";
		auto plan = BindExportQuery(source, sql);
		auto expression = FindExpression(*plan, [&](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
			       candidate.Cast<BoundFunctionExpression>().Function().GetName() == name;
		});
		REQUIRE(expression);
		auto &function = expression->Cast<BoundFunctionExpression>().Function();
		REQUIRE(function.GetLogicalArguments() == vector<LogicalType> {value.type()});
		REQUIRE(function.GetLogicalReturnType() == value.type());
		REQUIRE(ExpressionExecutor::EvaluateScalar(*source.context, *expression) == value);
		auto exported = BoundExpressionSQLExporter::Export(*expression, context);
		REQUIRE(exported.IsValid());
		REQUIRE(exported.IsSuccess());
		REQUIRE(exported.GetValue());
		REQUIRE(exported.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
		        QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(), name));
		plan.reset();
		auto rebound = target.Query("SELECT " + exported.GetValue()->ToString());
		REQUIRE_NO_FAIL(*rebound);
		REQUIRE(rebound->GetTypes() == vector<LogicalType> {value.type()});
		REQUIRE(rebound->RowCount() == 1);
		REQUIRE(rebound->GetValue(0, 0) == value);
	}
	auto standalone = make_function(LogicalType::INTEGER);
	standalone.SetCatalogName(Identifier::SystemCatalog());
	standalone.SetSchemaName(Identifier::DefaultSchema());
	vector<unique_ptr<Expression>> children;
	children.push_back(Constant(Value::INTEGER(-7)));
	auto bound = standalone.Bind(*source.context, std::move(children));
	REQUIRE(ExpressionExecutor::EvaluateScalar(*source.context, *bound) == Value::INTEGER(-7));
	RequireRoundTrip(target, *bound, context, string(), "incremental_sql_identity(-7::INTEGER)");
	source.Rollback();
}

TEST_CASE("SQL export excludes internal types recursively", "[bound_expression_sql_export]") {
	auto binding = ColumnBinding(TableIndex(0), ProjectionIndex(0));
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	for (auto &type : vector<LogicalType> {LogicalType::POINTER, LogicalType::LIST(LogicalType::POINTER),
	                                       LogicalType::TUPLE(vector<LogicalType> {LogicalType::INTEGER}),
	                                       LogicalType::LIST(LogicalType::ANY)}) {
		BoundColumnRefExpression expression(type, binding);
		auto context = ResolveBinding(binding, {Identifier("value")}, type);
		RequireIssue(BoundExpressionSQLExporter::Export(expression, context),
		             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	}
}

TEST_CASE("SQL export type admission follows DuckDB value types", "[bound_expression_sql_export]") {
	auto types = LogicalType::AllTypes();
	for (idx_t value = 0; value <= NumericLimits<uint8_t>::Maximum(); value++) {
		auto id = static_cast<LogicalTypeId>(value);
		bool admitted = id == LogicalTypeId::SQLNULL;
		for (auto &type : types) {
			admitted |= type.id() == id && id != LogicalTypeId::TUPLE;
		}
		REQUIRE(SQLExportHelpers::IsSQLExportType(id) == admitted);
	}
	types.push_back(LogicalType::SQLNULL);
	types.push_back(LogicalType::POINTER);
	types.push_back(LogicalType::ANY);
	types.push_back(LogicalType::INVALID);
	auto binding = ColumnBinding(TableIndex(0), ProjectionIndex(0));
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	for (auto &type : types) {
		const bool admitted = SQLExportHelpers::IsSQLExportType(type.id()) && type.IsComplete();
		for (auto &candidate : vector<LogicalType> {type, LogicalType::LIST(type)}) {
			BoundColumnRefExpression expression(candidate, binding);
			auto context = ResolveBinding(binding, {Identifier("value")}, candidate);
			auto result = BoundExpressionSQLExporter::Export(expression, context);
			if (admitted) {
				REQUIRE(result.IsValid());
				REQUIRE(result.IsSuccess());
				REQUIRE(result.GetValue());
			} else {
				RequireIssue(result, LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
			}
		}
	}
#ifndef DUCKDB_CRASH_ON_ASSERT
	// Invalid LogicalType construction aborts instead of throwing in crash-on-assert configurations.
	REQUIRE_THROWS(LogicalType(static_cast<LogicalTypeId>(255)));
#endif
}

TEST_CASE("SQL export composes with binary serializer plan verification",
          "[bound_expression_sql_export][serialization]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=false"));
	connection.BeginTransaction();
	auto plan = BindExportQuery(connection, "SELECT abs(-7::INTEGER)");
	auto matches = [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "abs";
	};
	BoundExpressionSQLExportContext context;
	auto live = FindExpression(*plan, matches);
	REQUIRE(live);
	REQUIRE(BoundExpressionSQLExporter::Export(*live, context).IsSuccess());
	REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=true"));
	Planner::VerifyPlan(*connection.context, plan);
	auto restored = FindExpression(*plan, matches);
	REQUIRE(restored);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *restored) == Value::INTEGER(7));
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	RequireRoundTrip(connection, *restored, context, string(), "abs(-7::INTEGER)");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export supports represented catalog bind state", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "synthetic_sql_export_bind_extension");
	const Identifier schema_name("synthetic_sql_export_bind_schema");
	loader.UseDedicatedSchemaForExtension(schema_name);
	loader.RegisterFunction(ScalarFunction(Identifier("ordinary_bound_scalar"),
	                                       {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
	                                       ScalarFunction::NopFunction, BindOpaqueSQLFunction));
	loader.RegisterFunction(ScalarFunction(Identifier("lost_scalar_argument"),
	                                       {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
	                                       ScalarFunction::NopFunction, BindDropLastScalarArgument));
	auto ordinary_sum = SyntheticSum(Identifier("ordinary_bound_sum"), 0);
	ordinary_sum.SetBindCallback(BindOpaqueSQLAggregate);
	loader.RegisterFunction(std::move(ordinary_sum));
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
	auto scalar_result = BoundExpressionSQLExporter::Export(*ordinary_scalar, context);
	REQUIRE(scalar_result.IsSuccess());
	REQUIRE(BoundExpressionSQLExporter::Export(*ordinary_scalar->Copy(), context).IsSuccess());
	REQUIRE(scalar_result.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
	        QualifiedName(Identifier::SystemCatalog(), schema_name, Identifier("ordinary_bound_scalar")));
	auto serialized_scalar = BinaryRoundTrip(*connection.context, *ordinary_scalar);
	RequireRoundTrip(connection, *serialized_scalar, context, string(),
	                 schema_name.GetIdentifierName() + ".ordinary_bound_scalar(7, 99)");
	RequireRoundTrip(connection, *ordinary_scalar, context, string(),
	                 schema_name.GetIdentifierName() + ".ordinary_bound_scalar(7, 99)");

	auto ordinary_aggregate = bind_aggregate("ordinary_bound_sum");
	REQUIRE(ordinary_aggregate->Cast<BoundAggregateExpression>().BindInfo());
	auto aggregate_result = BoundExpressionSQLExporter::Export(*ordinary_aggregate, context);
	REQUIRE(aggregate_result.IsSuccess());
	REQUIRE(BoundExpressionSQLExporter::Export(*ordinary_aggregate->Copy(), context).IsSuccess());
	REQUIRE(aggregate_result.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
	        QualifiedName(Identifier::SystemCatalog(), schema_name, Identifier("ordinary_bound_sum")));
	auto serialized_aggregate = BinaryRoundTrip(*connection.context, *ordinary_aggregate);
	RequireRoundTrip(connection, *serialized_aggregate, context, string(),
	                 schema_name.GetIdentifierName() + ".ordinary_bound_sum(7)");
	RequireRoundTrip(connection, *ordinary_aggregate, context, string(),
	                 schema_name.GetIdentifierName() + ".ordinary_bound_sum(7)");

	auto lost_scalar = bind_scalar("lost_scalar_argument");
	REQUIRE(lost_scalar->Cast<BoundFunctionExpression>().GetChildren().size() == 1);
	RequireIssue(BoundExpressionSQLExporter::Export(*lost_scalar, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path);
	auto lost_aggregate = bind_aggregate("lost_sum_argument", "CAST(7 AS INTEGER), CAST(99 AS INTEGER)");
	REQUIRE(lost_aggregate->Cast<BoundAggregateExpression>().GetChildren().size() == 1);
	RequireIssue(BoundExpressionSQLExporter::Export(*lost_aggregate, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path);
	// Diagnostics retain the logical signature even when a standalone binder removes children.
	auto standalone = SyntheticSum(Identifier("standalone_lost_argument"), 0);
	standalone.GetSignature().AddParameter(LogicalType::INTEGER);
	standalone.SetBindCallback(BindDropLastAggregateArgument);
	standalone.SetCatalogName(Identifier::SystemCatalog());
	standalone.SetSchemaName(schema_name);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(Constant(Value::INTEGER(7)));
	arguments.push_back(Constant(Value::INTEGER(99)));
	auto standalone_bound = standalone.Bind(*connection.context, std::move(arguments));
	RequireFunctionIssue(BoundExpressionSQLExporter::Export(*standalone_bound, context),
	                     LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, root_path, Identifier::SystemCatalog(),
	                     schema_name, standalone.GetName(), {LogicalType::INTEGER, LogicalType::INTEGER},
	                     LogicalType::BIGINT);
	standalone_bound->Cast<BoundAggregateExpression>().GetChildrenMutable()[0].reset();
	RequireIssue(BoundExpressionSQLExporter::Export(*standalone_bound, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, root_path);
	connection.Rollback();
}

TEST_CASE("Function deserialization restores enclosing context after callback exceptions",
          "[bound_expression_sql_export][serialization]") {
	DuckDB db;
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "sql_export_deserialization_context");
	ScalarFunction function("throwing_deserializer", {LogicalType::INTEGER}, LogicalType::INTEGER,
	                        ScalarFunction::NopFunction);
	function.SetSerializeCallback([](Serializer &, const optional_ptr<FunctionData>, const BoundScalarFunction &) {});
	function.SetDeserializeCallback([](Deserializer &deserializer, BoundScalarFunction &) -> unique_ptr<FunctionData> {
		REQUIRE(deserializer.Get<const LogicalType &>() == LogicalType::INTEGER);
		auto &children = deserializer.Get<const const_expression_list_t &>();
		REQUIRE(children.size() == 1);
		REQUIRE(children[0].get().GetReturnType() == LogicalType::INTEGER);
		throw InvalidInputException("Synthetic deserialization failure");
	});
	loader.RegisterFunction(std::move(function));
	connection.BeginTransaction();
	auto plan = BindExportQuery(connection, "SELECT throwing_deserializer(7::INTEGER)");
	auto expression = FindExpression(*plan, [](const Expression &candidate) {
		return candidate.GetExpressionType() == ExpressionType::BOUND_FUNCTION;
	});
	REQUIRE(expression);
	MemoryStream stream(Allocator::Get(*connection.context));
	BinarySerializer::Serialize(*expression, stream);
	stream.Rewind();
	BinaryDeserializer deserializer(stream);
	bound_parameter_map_t parameters;
	deserializer.Set<ClientContext &>(*connection.context);
	deserializer.Set<bound_parameter_map_t &>(parameters);
	const LogicalType enclosing_type = LogicalType::VARCHAR;
	auto enclosing_child = Constant(Value::BIGINT(42));
	const const_expression_list_t enclosing_children {*enclosing_child};
	deserializer.Set<const LogicalType &>(enclosing_type);
	deserializer.Set<const const_expression_list_t &>(enclosing_children);
	REQUIRE_THROWS_AS(deserializer.Deserialize<Expression>(), InvalidInputException);
	REQUIRE(&deserializer.Get<const LogicalType &>() == &enclosing_type);
	REQUIRE(&deserializer.Get<const const_expression_list_t &>() == &enclosing_children);
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

	auto standalone_aggregate = SyntheticSum(Identifier("count_if"), 0);
	standalone_aggregate.SetCatalogName(Identifier::SystemCatalog());
	standalone_aggregate.SetSchemaName(Identifier::DefaultSchema());
	vector<unique_ptr<Expression>> aggregate_children;
	aggregate_children.push_back(Constant(Value::INTEGER(7)));
	auto aggregate = standalone_aggregate.Bind(*connection.context, std::move(aggregate_children));
	REQUIRE(aggregate->Function().GetName() == "count_if");
	REQUIRE(EvaluateAggregate(*aggregate) == Value::BIGINT(7));

	connection.Commit();
	require_core_functions_absent();
}
#endif

TEST_CASE("Bound expression SQL export trusts native function definitions", "[bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	ScalarFunction standalone_scalar(Identifier("abs"), {LogicalType::INTEGER}, LogicalType::INTEGER,
	                                 ScalarFunction::NopFunction);
	standalone_scalar.SetCatalogName(Identifier::SystemCatalog());
	standalone_scalar.SetSchemaName(Identifier::DefaultSchema());
	vector<unique_ptr<Expression>> scalar_children;
	scalar_children.push_back(Constant(Value::INTEGER(-7)));
	auto scalar = standalone_scalar.Bind(*connection.context, std::move(scalar_children));
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *scalar) == Value::INTEGER(-7));
	// A deliberately misleading native definition is unsupported caller misuse, not an authentication test.
	REQUIRE_NOTHROW(BoundExpressionSQLExporter::Export(*scalar, context));

	auto &catalog = Catalog::GetSystemCatalog(*connection.context);
	auto &abs_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    *connection.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("abs")));
	auto abs_definition = abs_entry.functions.GetFunctionByArguments(*connection.context, {LogicalType::INTEGER});
	FunctionBinder function_binder(*connection.context);
	auto bind_scalar_definition = [&](shared_ptr<const ScalarFunction> definition) {
		vector<unique_ptr<Expression>> children;
		children.push_back(Constant(Value::INTEGER(-7)));
		return function_binder.BindScalarFunction(std::move(definition), std::move(children));
	};
	auto canonical_scalar = bind_scalar_definition(abs_definition);
	REQUIRE(canonical_scalar->Cast<BoundFunctionExpression>().Function().GetDefinition() == abs_definition);
	REQUIRE(BoundExpressionSQLExporter::Export(*canonical_scalar, context).IsSuccess());
	auto canonical_scalar_copy = canonical_scalar->Copy();
	REQUIRE(canonical_scalar_copy->Cast<BoundFunctionExpression>().Function().GetDefinition() == abs_definition);
	REQUIRE(BoundExpressionSQLExporter::Export(*canonical_scalar_copy, context).IsSuccess());

	vector<unique_ptr<Expression>> copied_children;
	copied_children.push_back(Constant(Value::INTEGER(-7)));
	auto copied_scalar = abs_definition->Bind(*connection.context, std::move(copied_children));
	RequireRoundTrip(connection, *copied_scalar, context, string(), "abs(-7::INTEGER)");
	for (bool has_catalog : {false, true}) {
		for (bool has_schema : {false, true}) {
			auto definition = make_shared_ptr<ScalarFunction>(*abs_definition);
			definition->SetCatalogName(has_catalog ? Identifier::SystemCatalog() : Identifier());
			definition->SetSchemaName(has_schema ? Identifier::DefaultSchema() : Identifier());
			auto bound = bind_scalar_definition(std::move(definition));
			auto exported = BoundExpressionSQLExporter::Export(*bound, context);
			if (!has_catalog || !has_schema) {
				RequireIssue(exported, LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
				RequireIssue(BoundExpressionSQLExporter::Export(*bound->Copy(), context),
				             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
				continue;
			}
			REQUIRE(exported.IsSuccess());
			auto &name = exported.GetValue()->Cast<FunctionExpression>().GetQualifiedName();
			REQUIRE(name.Name() == Identifier("abs"));
			REQUIRE(name.Catalog() == Identifier::SystemCatalog());
			REQUIRE(name.Schema() == Identifier::DefaultSchema());
			RequireRoundTrip(connection, *bound, context, string(), "abs(-7::INTEGER)");
		}
	}

	// Shared-pointer binding trusts definition copies to preserve the catalog function's semantics.
	vector<pair<string, shared_ptr<const ScalarFunction>>> scalar_copies;
	scalar_copies.emplace_back("copy construction", make_shared_ptr<ScalarFunction>(*abs_definition));
	ScalarFunction scalar_move_source(*abs_definition);
	scalar_copies.emplace_back("move construction", make_shared_ptr<ScalarFunction>(std::move(scalar_move_source)));
	auto scalar_copy_assignment =
	    make_shared_ptr<ScalarFunction>(Identifier("placeholder"), vector<LogicalType> {LogicalType::INTEGER},
	                                    LogicalType::INTEGER, ScalarFunction::NopFunction);
	*scalar_copy_assignment = *abs_definition;
	scalar_copies.emplace_back("copy assignment", std::move(scalar_copy_assignment));
	ScalarFunction scalar_move_assignment_source(*abs_definition);
	auto scalar_move_assignment =
	    make_shared_ptr<ScalarFunction>(Identifier("placeholder"), vector<LogicalType> {LogicalType::INTEGER},
	                                    LogicalType::INTEGER, ScalarFunction::NopFunction);
	*scalar_move_assignment = std::move(scalar_move_assignment_source);
	scalar_copies.emplace_back("move assignment", std::move(scalar_move_assignment));
	ScalarFunctionSet scalar_set(Identifier("abs"));
	scalar_set.functions.push_back(abs_definition);
	scalar_set.ApplyToFunctions([](ScalarFunction &) {});
	scalar_copies.emplace_back("FunctionSet::ApplyToFunctions", scalar_set.GetFunctionByOffset(0));
	for (auto &entry : scalar_copies) {
		INFO(entry.first);
		auto copied = bind_scalar_definition(entry.second);
		REQUIRE(copied->Cast<BoundFunctionExpression>().Function().GetDefinition() == entry.second);
		REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *copied) == Value::INTEGER(7));
		RequireRoundTrip(connection, *copied, context, string(), "abs(-7::INTEGER)");
		vector<unique_ptr<Expression>> children;
		children.push_back(Constant(Value::INTEGER(-7)));
		auto standalone_copy = entry.second->Bind(*connection.context, std::move(children));
		RequireRoundTrip(connection, *standalone_copy, context, string(), "abs(-7::INTEGER)");
	}

	auto standalone_aggregate = SyntheticSum(Identifier("sum"), 1);
	standalone_aggregate.SetCatalogName(Identifier::SystemCatalog());
	standalone_aggregate.SetSchemaName(Identifier::DefaultSchema());
	vector<unique_ptr<Expression>> aggregate_children;
	aggregate_children.push_back(Constant(Value::INTEGER(7)));
	auto aggregate = standalone_aggregate.Bind(*connection.context, std::move(aggregate_children));
	REQUIRE(EvaluateAggregate(*aggregate) == Value::BIGINT(8));
	REQUIRE_NOTHROW(BoundExpressionSQLExporter::Export(*aggregate, context));
	auto &sum_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    *connection.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("sum")));
	auto sum_definition = sum_entry.functions.GetFunctionByArguments(*connection.context, {LogicalType::INTEGER});
	auto bind_aggregate_definition = [&](shared_ptr<const AggregateFunction> definition) {
		vector<unique_ptr<Expression>> children;
		children.push_back(Constant(Value::INTEGER(7)));
		return function_binder.BindAggregateFunction(std::move(definition), std::move(children));
	};
	auto canonical_aggregate = bind_aggregate_definition(sum_definition);
	for (bool has_catalog : {false, true}) {
		for (bool has_schema : {false, true}) {
			auto definition = make_shared_ptr<AggregateFunction>(*sum_definition);
			definition->SetCatalogName(has_catalog ? Identifier::SystemCatalog() : Identifier());
			definition->SetSchemaName(has_schema ? Identifier::DefaultSchema() : Identifier());
			auto bound = bind_aggregate_definition(std::move(definition));
			if (!has_catalog || !has_schema) {
				RequireIssue(BoundExpressionSQLExporter::Export(*bound, context),
				             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
				RequireIssue(BoundExpressionSQLExporter::Export(*bound->Copy(), context),
				             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
			} else {
				RequireRoundTrip(connection, *bound, context, string(), "sum(7::INTEGER)");
			}
		}
	}
	REQUIRE(canonical_aggregate->Function().GetDefinition() == sum_definition);
	REQUIRE(BoundExpressionSQLExporter::Export(*canonical_aggregate, context).IsSuccess());
	auto canonical_aggregate_copy = canonical_aggregate->Copy();
	REQUIRE(canonical_aggregate_copy->Cast<BoundAggregateExpression>().Function().GetDefinition() == sum_definition);
	REQUIRE(BoundExpressionSQLExporter::Export(*canonical_aggregate_copy, context).IsSuccess());
	auto &sum_no_overflow_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    *connection.context,
	    QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("sum_no_overflow")));
	auto sum_no_overflow =
	    sum_no_overflow_entry.functions.GetFunctionByArguments(*connection.context, {LogicalType::INTEGER});
	canonical_aggregate->FunctionMutable().ReplaceImplementation(*sum_no_overflow);
	REQUIRE(canonical_aggregate->Function().GetDefinition() == sum_definition);
	REQUIRE(BoundExpressionSQLExporter::Export(*canonical_aggregate, context).IsSuccess());

	vector<unique_ptr<Expression>> copied_aggregate_children;
	copied_aggregate_children.push_back(Constant(Value::INTEGER(7)));
	auto copied_aggregate = sum_definition->Bind(*connection.context, std::move(copied_aggregate_children));
	REQUIRE(EvaluateAggregate(*copied_aggregate) == Value::HUGEINT(hugeint_t(7)));
	RequireRoundTrip(connection, *copied_aggregate, context, string(), "sum(7::INTEGER)");

	vector<pair<string, shared_ptr<const AggregateFunction>>> aggregate_copies;
	aggregate_copies.emplace_back("copy construction", make_shared_ptr<AggregateFunction>(*sum_definition));
	AggregateFunction aggregate_move_source(*sum_definition);
	aggregate_copies.emplace_back("move construction",
	                              make_shared_ptr<AggregateFunction>(std::move(aggregate_move_source)));
	auto aggregate_copy_assignment = make_shared_ptr<AggregateFunction>(SyntheticSum(Identifier("placeholder"), 0));
	*aggregate_copy_assignment = *sum_definition;
	aggregate_copies.emplace_back("copy assignment", std::move(aggregate_copy_assignment));
	AggregateFunction aggregate_move_assignment_source(*sum_definition);
	auto aggregate_move_assignment = make_shared_ptr<AggregateFunction>(SyntheticSum(Identifier("placeholder"), 0));
	*aggregate_move_assignment = std::move(aggregate_move_assignment_source);
	aggregate_copies.emplace_back("move assignment", std::move(aggregate_move_assignment));
	AggregateFunctionSet aggregate_set(Identifier("sum"));
	aggregate_set.functions.push_back(sum_definition);
	aggregate_set.ApplyToFunctions([](AggregateFunction &) {});
	aggregate_copies.emplace_back("FunctionSet::ApplyToFunctions", aggregate_set.GetFunctionByOffset(0));
	for (auto &entry : aggregate_copies) {
		INFO(entry.first);
		auto copied = bind_aggregate_definition(entry.second);
		REQUIRE(copied->Function().GetDefinition() == entry.second);
		REQUIRE(EvaluateAggregate(*copied) == Value::HUGEINT(hugeint_t(7)));
		RequireRoundTrip(connection, *copied, context, string(), "sum(7::INTEGER)");
		vector<unique_ptr<Expression>> children;
		children.push_back(Constant(Value::INTEGER(7)));
		auto standalone_copy = entry.second->Bind(*connection.context, std::move(children));
		RequireRoundTrip(connection, *standalone_copy, context, string(), "sum(7::INTEGER)");
	}
	connection.Rollback();
}

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
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	vector<unique_ptr<Expression>> structural;
	structural.push_back(
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT));
	structural.push_back(BoundBetweenExpression::Create(Constant(Value::INTEGER(7)), Constant(Value::INTEGER(2)),
	                                                    Constant(Value::INTEGER(9)), true, true));
	for (auto &expression : structural) {
		auto &data = *expression->Cast<BoundFunctionExpression>().BindInfoMutable();
		auto &alias = data;
		data = alias;
		REQUIRE(BoundExpressionSQLExporter::Export(*expression, context).IsSuccess());
		data = std::move(alias);
		REQUIRE(BoundExpressionSQLExporter::Export(*expression, context).IsSuccess());
		REQUIRE(BoundExpressionSQLExporter::Export(*expression->Copy(), context).IsSuccess());
		auto restored = BinaryRoundTrip(*connection.context, *expression);
		REQUIRE(BoundExpressionSQLExporter::Export(*restored, context).IsSuccess());
	}

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
	SpoofCastFunctionData spoof_cast_move(std::move(spoof_cast_copy));
	REQUIRE(spoof_cast_move.value == 42);
	spoofed_cast->Cast<BoundFunctionExpression>().BindInfoMutable() =
	    make_uniq<SpoofCastFunctionData>(std::move(spoof_cast_move));
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
	SpoofBetweenFunctionData spoof_between_move(std::move(spoof_between_copy));
	REQUIRE(spoof_between_move.value == 84);
	spoofed_between->Cast<BoundFunctionExpression>().BindInfoMutable() =
	    make_uniq<SpoofBetweenFunctionData>(std::move(spoof_between_move));
	RequireIssue(BoundExpressionSQLExporter::Export(*spoofed_between, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);

	auto wrong_arity = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(2)),
	                                                  Constant(Value::INTEGER(9)), true, true);
	wrong_arity->Cast<BoundFunctionExpression>().GetChildrenMutable().pop_back();
	RequireIssue(BoundExpressionSQLExporter::Export(*wrong_arity, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
	connection.Rollback();
}

TEST_CASE("Binary function round trips recover represented SQL calls", "[bound_expression_sql_export][serialization]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	for (auto sql : {"abs(-7)", "log2(8)", "equi_width_bins(0, 10, 2, true)"}) {
		INFO(sql);
		auto plan = BindExportQuery(connection, string("SELECT ") + sql);
		auto expression = FindExpression(*plan, [](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
			       candidate.GetExpressionType() == ExpressionType::BOUND_FUNCTION;
		});
		REQUIRE(expression);
		RequireRoundTrip(connection, *expression, {}, string(), sql);
		auto restored = BinaryRoundTrip(*connection.context, *expression);
		REQUIRE(restored->GetReturnType() == expression->GetReturnType());
		REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *restored) ==
		        ExpressionExecutor::EvaluateScalar(*connection.context, *expression));
		RequireRoundTrip(connection, *restored, {}, string(), sql);
	}
	for (auto window : {false, true}) {
		auto plan = BindExportQuery(connection, "SELECT quantile_cont(i, 0.5)" + string(window ? " OVER ()" : "") +
		                                            " FROM (VALUES (1), (2)) v(i)");
		auto expression = FindExpression(*plan, [&](const Expression &candidate) {
			return candidate.GetExpressionClass() ==
			       (window ? ExpressionClass::BOUND_WINDOW : ExpressionClass::BOUND_AGGREGATE);
		});
		REQUIRE(expression);
		auto restored = BinaryRoundTrip(*connection.context, *expression);
		REQUIRE(restored->GetReturnType() == LogicalType::DOUBLE);
		if (!window) {
			auto source = CreateSyntheticSQLSource(*restored);
			RequireRoundTrip(connection, *restored, source.context, source.from_clause,
			                 "quantile_cont(v.exported_0, 0.5)");
		}
	}
	REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=true"));
	auto result = connection.Query("SELECT log2(8), equi_width_bins(0, 10, 2, true), "
	                               "quantile_cont(i, 0.5) OVER () FROM (VALUES (1), (2)) v(i)");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetTypes() ==
	        vector<LogicalType> {LogicalType::DOUBLE, LogicalType::LIST(LogicalType::INTEGER), LogicalType::DOUBLE});
	REQUIRE(result->RowCount() == 2);
	REQUIRE(result->GetValue(0, 0) == Value::DOUBLE(3));
	REQUIRE(result->GetValue(1, 0) == Value::LIST({Value::INTEGER(5), Value::INTEGER(10)}));
	REQUIRE(result->GetValue(2, 0) == Value::DOUBLE(1.5));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO integers VALUES (42), (84)"));
	auto histogram = connection.Query("SELECT * FROM histogram_values(integers, i, bin_count := 2)");
	REQUIRE_FALSE(histogram->HasError());
	REQUIRE(histogram->GetTypes() == vector<LogicalType> {LogicalType::INTEGER, LogicalType::UBIGINT});
	REQUIRE(histogram->RowCount() == 3);
	REQUIRE(histogram->GetValue(0, 0) == Value::INTEGER(60));
	REQUIRE(histogram->GetValue(1, 0) == Value::UBIGINT(1));
	REQUIRE(histogram->GetValue(0, 1) == Value::INTEGER(80));
	REQUIRE(histogram->GetValue(1, 1) == Value::UBIGINT(0));
	REQUIRE(histogram->GetValue(0, 2) == Value::INTEGER(100));
	REQUIRE(histogram->GetValue(1, 2) == Value::UBIGINT(1));
	connection.Rollback();
}

TEST_CASE("Rewritten scalar calls fail SQL export as unsupported", "[bound_expression_sql_export][serialization]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE lists(l INTEGER[], c INTEGER)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO lists VALUES ([10, 9, 8, 7, 6], 5)"));
	connection.BeginTransaction();
	auto plan = OptimizeExportQuery(connection, "SELECT [x + c FOR x, i IN l IF i > 2] FROM lists");
	auto filter = FindExpression(*plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       candidate.Cast<BoundFunctionExpression>().Function().GetName() == "list_filter";
	});
	REQUIRE(filter);
	REQUIRE(filter->GetReturnType() == LogicalType::LIST(LogicalType::INTEGER));
	REQUIRE(filter->Cast<BoundFunctionExpression>().Function().GetLogicalReturnType() != filter->GetReturnType());
	BoundExpressionSQLExportContext context;
	REQUIRE(BoundExpressionSQLExporter::Export(*filter, context).HasError());
	auto restored = BinaryRoundTrip(*connection.context, *filter);
	REQUIRE(restored->GetReturnType() == filter->GetReturnType());
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(BoundExpressionSQLExporter::Export(*filter, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	RequireIssue(BoundExpressionSQLExporter::Export(*restored, context),
	             LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
	REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=true"));
	auto result = connection.Query("SELECT [x + c FOR x, i IN l IF i > 2] FROM lists");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetTypes() == vector<LogicalType> {LogicalType::LIST(LogicalType::INTEGER)});
	REQUIRE(result->GetValue(0, 0) == Value::LIST({Value::INTEGER(13), Value::INTEGER(12), Value::INTEGER(11)}));
	connection.Rollback();
}

TEST_CASE("Live optimized decimal sum exports its logical result", "[bound_expression_sql_export][optimizer]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE decimal_values(i DECIMAL(9,2))"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO decimal_values VALUES (1.25), (2.50)"));
	connection.BeginTransaction();
	auto plan = OptimizeExportQuery(connection, "SELECT sum(i) FROM decimal_values");
	auto expression = FindExpression(*plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE;
	});
	REQUIRE(expression);
	auto &aggregate = expression->Cast<BoundAggregateExpression>();
	REQUIRE(aggregate.Function().GetName() == "sum_no_overflow");
	REQUIRE(aggregate.Function().GetDefinition()->GetName() == "sum");
	REQUIRE(aggregate.Function().GetLogicalArguments() == vector<LogicalType> {LogicalType::DECIMAL(9, 2)});
	REQUIRE(aggregate.Function().GetLogicalReturnType() == LogicalType::DECIMAL(38, 2));
	REQUIRE(aggregate.GetReturnType() == LogicalType::DECIMAL(38, 2));
	auto &column = aggregate.GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto context = ResolveBinding(column.Binding(), {Identifier("i")}, column.GetReturnType());
	auto exported = BoundExpressionSQLExporter::Export(aggregate, context);
	REQUIRE(exported.IsSuccess());
	auto &cast = exported.GetValue()->Cast<CastExpression>();
	RequireCastTarget(cast, LogicalType::DECIMAL(38, 2));
	REQUIRE(cast.Child().Cast<FunctionExpression>().FunctionName() == "sum");
	DuckDB receiving_db;
	Connection receiving_connection(receiving_db);
	auto result =
	    receiving_connection.Query("SELECT " + exported.GetValue()->ToString() +
	                               " FROM (VALUES (1.25::DECIMAL(9,2)), (2.50::DECIMAL(9,2))) values_to_sum(i)");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetTypes() == vector<LogicalType> {LogicalType::DECIMAL(38, 2)});
	REQUIRE(result->GetValue(0, 0) == Value::DECIMAL(hugeint_t(375), 38, 2));
	RequireRoundTrip(connection, *aggregate.Copy(), context, " FROM decimal_values", "sum(i)");
	auto restored = BinaryRoundTrip(*connection.context, aggregate);
	auto &restored_aggregate = restored->Cast<BoundAggregateExpression>();
	REQUIRE(restored_aggregate.Function().GetName() == "sum_no_overflow");
	REQUIRE(restored_aggregate.Function().GetDefinition()->GetName() == "sum");
	REQUIRE(restored_aggregate.Function().GetLogicalArguments() == aggregate.Function().GetLogicalArguments());
	REQUIRE(restored_aggregate.Function().GetLogicalReturnType() == LogicalType::DECIMAL(38, 2));
	RequireRoundTrip(connection, restored_aggregate, context, " FROM decimal_values", "sum(i)");
	connection.Rollback();
}

TEST_CASE("Widened decimal calls retain execution and fail closed when SQL arguments differ",
          "[bound_expression_sql_export][serialization]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	for (auto width : {4, 9, 18, 37}) {
		INFO(width);
		REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=false"));
		auto type = LogicalType::DECIMAL(width, 0);
		auto result_type = LogicalType::DECIMAL(width + 1, 0);
		REQUIRE_NO_FAIL(connection.Query("CREATE OR REPLACE TABLE rounding_values(i " + type.ToString() + ")"));
		REQUIRE_NO_FAIL(connection.Query("INSERT INTO rounding_values VALUES (" + string(width, '9') + ")"));
		auto plan = BindExportQuery(connection, "SELECT round(i, -1) FROM rounding_values");
		auto expression = FindExpression(*plan, [](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
			       candidate.Cast<BoundFunctionExpression>().Function().GetName() == "round";
		});
		REQUIRE(expression);
		auto &function = expression->Cast<BoundFunctionExpression>().Function();
		REQUIRE(function.GetLogicalArguments()[0] == type);
		REQUIRE(function.GetLogicalReturnType() == result_type);
		REQUIRE(function.GetArguments()[0] == (width == 37 ? type : result_type));
		auto restored = BinaryRoundTrip(*connection.context, *expression);
		REQUIRE(restored->GetReturnType() == result_type);
		REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=true"));
		auto result = connection.Query("SELECT round(i, -1) FROM rounding_values");
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->GetTypes() == vector<LogicalType> {result_type});
		REQUIRE(result->GetValue(0, 0) == Value("1" + string(width, '0')).DefaultCastAs(result_type));
		auto &column = expression->Cast<BoundFunctionExpression>().GetChildren()[0];
		vector<SQLBindingEntry> bindings;
		CollectSQLBindings(*column, bindings);
		REQUIRE(bindings.size() == 1);
		auto context = ResolveBinding(bindings[0].binding, {Identifier("i")}, type);
		if (width == 37) {
			RequireRoundTrip(connection, *expression, context, " FROM rounding_values", "round(i, -1)");
		} else {
			auto exported = BoundExpressionSQLExporter::Export(*expression, context);
			REQUIRE(exported.IsValid());
			REQUIRE(exported.HasError());
			REQUIRE(exported.GetIssues().size() == 1);
			REQUIRE(exported.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION);
			REQUIRE(exported.GetIssues()[0].phase == LogicalPlanVerificationPhase::EXPRESSION_EXPORT);
		}
	}
	REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=false"));
	auto hash_plan = BindExportQuery(connection, "SELECT hash(i) FROM rounding_values");
	auto hash = FindExpression(*hash_plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       candidate.Cast<BoundFunctionExpression>().Function().GetName() == "hash";
	});
	REQUIRE(hash);
	REQUIRE_FALSE(hash->Cast<BoundFunctionExpression>().Function().GetArguments()[0].IsComplete());
	auto hash_copy = hash->Copy();
	auto &hash_column = hash->Cast<BoundFunctionExpression>().GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto hash_context = ResolveBinding(hash_column.Binding(), {Identifier("i")}, hash_column.GetReturnType());
	RequireRoundTrip(connection, *hash_copy, hash_context, " FROM rounding_values", "hash(i)");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export preserves explicit string_agg separators",
          "[bound_expression_sql_export][aggregate][serialization]") {
	DuckDB db;
	Connection connection(db);
	Connection baseline(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE string_values(x VARCHAR)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO string_values VALUES ('a'), ('b')"));
	REQUIRE_NO_FAIL(baseline.Query("SET debug_verify_serializer=true"));
	REQUIRE_NO_FAIL(baseline.Query("PRAGMA disable_optimizer"));
	connection.BeginTransaction();

	vector<string> calls {"string_agg(x, '&')",
	                      "string_agg(DISTINCT x, '&')",
	                      "string_agg(x, '&') FILTER (WHERE x IS NOT NULL)",
	                      "string_agg(x, '&' ORDER BY x)",
	                      "string_agg(DISTINCT x, '&' ORDER BY x)",
	                      "string_agg(x, '&') EXPORT_STATE",
	                      "string_agg(x, '&' ORDER BY x) EXPORT_STATE"};
	for (auto &call : calls) {
		INFO(call);
		REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=false"));
		auto plan = BindExportQuery(connection, "SELECT " + call + " FROM string_values");
		auto expression = FindExpression(*plan, [](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE;
		});
		REQUIRE(expression);
		auto &aggregate = expression->Cast<BoundAggregateExpression>();
		REQUIRE(aggregate.BindInfo());
		REQUIRE(aggregate.GetChildren().size() == 2);
		REQUIRE(aggregate.Function().GetArguments().size() == 2);
		REQUIRE(aggregate.Function().GetLogicalArguments() ==
		        vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR});
		REQUIRE(aggregate.GetChildren()[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT);
		REQUIRE(aggregate.GetChildren()[1]->Cast<BoundConstantExpression>().GetValue() == Value("&"));
		auto &column = aggregate.GetChildren()[0]->Cast<BoundColumnRefExpression>();
		auto context = ResolveBinding(column.Binding(), {Identifier("x")}, LogicalType::VARCHAR);
		for (auto serialize : {false, true}) {
			auto restored = serialize ? BinaryRoundTrip(*connection.context, aggregate) : aggregate.Copy();
			auto &restored_aggregate = restored->Cast<BoundAggregateExpression>();
			REQUIRE(restored_aggregate.GetChildren().size() == 2);
			REQUIRE(restored_aggregate.Function().GetArguments().size() == 2);
			auto exported = BoundExpressionSQLExporter::Export(*restored, context);
			REQUIRE(exported.IsValid());
			REQUIRE(exported.IsSuccess());
			auto sql = exported.GetValue()->ToString();
			auto oracle = call;
			if (aggregate.StateExportMode() == AggregateStateExportMode::STATE_EXPORT) {
				sql = "finalize(" + sql + ")";
				oracle = "finalize(" + oracle + ")";
			}
			if (aggregate.GetAggregateType() == AggregateType::DISTINCT &&
			    (!aggregate.GetOrderBys() || aggregate.GetOrderBys()->orders.empty())) {
				// DISTINCT without ORDER BY does not define concatenation order.
				sql = "array_to_string(list_sort(string_split(" + sql + ", '&')), '&')";
				oracle = "array_to_string(list_sort(string_split(" + oracle + ", '&')), '&')";
			}
			REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=true"));
			auto result = connection.Query("SELECT " + sql + " FROM string_values");
			auto expected = baseline.Query("SELECT " + oracle + " FROM string_values");
			REQUIRE_FALSE(result->HasError());
			REQUIRE_FALSE(expected->HasError());
			REQUIRE(result->Equals(*expected, false));
			REQUIRE(result->GetTypes() == vector<LogicalType> {LogicalType::VARCHAR});
			REQUIRE(result->GetValue(0, 0) == Value("a&b"));
		}
	}
	connection.Rollback();
}

TEST_CASE("SQL export recovers calls from supported binary compatibility targets",
          "[bound_expression_sql_export][serialization]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE compatibility_values(i INTEGER)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO compatibility_values VALUES (1), (2)"));
	connection.BeginTransaction();
	for (auto call : {"abs(i)", "log2(i)", "sum(i)", "quantile_cont(i, 0.5)"}) {
		INFO(call);
		auto plan = BindExportQuery(connection, string("SELECT ") + call + " FROM compatibility_values");
		auto expression = FindExpression(*plan, [](const Expression &candidate) {
			return candidate.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE ||
			       (candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
			        candidate.GetExpressionType() == ExpressionType::BOUND_FUNCTION);
		});
		REQUIRE(expression);
		auto source = CreateSyntheticSQLSource(*expression);
		auto current =
		    BinaryRoundTrip(*connection.context, *expression, StorageCompatibility::FromIndex(StorageVersion::V2_0_0));
		LogicalPlanVerificationPath current_path;
		current_path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
		RequireRoundTrip(connection, *current, source.context, source.from_clause,
		                 StringUtil::Replace(call, "(i", "(v.exported_0"));
		auto legacy =
		    BinaryRoundTrip(*connection.context, *expression, StorageCompatibility::FromIndex(StorageVersion::V1_5_0));
		REQUIRE(legacy->GetReturnType() == expression->GetReturnType());
		LogicalPlanVerificationPath path;
		path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
		REQUIRE(BoundExpressionSQLExporter::Export(*legacy, source.context).IsSuccess());
	}
	REQUIRE_NO_FAIL(connection.Query("SET storage_compatibility_version='v1.5.0'"));
	REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=true"));
	auto result = connection.Query("SELECT abs(-7), log2(8), sum(i), quantile_cont(i, 0.5) FROM compatibility_values");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(7));
	REQUIRE(result->GetValue(1, 0) == Value::DOUBLE(3));
	REQUIRE(result->GetValue(2, 0) == Value::HUGEINT(hugeint_t(3)));
	REQUIRE(result->GetValue(3, 0) == Value::DOUBLE(1.5));
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
	auto serialized_sum = BinaryRoundTrip(*connection.context, optimized_sum_bound);
	REQUIRE(serialized_sum->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
	auto &serialized_sum_bound = serialized_sum->Cast<BoundAggregateExpression>();
	REQUIRE(serialized_sum_bound.Function().GetName() == "sum_no_overflow");
	REQUIRE(serialized_sum_bound.Function().GetDefinition());
	REQUIRE(serialized_sum_bound.Function().GetDefinition()->GetName() == "sum");
	auto post_serialization = BoundExpressionSQLExporter::Export(serialized_sum_bound, optimized_sum_context);
	LogicalPlanVerificationPath serialized_path;
	serialized_path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	REQUIRE(post_serialization.IsSuccess());
	RequireRoundTrip(connection, serialized_sum_bound, optimized_sum_context, " FROM aggregate_values AS v",
	                 "sum(v.i)");
	auto rebound_sum =
	    connection.Query("SELECT " + pre_serialization.GetValue()->ToString() + " FROM aggregate_values AS v");
	REQUIRE_FALSE(rebound_sum->HasError());
	REQUIRE(rebound_sum->GetTypes() == vector<LogicalType> {LogicalType::HUGEINT});
	REQUIRE(rebound_sum->GetValue(0, 0).ToString() == "5");

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
	auto serialized_quantile = BinaryRoundTrip(*connection.context, quantile_bound);
	REQUIRE(serialized_quantile->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
	auto &serialized_quantile_bound = serialized_quantile->Cast<BoundAggregateExpression>();
	REQUIRE(serialized_quantile_bound.BindInfo());
	REQUIRE(serialized_quantile_bound.Function().GetDefinition());
	REQUIRE(serialized_quantile_bound.Function().GetDefinition()->GetName() == "quantile_cont");
	RequireRoundTrip(connection, serialized_quantile_bound, quantile_source.context, quantile_source.from_clause,
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
	REQUIRE(count_aggregate.Function().GetDefinition()->GetCatalogName().empty());
	REQUIRE(count_aggregate.Function().GetDefinition()->GetSchemaName().empty());
	LogicalPlanVerificationPath count_path;
	count_path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(count_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, count_path);
	auto serialized_count = BinaryRoundTrip(*connection.context, *count);
	REQUIRE(serialized_count->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
	auto &serialized_count_bound = serialized_count->Cast<BoundAggregateExpression>();
	REQUIRE(serialized_count_bound.Function().GetName() == "count_star");
	REQUIRE(serialized_count_bound.Function().GetDefinition());
	REQUIRE(serialized_count_bound.Function().GetDefinition()->GetName() == "count_star");
	auto serialized_count_result = BoundExpressionSQLExporter::Export(serialized_count_bound, count_context);
	REQUIRE(serialized_count_result.IsSuccess());
	RequireRoundTrip(connection, serialized_count_bound, count_context, " FROM count_values", "count(*)");
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
	RequireCastTarget(*success.GetValue(), LogicalType::SMALLINT);

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

TEST_CASE("SQL export preserves optimizer function identity despite search path shadowing",
          "[bound_expression_sql_export][optimizer][serialization]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	REQUIRE_NO_FAIL(connection.Query("CREATE SCHEMA shadow"));
	for (auto name : {"prefix", "suffix", "contains"}) {
		REQUIRE_NO_FAIL(connection.Query("CREATE MACRO shadow." + string(name) + "(a,b) AS false"));
	}
	REQUIRE_NO_FAIL(connection.Query("CREATE MACRO shadow.count(a) AS 999::BIGINT"));
	REQUIRE_NO_FAIL(connection.Query("SET search_path='shadow,main'"));
	struct Case {
		string source;
		string from;
		Identifier name;
		string arguments;
	};
	vector<Case> cases {{"s LIKE 'a%'", " FROM (VALUES ('abc'),('cab'),(NULL)) v(s)", "prefix", "s, 'a'"},
	                    {"s LIKE '%a'", " FROM (VALUES ('bca'),('abc'),(NULL)) v(s)", "suffix", "s, 'a'"},
	                    {"s LIKE '%a%'", " FROM (VALUES ('abc'),('bbb'),(NULL)) v(s)", "contains", "s, 'a'"},
	                    {"avg(i)", " FROM (VALUES (1),(2),(NULL)) v(i)", "count", "i"}};
	for (auto &entry : cases) {
		INFO(entry.source);
		auto query = "SELECT " + entry.source + entry.from;
		auto plan = OptimizeExportQuery(connection, query);
		auto expression = FindExpression(*plan, [&](const Expression &candidate) {
			if (candidate.GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
				return candidate.Cast<BoundFunctionExpression>().Function().GetDefinition()->GetName() == entry.name;
			}
			return candidate.GetExpressionType() == ExpressionType::BOUND_AGGREGATE &&
			       candidate.Cast<BoundAggregateExpression>().Function().GetDefinition()->GetName() == entry.name;
		});
		REQUIRE(expression);
		auto &definition =
		    expression->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION
		        ? static_cast<const Function &>(*expression->Cast<BoundFunctionExpression>().Function().GetDefinition())
		        : static_cast<const Function &>(
		              *expression->Cast<BoundAggregateExpression>().Function().GetDefinition());
		REQUIRE(definition.GetCatalogName() == Identifier::SystemCatalog());
		REQUIRE(definition.GetSchemaName() == Identifier::DefaultSchema());
		auto optimized = connection.Query(query);
		REQUIRE_NO_FAIL(*optimized);
		REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
		auto baseline = connection.Query(query);
		REQUIRE_NO_FAIL(*baseline);
		REQUIRE(optimized->Equals(*baseline, false));
		REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
		auto canonical_call = "system.main." + entry.name.GetIdentifierName() + "(" + entry.arguments + ")";
		auto canonical = connection.Query("SELECT " + canonical_call + entry.from);
		auto shadowed =
		    connection.Query("SELECT " + entry.name.GetIdentifierName() + "(" + entry.arguments + ")" + entry.from);
		REQUIRE_NO_FAIL(*canonical);
		REQUIRE_NO_FAIL(*shadowed);
		REQUIRE_FALSE(canonical->Equals(*shadowed, false));
		vector<SQLBindingEntry> bindings;
		CollectSQLBindings(*expression, bindings);
		REQUIRE(bindings.size() == 1);
		auto context = ResolveBinding(
		    bindings[0].binding, {Identifier("v"), Identifier(entry.name == "count" ? "i" : "s")}, bindings[0].type);
		RequireRoundTrip(connection, *expression, context, entry.from, canonical_call);
		auto copied = expression->Copy();
		RequireRoundTrip(connection, *copied, context, entry.from, canonical_call);
		auto restored = BinaryRoundTrip(*connection.context, *expression);
		auto exported = BoundExpressionSQLExporter::Export(*restored, context);
		REQUIRE(exported.IsSuccess());
		auto &name = exported.GetValue()->Cast<FunctionExpression>().GetQualifiedName();
		REQUIRE(name.Catalog() == Identifier::SystemCatalog());
		REQUIRE(name.Schema() == Identifier::DefaultSchema());
		REQUIRE(name.Name() == entry.name);
		RequireRoundTrip(connection, *restored, context, entry.from, canonical_call);
	}
	connection.Rollback();
}

TEST_CASE("Optimized expression SQL export covers qualified AVG and LIKE rewrites",
          "[bound_expression_sql_export][optimizer][serialization]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	struct Case {
		string query;
		vector<Identifier> required_functions;
	};
	vector<Case> cases {
	    {"SELECT avg(i) FROM (VALUES (1),(2),(NULL)) t(i)", {"sum", "count", "/"}},
	    {"SELECT sum(i + 1) FROM (VALUES (1),(2),(NULL)) t(i)", {"sum", "count"}},
	    {"SELECT count(i) FROM (VALUES (1),(2),(NULL)) t(i)", {"count"}},
	    {"SELECT count(*) FROM (VALUES (1),(2)) t(i)", {"count_star"}},
	    {"SELECT sum(i), avg(i), count(DISTINCT i) FROM (VALUES (1),(2),(2),(NULL)) t(i)", {"sum", "count", "/"}},
	    {"SELECT s FROM (VALUES ('abc'),('cab'),('bca'),(NULL)) t(s) WHERE s LIKE 'a%'", {"prefix"}},
	    {"SELECT s FROM (VALUES ('abc'),('cab'),('bca'),(NULL)) t(s) WHERE s LIKE '%a'", {"suffix"}},
	    {"SELECT s FROM (VALUES ('abc'),('cab'),('bca'),(NULL)) t(s) WHERE s LIKE '%a%'", {"contains"}},
	    {"SELECT s FROM (VALUES ('abc'),('cab'),('bca'),(NULL)) t(s) WHERE s NOT LIKE 'a%'", {"prefix"}},
	};
	for (auto &entry : cases) {
		INFO(entry.query);
		auto plan = OptimizeExportQuery(connection, entry.query);
		vector<Identifier> functions;
		std::function<void(const Expression &)> visit_expression = [&](const Expression &expression) {
			if (expression.GetExpressionType() == ExpressionType::BOUND_FUNCTION ||
			    expression.GetExpressionType() == ExpressionType::BOUND_AGGREGATE) {
				auto &definition = expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION
				                       ? static_cast<const Function &>(
				                             *expression.Cast<BoundFunctionExpression>().Function().GetDefinition())
				                       : static_cast<const Function &>(
				                             *expression.Cast<BoundAggregateExpression>().Function().GetDefinition());
				functions.push_back(definition.GetName());
				REQUIRE(definition.GetCatalogName() == Identifier::SystemCatalog());
				REQUIRE(definition.GetSchemaName() == Identifier::DefaultSchema());
				vector<SQLBindingEntry> bindings;
				CollectSQLBindings(expression, bindings);
				SyntheticSQLSource source;
				if (!bindings.empty()) {
					source = CreateSyntheticSQLSource(expression);
				}
				auto result = BoundExpressionSQLExporter::Export(expression, source.context);
				if (result.HasError()) {
					INFO("issue code=" << static_cast<idx_t>(result.GetIssues()[0].code));
					FAIL(result.GetIssues()[0].message);
				}
				REQUIRE(result.IsValid());
				REQUIRE(result.IsSuccess());
				REQUIRE(result.GetValue());
				auto sql = "SELECT " + result.GetValue()->ToString() + source.from_clause;
				auto rebound = connection.Query(sql);
				REQUIRE_NO_FAIL(*rebound);
				REQUIRE(rebound->GetTypes() == vector<LogicalType> {expression.GetReturnType()});
				auto restored = BinaryRoundTrip(*connection.context, expression);
				auto restored_export = BoundExpressionSQLExporter::Export(*restored, source.context);
				REQUIRE(restored_export.IsSuccess());
				auto restored_result =
				    connection.Query("SELECT " + restored_export.GetValue()->ToString() + source.from_clause);
				REQUIRE_NO_FAIL(*restored_result);
				REQUIRE(restored_result->Equals(*rebound, false));
			}
			ExpressionIterator::EnumerateChildren(expression, visit_expression);
		};
		std::function<void(const LogicalOperator &)> visit_operator = [&](const LogicalOperator &op) {
			LogicalOperatorVisitor::EnumerateExpressions(
			    op, [&](const unique_ptr<Expression> *expression) { visit_expression(**expression); });
			for (auto &child : op.children) {
				visit_operator(*child);
			}
		};
		visit_operator(*plan);
		for (auto &required : entry.required_functions) {
			INFO(required);
			REQUIRE(std::find(functions.begin(), functions.end(), required) != functions.end());
		}
		auto optimized = connection.Query(entry.query);
		REQUIRE_NO_FAIL(*optimized);
		REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
		auto baseline = connection.Query(entry.query);
		REQUIRE_NO_FAIL(*baseline);
		REQUIRE(optimized->Equals(*baseline, true));
		REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
	}
	connection.Rollback();
}

TEST_CASE("SQL export covers catalog-bound aggregate and TopN rewrites",
          "[bound_expression_sql_export][optimizer][serialization]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	REQUIRE_NO_FAIL(
	    connection.Query("CREATE TEMP TABLE reuse_owner AS FROM (VALUES (1,10),(2,20),(3,30),(4,40)) t(k,dim_id)"));
	REQUIRE_NO_FAIL(connection.Query(
	    "CREATE TEMP TABLE reuse_dim AS FROM (VALUES (10,'x'),(20,'b'),(30,'x'),(40,'d')) t(id,label)"));
	REQUIRE_NO_FAIL(connection.Query(
	    "CREATE TEMP TABLE reuse_fact AS FROM (VALUES (1,10),(1,20),(2,5),(3,40),(3,50),(4,NULL)) t(k,v)"));
	struct Case {
		string optimizer;
		string query;
		Identifier introduced_function;
	};
	string fact_values;
	for (idx_t index = 0; index < 120; index++) {
		if (index > 0) {
			fact_values += ",";
		}
		fact_values += "(" + to_string(index % 3) + "," + to_string(index % 7) + ")";
	}
	vector<Case> cases {{"grouping_sets",
	                     "SELECT i,j,sum(k) FROM (VALUES (1,1,2),(1,2,3),(2,1,4),(2,2,5)) t(i,j,k) "
	                     "GROUP BY ROLLUP(i,j) ORDER BY ALL",
	                     "combine_aggr"},
	                    {"grouping_sets",
	                     "SELECT i,sum(j) FROM (VALUES (1,2),(1,3),(2,4),(2,5)) t(i,j) "
	                     "GROUP BY GROUPING SETS ((i),()) ORDER BY ALL",
	                     "combine_aggr"},
	                    {"grouping_sets",
	                     "SELECT i LIKE 'a%',sum(j) FROM (VALUES ('a',2),('ab',3),('b',4),(NULL,5)) t(i,j) "
	                     "GROUP BY GROUPING SETS ((i),()) ORDER BY ALL",
	                     "combine_aggr"},
	                    {"partial_aggregate_pushdown",
	                     "SELECT d.a,d.b,d.c,d.d,sum(f.v) FROM (VALUES " + fact_values +
	                         ") f(k,v) "
	                         "JOIN (VALUES (0,1,2,3,4),(1,5,6,7,8),(2,9,10,11,12)) d(k,a,b,c,d) USING(k) "
	                         "GROUP BY d.a,d.b,d.c,d.d ORDER BY ALL",
	                     "combine_aggr"},
	                    {"top_n_window_elimination",
	                     "SELECT i FROM (VALUES (1,1),(1,2),(2,3),(2,4)) t(g,i) "
	                     "QUALIFY row_number() OVER (PARTITION BY g ORDER BY i) <= 1 ORDER BY ALL",
	                     "min"},
	                    {"aggregate_reuse",
	                     "SELECT o.k,d.label,sum(f.v) FROM reuse_owner o JOIN reuse_dim d ON o.dim_id=d.id "
	                     "JOIN reuse_fact f USING(k) WHERE o.k IN "
	                     "(SELECT k FROM reuse_fact GROUP BY k HAVING sum(v)>25) GROUP BY o.k,d.label ORDER BY ALL",
	                     "combine_aggr"},
	                    {"join_filter_pushdown",
	                     "SELECT f.v FROM reuse_fact f JOIN reuse_owner o USING(k) WHERE o.dim_id>10 ORDER BY ALL",
	                     "min"},
	                    {"unused_columns", "SELECT 42 FROM (SELECT sum(i) FROM (VALUES (1),(2)) t(i))", "count_star"}};
	auto collect = [](const LogicalOperator &plan) {
		vector<reference<const Expression>> expressions;
		std::function<void(const Expression &)> visit_expression = [&](const Expression &expression) {
			if (expression.GetExpressionType() == ExpressionType::BOUND_FUNCTION ||
			    expression.GetExpressionType() == ExpressionType::BOUND_AGGREGATE) {
				expressions.emplace_back(expression);
			}
			ExpressionIterator::EnumerateChildren(expression, visit_expression);
		};
		std::function<void(const LogicalOperator &)> visit_operator = [&](const LogicalOperator &op) {
			LogicalOperatorVisitor::EnumerateExpressions(
			    op, [&](const unique_ptr<Expression> *expression) { visit_expression(**expression); });
			// Runtime-filter aggregates live outside the operator's ordinary expression list.
			if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
				auto &join = op.Cast<LogicalComparisonJoin>();
				if (join.filter_pushdown) {
					for (auto &aggregate : join.filter_pushdown->min_max_aggregates) {
						visit_expression(*aggregate);
					}
				}
			}
			for (auto &child : op.children) {
				visit_operator(*child);
			}
		};
		visit_operator(plan);
		return expressions;
	};
	auto count_function = [&](const LogicalOperator &plan, const Identifier &name) {
		idx_t count = 0;
		for (auto &reference : collect(plan)) {
			auto &expression = reference.get();
			auto function_name = expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE
			                         ? expression.Cast<BoundAggregateExpression>().Function().GetDefinition()->GetName()
			                         : expression.Cast<BoundFunctionExpression>().Function().GetDefinition()->GetName();
			count += function_name == name;
		}
		return count;
	};
	for (auto &entry : cases) {
		INFO(entry.optimizer);
		auto plan = OptimizeExportQuery(connection, entry.query);
		REQUIRE(count_function(*plan, entry.introduced_function) > 0);
		REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='" + entry.optimizer + "'"));
		auto disabled_plan = OptimizeExportQuery(connection, entry.query);
		REQUIRE(count_function(*disabled_plan, entry.introduced_function) == 0);
		auto baseline = connection.Query(entry.query);
		REQUIRE_NO_FAIL(*baseline);
		REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers=''"));
		auto optimized = connection.Query(entry.query);
		REQUIRE_NO_FAIL(*optimized);
		REQUIRE(optimized->Equals(*baseline));
		auto expressions = collect(*plan);
		idx_t unqualified_introduced_count = 0;
		for (auto &reference : expressions) {
			auto &expression = reference.get();
			auto &definition = expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION
			                       ? static_cast<const Function &>(
			                             *expression.Cast<BoundFunctionExpression>().Function().GetDefinition())
			                       : static_cast<const Function &>(
			                             *expression.Cast<BoundAggregateExpression>().Function().GetDefinition());
			if (definition.GetCatalogName().empty() || definition.GetSchemaName().empty()) {
				INFO("unqualified function: " << definition.GetName());
				LogicalPlanVerificationPath path;
				path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
				auto exported = BoundExpressionSQLExporter::Export(expression, {});
				REQUIRE(exported.IsValid());
				REQUIRE(exported.HasError());
				REQUIRE_FALSE(exported.IsSuccess());
				REQUIRE(exported.GetIssues().size() == 1);
				REQUIRE(exported.GetIssues()[0].phase == LogicalPlanVerificationPhase::EXPRESSION_EXPORT);
				REQUIRE(exported.GetIssues()[0].path == optional<LogicalPlanVerificationPath>(path));
				if (definition.GetName() == entry.introduced_function) {
					RequireIssue(exported, LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION, path);
				}
				unqualified_introduced_count += definition.GetName() == entry.introduced_function;
				continue;
			}
			vector<SQLBindingEntry> bindings;
			CollectSQLBindings(expression, bindings);
			BoundExpressionSQLExportContext context;
			context.resolve_binding = [bindings](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
				for (auto &entry : bindings) {
					if (entry.binding == binding) {
						return ResolvedSQLColumnReference {{entry.name}, entry.type};
					}
				}
				return {};
			};
			string from_clause;
			for (auto &binding : bindings) {
				unique_ptr<ParsedExpression> value;
				if (binding.type.IsAggregateState()) {
					for (auto &candidate : expressions) {
						if (candidate.get().GetReturnType() != binding.type ||
						    candidate.get().GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
							continue;
						}
						vector<SQLBindingEntry> inputs;
						CollectSQLBindings(candidate, inputs);
						bool has_state_input = false;
						for (auto &input : inputs) {
							has_state_input |= input.type.IsAggregateState();
						}
						if (has_state_input) {
							continue;
						}
						auto fixture = candidate.get().Copy();
						ExpressionIterator::VisitExpressionClassMutable(
						    fixture, ExpressionClass::BOUND_COLUMN_REF,
						    [](unique_ptr<Expression> &column) { column = Constant(Value(column->GetReturnType())); });
						auto exported = BoundExpressionSQLExporter::Export(*fixture, {});
						REQUIRE(exported.IsSuccess());
						value = std::move(exported.GetValue());
						break;
					}
					REQUIRE(value);
				} else {
					value = make_uniq<CastExpression>(binding.type, ConstantExpression::Null());
				}
				from_clause += from_clause.empty() ? " FROM (SELECT " : ", ";
				from_clause += value->ToString() + " AS " + SQLIdentifier(binding.name);
			}
			if (!from_clause.empty()) {
				from_clause += ") AS inputs";
			}
			auto exported = BoundExpressionSQLExporter::Export(expression, context);
			REQUIRE(exported.IsSuccess());
			auto result = connection.Query("SELECT " + exported.GetValue()->ToString() + " AS exported" + from_clause);
			REQUIRE_NO_FAIL(*result);
			REQUIRE(result->GetTypes() == vector<LogicalType> {expression.GetReturnType()});
			auto restored = BinaryRoundTrip(*connection.context, expression);
			auto restored_export = BoundExpressionSQLExporter::Export(*restored, context);
			REQUIRE(restored_export.IsSuccess());
			auto restored_result =
			    connection.Query("SELECT " + restored_export.GetValue()->ToString() + " AS exported" + from_clause);
			REQUIRE_NO_FAIL(*restored_result);
			REQUIRE(restored_result->Equals(*result));
		}
		REQUIRE(unqualified_introduced_count == 0);
	}
	connection.Rollback();
}
