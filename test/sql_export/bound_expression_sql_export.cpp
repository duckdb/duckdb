#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/main/extension/linked_extension_registry.hpp"

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
#include "duckdb/optimizer/expression_rewriter.hpp"
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
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
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
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include <cmath>
#include <cstring>

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

static const Value &GetIssueFact(const LogicalPlanVerificationIssue &issue, const string &name) {
	for (auto &fact : issue.facts) {
		if (fact.first == name) {
			return fact.second;
		}
	}
	throw InternalException("Missing logical plan SQL export issue fact");
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

static unique_ptr<ParsedExpression> PositionalSQLUnbind(FunctionUnbindInput &input) {
	return make_uniq<FunctionExpression>(input.expression.Function().GetQualifiedName(), std::move(input.children));
}

static unique_ptr<ParsedExpression> DeclineSQLUnbind(FunctionUnbindInput &) {
	return nullptr;
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

} // namespace

TEST_CASE("Bound constant SQL export discards display metadata", "[sql_export][bound_expression_sql_export]") {
	BoundExpressionSQLExportContext context;
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

TEST_CASE("Bound expression SQL export resolves columns only by binding", "[sql_export][bound_expression_sql_export]") {
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
}

TEST_CASE("Bound expression SQL export composes deterministic expression paths",
          "[sql_export][bound_expression_sql_export]") {
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
}

TEST_CASE("Bound expression SQL export handles default casts and exclusive BETWEEN",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;

	auto default_cast = BoundCastExpression::AddDefaultCastToType(Constant(Value::INTEGER(42)), LogicalType::BIGINT);
	auto default_result = BoundExpressionSQLExporter::Export(*default_cast, context);
	REQUIRE(default_result.HasError());
	REQUIRE(default_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(*default_result.GetIssues()[0].construct->identifier == "default_cast_binding");
	BoundExpressionSQLExportContext default_context;
	default_context.client_context = connection.context.get();
	auto supported_default = BoundExpressionSQLExporter::Export(*default_cast, default_context);
	REQUIRE(supported_default.IsSuccess());
	REQUIRE(supported_default.GetValue()->Cast<CastExpression>().GetTargetType()->Equals(
	    *TypeExpression::FromLogicalType(LogicalType::BIGINT)));
	RequireRoundTrip(connection, *default_cast, default_context, string(), "CAST(42 AS BIGINT)");
	auto serialized_default = BinaryRoundTrip(*connection.context, *default_cast);
	REQUIRE(BoundCastExpression::IsDefaultCast(serialized_default->Cast<BoundFunctionExpression>()));
	RequireRoundTrip(connection, *serialized_default, default_context, string(), "CAST(42 AS BIGINT)");
	auto try_default =
	    BoundCastExpression::AddDefaultCastToType(Constant(Value("not an integer")), LogicalType::INTEGER, true);
	RequireRoundTrip(connection, *try_default, default_context, string(), "TRY_CAST('not an integer' AS INTEGER)");

	auto exclusive = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(1)),
	                                                Constant(Value::INTEGER(3)), false, true);
	auto exclusive_result = BoundExpressionSQLExporter::Export(*exclusive, context);
	REQUIRE(exclusive_result.IsSuccess());
	REQUIRE(exclusive_result.GetValue()->GetExpressionClass() == ExpressionClass::CONJUNCTION);
	auto &exclusive_conjunction = exclusive_result.GetValue()->Cast<ConjunctionExpression>();
	REQUIRE(exclusive_conjunction.GetChildren().size() == 2);
	REQUIRE(exclusive_conjunction.GetChildren()[0]->GetExpressionType() == ExpressionType::COMPARE_GREATERTHAN);
	REQUIRE(exclusive_conjunction.GetChildren()[1]->GetExpressionType() == ExpressionType::COMPARE_LESSTHANOREQUALTO);

	connection.Rollback();
}

TEST_CASE("Incremental scalar registration preserves live SQL identity",
          "[sql_export][bound_expression_sql_export][extension]") {
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

TEST_CASE("SQL export type admission follows DuckDB value types", "[sql_export][bound_expression_sql_export]") {
	auto types = LogicalType::AllTypes();
	for (idx_t value = 0; value <= NumericLimits<uint8_t>::Maximum(); value++) {
		auto id = static_cast<LogicalTypeId>(value);
		bool admitted = id == LogicalTypeId::SQLNULL || id == LogicalTypeId::TYPE;
		for (auto &type : types) {
			admitted |= type.id() == id;
		}
		REQUIRE(SQLExportHelpers::IsSQLExportType(id) == admitted);
	}
	types.push_back(LogicalType::SQLNULL);
	types.push_back(LogicalType::TYPE());
	types.push_back(LogicalType::POINTER);
	types.push_back(LogicalType::ANY);
	types.push_back(LogicalType::INVALID);
	auto binding = ColumnBinding(TableIndex(0), ProjectionIndex(0));
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	for (auto &type : types) {
		const bool admitted = SQLExportHelpers::IsSQLExportType(type.id()) && type.IsComplete();
		for (auto &candidate : vector<LogicalType> {type, LogicalType::LIST(type)}) {
			REQUIRE(SQLExportHelpers::IsSQLRepresentableType(candidate) == admitted);
			REQUIRE(SQLExportHelpers::IsSQLValueType(candidate) == admitted);
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
	auto tuple = LogicalType::TUPLE(vector<LogicalType> {LogicalType::INTEGER});
	REQUIRE(SQLExportHelpers::IsSQLRepresentableType(tuple));
	REQUIRE_FALSE(SQLExportHelpers::IsSQLRepresentableType(LogicalType::TUPLE(vector<LogicalType> {})));
	REQUIRE(SQLExportHelpers::IsSQLValueType(tuple));
	REQUIRE(SQLExportHelpers::IsSQLValueType(LogicalType::LIST(tuple)));
	REQUIRE_FALSE(SQLExportHelpers::IsSQLValueType(LogicalType::TUPLE(vector<LogicalType> {LogicalType::POINTER})));
}

TEST_CASE("Bound expression SQL export supports represented catalog bind state",
          "[sql_export][bound_expression_sql_export]") {
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

	vector<pair<string, unique_ptr<Expression>>> represented;
	represented.emplace_back("ordinary_bound_scalar", bind_scalar("ordinary_bound_scalar"));
	represented.emplace_back("ordinary_bound_sum", bind_aggregate("ordinary_bound_sum"));
	for (auto &entry : represented) {
		CAPTURE(entry.first);
		auto &expression = *entry.second;
		const bool scalar = expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION;
		REQUIRE((scalar ? expression.Cast<BoundFunctionExpression>().BindInfo()
		                : expression.Cast<BoundAggregateExpression>().BindInfo()));
		auto result = BoundExpressionSQLExporter::Export(expression, context);
		REQUIRE(result.IsSuccess());
		REQUIRE(BoundExpressionSQLExporter::Export(*expression.Copy(), context).IsSuccess());
		REQUIRE(result.GetValue()->Cast<FunctionExpression>().GetQualifiedName() ==
		        QualifiedName(Identifier::SystemCatalog(), schema_name, Identifier(entry.first)));
		const string oracle = schema_name.GetIdentifierName() + "." + entry.first + (scalar ? "(7, 99)" : "(7)");
		auto serialized = BinaryRoundTrip(*connection.context, expression);
		RequireRoundTrip(connection, *serialized, context, string(), oracle);
		RequireRoundTrip(connection, expression, context, string(), oracle);
	}

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
	connection.Rollback();
}

TEST_CASE("Function deserialization restores enclosing context after callback exceptions",
          "[sql_export][bound_expression_sql_export][serialization]") {
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

TEST_CASE("Standalone function binding does not autoload catalog collisions",
          "[sql_export][bound_expression_sql_export][logical_plan_verification][dont_link]") {
	for (auto &linked : LinkedExtensionRegistry::Get()) {
		if (linked.name == "core_functions") {
			SUCCEED("core_functions is linked into this binary, nothing to autoload");
			return;
		}
	}
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

TEST_CASE("Bound expression SQL export canonicalizes native function definitions",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;

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
			if (has_catalog && has_schema) {
				continue;
			}
			auto definition = make_shared_ptr<ScalarFunction>(*abs_definition);
			definition->SetCatalogName(has_catalog ? Identifier::SystemCatalog() : Identifier());
			definition->SetSchemaName(has_schema ? Identifier::DefaultSchema() : Identifier());
			auto bound = bind_scalar_definition(std::move(definition));
			auto exported = BoundExpressionSQLExporter::Export(*bound, context);
			REQUIRE(exported.IsSuccess());
			auto &name = exported.GetValue()->Cast<FunctionExpression>().GetQualifiedName();
			REQUIRE(name.Name() == Identifier("abs"));
			REQUIRE(name.Catalog() == Identifier::SystemCatalog());
			REQUIRE(name.Schema() == Identifier::DefaultSchema());
			RequireRoundTrip(connection, *bound->Copy(), context, string(), "abs(-7::INTEGER)");
		}
	}

	ScalarFunctionSet scalar_set(Identifier("abs"));
	scalar_set.functions.push_back(abs_definition);
	scalar_set.ApplyToFunctions([](ScalarFunction &) {});
	REQUIRE(scalar_set.GetFunctionByOffset(0) != abs_definition);
	auto applied_scalar = bind_scalar_definition(scalar_set.GetFunctionByOffset(0));
	REQUIRE(applied_scalar->Cast<BoundFunctionExpression>().Function().GetDefinition() ==
	        scalar_set.GetFunctionByOffset(0));
	REQUIRE(ExpressionExecutor::EvaluateScalar(*connection.context, *applied_scalar) == Value::INTEGER(7));
	RequireRoundTrip(connection, *applied_scalar, context, string(), "abs(-7::INTEGER)");

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
			if (has_catalog && has_schema) {
				continue;
			}
			auto definition = make_shared_ptr<AggregateFunction>(*sum_definition);
			definition->SetCatalogName(has_catalog ? Identifier::SystemCatalog() : Identifier());
			definition->SetSchemaName(has_schema ? Identifier::DefaultSchema() : Identifier());
			auto bound = bind_aggregate_definition(std::move(definition));
			RequireRoundTrip(connection, *bound->Copy(), context, string(), "sum(7::INTEGER)");
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

	AggregateFunctionSet aggregate_set(Identifier("sum"));
	aggregate_set.functions.push_back(sum_definition);
	aggregate_set.ApplyToFunctions([](AggregateFunction &) {});
	REQUIRE(aggregate_set.GetFunctionByOffset(0) != sum_definition);
	auto applied_aggregate = bind_aggregate_definition(aggregate_set.GetFunctionByOffset(0));
	REQUIRE(applied_aggregate->Function().GetDefinition() == aggregate_set.GetFunctionByOffset(0));
	REQUIRE(EvaluateAggregate(*applied_aggregate) == Value::HUGEINT(hugeint_t(7)));
	RequireRoundTrip(connection, *applied_aggregate, context, string(), "sum(7::INTEGER)");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export reconstructs registered casts", "[sql_export][bound_expression_sql_export]") {
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

	context.client_context = registered_connection.context.get();
	auto default_direct = BoundCastExpression::AddDefaultCastToType(Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	auto default_direct_result = BoundExpressionSQLExporter::Export(*default_direct, context);
	REQUIRE(default_direct_result.HasError());
	REQUIRE(*default_direct_result.GetIssues()[0].construct->identifier == "default_cast_binding");

	DuckDB unrelated_db;
	Connection unrelated_connection(unrelated_db);
	ExtensionLoader unrelated_loader(*unrelated_db.instance, "synthetic_unrelated_cast_extension");
	unrelated_loader.RegisterCastFunction(LogicalType::INTEGER, LogicalType::BIGINT,
	                                      BoundCastInfo(IntegerToBigintPlusOne), 0);
	BoundExpressionSQLExportContext unrelated_context;
	unrelated_context.client_context = unrelated_connection.context.get();
	auto default_unrelated = BoundCastExpression::AddDefaultCastToType(Constant(Value("7")), LogicalType::INTEGER);
	auto default_unrelated_result = BoundExpressionSQLExporter::Export(*default_unrelated, unrelated_context);
	REQUIRE(default_unrelated_result.HasError());
	REQUIRE(*default_unrelated_result.GetIssues()[0].construct->identifier == "default_cast_binding");
}

TEST_CASE("Bound expression SQL export rejects TRY around volatile children",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto random_plan = BindExportQuery(connection, "SELECT random()");
	auto random = FindExpression(*random_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION && expression.IsVolatile() &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "random";
	});
	REQUIRE(random);
	BoundOperatorExpression volatile_try(ExpressionType::OPERATOR_TRY, LogicalType::DOUBLE);
	volatile_try.GetChildrenMutable().push_back(random->Copy());
	auto volatile_result = BoundExpressionSQLExporter::Export(volatile_try, context);
	RequireIssue(volatile_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
	REQUIRE(*volatile_result.GetIssues()[0].construct->identifier == "try_volatile_child");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export drops plan-local table filter hints",
          "[sql_export][bound_expression_sql_export]") {
	auto filter = CreateOptionalFilterExpression(
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHAN,
	                                      make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0),
	                                      make_uniq<BoundConstantExpression>(Value::INTEGER(10))),
	    LogicalType::INTEGER);
	BoundExpressionSQLExportContext standalone;
	REQUIRE(BoundExpressionSQLExporter::Export(*filter, standalone).HasError());
	BoundExpressionSQLExportContext plan_context;
	plan_context.discard_optimizer_metadata = true;
	auto exported = BoundExpressionSQLExporter::Export(*filter, plan_context);
	REQUIRE(exported.IsSuccess());
	REQUIRE(exported.GetValue()->GetExpressionClass() == ExpressionClass::CONSTANT);
	REQUIRE(exported.GetValue()->Equals(*ConstantExpression::Boolean(true)));
}

TEST_CASE("Bound expression SQL export rejects repeated volatile exclusive BETWEEN input",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();

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
          "[sql_export][bound_expression_sql_export]") {
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

TEST_CASE("Bound expression SQL export preserves structural operators through copies",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	vector<unique_ptr<Expression>> structural;
	structural.push_back(
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT));
	structural.push_back(BoundBetweenExpression::Create(Constant(Value::INTEGER(7)), Constant(Value::INTEGER(2)),
	                                                    Constant(Value::INTEGER(9)), true, true));
	for (auto &expression : structural) {
		REQUIRE(BoundExpressionSQLExporter::Export(*expression->Copy(), context).IsSuccess());
		auto restored = BinaryRoundTrip(*connection.context, *expression);
		REQUIRE(BoundExpressionSQLExporter::Export(*restored, context).IsSuccess());
	}

	connection.Rollback();
}

TEST_CASE("Live optimized decimal sum exports its logical result",
          "[sql_export][bound_expression_sql_export][optimizer]") {
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
	REQUIRE(cast.GetTargetType()->Equals(*TypeExpression::FromLogicalType(LogicalType::DECIMAL(38, 2))));
	REQUIRE(cast.Child().Cast<FunctionExpression>().FunctionName() == "sum");
	auto restored = BinaryRoundTrip(*connection.context, aggregate);
	auto &restored_aggregate = restored->Cast<BoundAggregateExpression>();
	REQUIRE(restored_aggregate.Function().GetName() == "sum_no_overflow");
	REQUIRE(restored_aggregate.Function().GetDefinition()->GetName() == "sum");
	REQUIRE(restored_aggregate.Function().GetLogicalArguments() == aggregate.Function().GetLogicalArguments());
	REQUIRE(restored_aggregate.Function().GetLogicalReturnType() == LogicalType::DECIMAL(38, 2));
	connection.Rollback();
}

TEST_CASE("SQL export recovers calls from supported binary compatibility targets",
          "[sql_export][bound_expression_sql_export][serialization]") {
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
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export serializes logical SUM identity independently of implementation",
          "[sql_export][bound_expression_sql_export][optimizer]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE aggregate_values(i INTEGER)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO aggregate_values VALUES (1), (2), (2), (NULL)"));
	REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='compressed_materialization'"));
	connection.BeginTransaction();

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
	REQUIRE(post_serialization.IsSuccess());
	REQUIRE(post_serialization.GetValue()->Cast<FunctionExpression>().FunctionName() == "sum");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export rejects deferred expression kinds",
          "[sql_export][bound_expression_sql_export]") {
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
}

TEST_CASE("Bound expression SQL export owns outputs and propagates resolver exceptions",
          "[sql_export][bound_expression_sql_export]") {
	auto success = []() {
		BoundConstantExpression expression(Value::SMALLINT(9));
		BoundExpressionSQLExportContext context;
		return BoundExpressionSQLExporter::Export(expression, context);
	}();
	REQUIRE(success.IsSuccess());
	REQUIRE(success.GetValue()->Cast<CastExpression>().GetTargetType()->Equals(
	    *TypeExpression::FromLogicalType(LogicalType::SMALLINT)));

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

TEST_CASE("SQL export distinguishes inherited and unrepresented collations",
          "[sql_export][bound_expression_sql_export]") {
	const auto collated = LogicalType::VARCHAR_COLLATION("nocase");
	const ColumnBinding binding(TableIndex(1), ProjectionIndex(0));
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	for (const auto &type :
	     {collated, LogicalType::LIST(collated), LogicalType::ARRAY(collated, 2),
	      LogicalType::LIST(LogicalType::LIST(collated)), LogicalType::STRUCT({{Identifier("s"), collated}}),
	      LogicalType::MAP(LogicalType::INTEGER, collated)}) {
		CAPTURE(type.ToString());
		auto plain = TypeVisitor::VisitReplace(type, [](const LogicalType &child) {
			return child.id() == LogicalTypeId::VARCHAR ? LogicalType::VARCHAR : child;
		});
		REQUIRE(type == plain);
		REQUIRE_FALSE(SQLExportHelpers::SQLTypesMatch(type, plain));
		REQUIRE_FALSE(SQLExportHelpers::SQLTypesMatch(plain, type));
		REQUIRE(SQLExportHelpers::SQLTypesMatch(type, type));
		BoundColumnRefExpression column(type, binding);
		auto matching = ResolveBinding(binding, {Identifier("v")}, type);
		auto exported = BoundExpressionSQLExporter::Export(column, matching);
		REQUIRE(exported.IsSuccess());
		REQUIRE(exported.GetValue()->GetExpressionClass() == ExpressionClass::COLUMN_REF);
		auto changed = ResolveBinding(binding, {Identifier("v")}, plain);
		auto replacement = BoundExpressionSQLExporter::Export(column, changed);
		if (type.id() == LogicalTypeId::VARCHAR) {
			REQUIRE(replacement.IsSuccess());
			REQUIRE(replacement.GetValue()->GetExpressionClass() == ExpressionClass::COLLATE);
		} else {
			RequireIssue(replacement, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
			REQUIRE(replacement.GetIssues()[0].construct ==
			        LogicalPlanVerificationConstructIdentity::ExportFeature("nested_result_collation"));
			auto &replacement_issue = replacement.GetIssues()[0];
			REQUIRE(GetIssueFact(replacement_issue, "logical_type") == Value(type.ToString()));
			REQUIRE(GetIssueFact(replacement_issue, "input_logical_type") == Value(plain.ToString()));
			REQUIRE(GetIssueFact(replacement_issue, "varchar_collations") ==
			        Value(SQLExportHelpers::TypeCollationSignature(type)));
			REQUIRE(GetIssueFact(replacement_issue, "input_varchar_collations") ==
			        Value(SQLExportHelpers::TypeCollationSignature(plain)));
		}
		BoundColumnRefExpression reset(plain, binding);
		auto reset_result = BoundExpressionSQLExporter::Export(reset, matching);
		RequireIssue(reset_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
		REQUIRE(reset_result.GetIssues()[0].construct ==
		        LogicalPlanVerificationConstructIdentity::ExportFeature(
		            type.id() == LogicalTypeId::VARCHAR ? "column_collation_reset" : "nested_result_collation"));
		if (type.id() != LogicalTypeId::VARCHAR) {
			auto &reset_issue = reset_result.GetIssues()[0];
			REQUIRE(GetIssueFact(reset_issue, "logical_type") == Value(plain.ToString()));
			REQUIRE(GetIssueFact(reset_issue, "input_logical_type") == Value(type.ToString()));
			REQUIRE(GetIssueFact(reset_issue, "varchar_collations") ==
			        Value(SQLExportHelpers::TypeCollationSignature(plain)));
			REQUIRE(GetIssueFact(reset_issue, "input_varchar_collations") ==
			        Value(SQLExportHelpers::TypeCollationSignature(type)));
		}
	}
}

TEST_CASE("Nested SQL cast reconstruction distinguishes collation and error semantics",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	context.client_context = connection.context.get();
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	const auto target = LogicalType::LIST(LogicalType::VARCHAR_COLLATION("nocase"));
	GetCastFunctionInput input(*connection.context);
	for (bool same_type : {false, true}) {
		for (bool try_cast : {false, true}) {
			CAPTURE(same_type, try_cast);
			const auto source =
			    same_type ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::LIST(LogicalType::INTEGER);
			auto cast_info = CastFunctionSet::Get(*connection.context).GetCastFunction(source, target, input);
			auto expression = BoundCastExpression::Create(make_uniq<BoundConstantExpression>(Value(source)), target,
			                                              std::move(cast_info), try_cast);
			vector<unique_ptr<Expression>> variants;
			variants.push_back(expression->Copy());
			variants.push_back(BinaryRoundTrip(*connection.context, *expression));
			variants.push_back(std::move(expression));
			for (auto &variant : variants) {
				auto exported = BoundExpressionSQLExporter::Export(*variant, context);
				if (same_type || try_cast) {
					RequireIssue(exported, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
					REQUIRE(exported.GetIssues()[0].construct ==
					        LogicalPlanVerificationConstructIdentity::ExportFeature("nested_result_collation"));
				} else {
					REQUIRE(exported.IsSuccess());
					auto rebound = connection.Query("SELECT " + exported.GetValue()->ToString());
					REQUIRE_NO_FAIL(*rebound);
					REQUIRE(SQLExportHelpers::SQLTypesMatch(rebound->GetTypes()[0], target));
					REQUIRE(rebound->GetValue(0, 0).IsNull());
				}
			}
		}
	}
	connection.Rollback();
}

TEST_CASE("Aggregate SQL clauses retain logical result annotations",
          "[sql_export][bound_expression_sql_export][aggregate_call_sql_export]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(
	    connection.Query("CREATE TABLE clause_values(v VARCHAR); INSERT INTO clause_values VALUES ('A'),('a')"));
	connection.BeginTransaction();
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	auto plan = BindExportQuery(connection, "SELECT last(v) FROM clause_values");
	auto expression = FindExpression(*plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE;
	});
	REQUIRE(expression);
	for (bool binary : {false, true}) {
		auto copy = binary ? BinaryRoundTrip(*connection.context, *expression) : expression->Copy();
		auto &aggregate = copy->Cast<BoundAggregateExpression>();
		auto &column = aggregate.GetChildren()[0]->Cast<BoundColumnRefExpression>();
		auto context = ResolveBinding(column.Binding(), {Identifier("v")}, column.GetReturnType());
		auto call = BoundExpressionSQLExporter::ExportAggregateCallAtPath(aggregate, context, path);
		REQUIRE(call.IsSuccess());
		REQUIRE(call.GetValue()->GetExpressionClass() == ExpressionClass::FUNCTION);
		REQUIRE(call.GetValue()->GetQualifiedName() == QualifiedName("system", "main", "last"));
		auto direct = connection.Query("SELECT (" + call.GetValue()->ToString() + ")='A' FROM clause_values");
		REQUIRE_NO_FAIL(*direct);
		REQUIRE(direct->GetValue(0, 0) == Value::BOOLEAN(false));
		aggregate.SetReturnType(LogicalType::VARCHAR_COLLATION("nocase"));
		auto rejected = BoundExpressionSQLExporter::ExportAggregateCallAtPath(aggregate, context, path);
		REQUIRE(rejected.HasError());
		REQUIRE(rejected.GetIssues()[0].construct ==
		        LogicalPlanVerificationConstructIdentity::ExportFeature("aggregate_call_result_type"));
		auto ordinary = BoundExpressionSQLExporter::Export(aggregate, context);
		REQUIRE(ordinary.IsSuccess());
		auto annotated = connection.Query("SELECT (" + ordinary.GetValue()->ToString() + ")='A' FROM clause_values");
		REQUIRE_NO_FAIL(*annotated);
		REQUIRE(annotated->GetValue(0, 0) == Value::BOOLEAN(true));
	}
	const ColumnBinding binding(TableIndex(1), ProjectionIndex(0));
	for (const auto &type :
	     {LogicalType::VARCHAR_COLLATION("nocase"), LogicalType::LIST(LogicalType::VARCHAR_COLLATION("nocase")),
	      LogicalType::STRUCT({{Identifier("v"), LogicalType::VARCHAR_COLLATION("nocase")}})}) {
		auto &entry = Catalog::GetEntry<AggregateFunctionCatalogEntry>(*connection.context,
		                                                               QualifiedName("system", "main", "last"));
		auto definition = entry.functions.GetFunctionByArguments(*connection.context, {type});
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(make_uniq<BoundColumnRefExpression>(type, binding));
		FunctionBinder binder(*connection.context);
		auto aggregate = binder.BindAggregateFunction(definition, std::move(arguments));
		auto context = ResolveBinding(binding, {Identifier("v")}, type);
		for (idx_t generation = 0; generation < 3; generation++) {
			auto call = BoundExpressionSQLExporter::ExportAggregateCallAtPath(*aggregate, context, path);
			REQUIRE(call.IsSuccess());
			REQUIRE(call.GetValue()->GetExpressionClass() == ExpressionClass::FUNCTION);
			REQUIRE(SQLExportHelpers::SQLTypesMatch(aggregate->GetReturnType(), type));
			REQUIRE(SQLExportHelpers::SQLTypesMatch(aggregate->Function().GetLogicalReturnType(), type));
			aggregate =
			    unique_ptr_cast<Expression, BoundAggregateExpression>(BinaryRoundTrip(*connection.context, *aggregate));
		}
	}
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export owns named arguments after source destruction",
          "[sql_export][bound_expression_sql_export][struct_insert_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	const string query = "SELECT struct_insert(s, \"named column\" := j, \"constant field\" := 7, "
	                     "\"list field\" := l, \"nested field\" := n) "
	                     "FROM (VALUES ({'a': 1, 'MiXeD': [2]}, 2, [2, NULL]::INTEGER[], "
	                     "{'items': [2, NULL]::INTEGER[]}), ({'a': 1, 'MiXeD': [2]}, 2, [2, NULL]::INTEGER[], "
	                     "{'items': [2, NULL]::INTEGER[]}), ({'a': NULL, 'MiXeD': NULL::INTEGER[]}, NULL, "
	                     "NULL::INTEGER[], {'items': NULL::INTEGER[]})) src(s, j, l, n)";
	auto plan = BindExportQuery(connection, query);
	auto expression = FindExpression(*plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       candidate.Cast<BoundFunctionExpression>().Function().GetName() == "struct_insert";
	});
	REQUIRE(expression);
	vector<SQLBindingEntry> entries;
	CollectSQLBindings(*expression, entries);

	BoundExpressionSQLExportContext context;
	context.client_context = connection.context.get();
	context.resolve_binding = [entries](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		for (auto &entry : entries) {
			if (entry.binding == binding) {
				return ResolvedSQLColumnReference {{Identifier("v"), entry.name}, entry.type};
			}
		}
		return {};
	};
	const string from_clause = " FROM (VALUES ({'a': 1, 'MiXeD': [2]}, 2, [2, NULL]::INTEGER[], "
	                           "{'items': [2, NULL]::INTEGER[]}), ({'a': 1, 'MiXeD': [2]}, 2, [2, NULL]::INTEGER[], "
	                           "{'items': [2, NULL]::INTEGER[]}), ({'a': NULL, 'MiXeD': NULL::INTEGER[]}, NULL, "
	                           "NULL::INTEGER[], {'items': NULL::INTEGER[]})) "
	                           "v(exported_0, exported_1, exported_2, exported_3)";
	const string oracle = "SELECT struct_insert(v.exported_0, \"named column\" := v.exported_1, "
	                      "\"constant field\" := 7, "
	                      "\"list field\" := v.exported_2, \"nested field\" := v.exported_3)" +
	                      from_clause;
	auto expected = connection.Query(oracle);
	REQUIRE_NO_FAIL(*expected);
	auto exported = BoundExpressionSQLExporter::Export(*expression, context);
	REQUIRE(exported.IsSuccess());
	auto retained_ast = exported.GetValue()->Copy();
	exported.GetValue().reset();

	plan.reset();
	expression = nullptr;
	REQUIRE(retained_ast);
	auto retained = connection.Query("SELECT " + retained_ast->ToString() + from_clause);
	REQUIRE_NO_FAIL(*retained);
	REQUIRE(retained->GetTypes() == expected->GetTypes());
	REQUIRE(retained->Equals(*expected, false));
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export uses scalar unbind callbacks",
          "[sql_export][bound_expression_sql_export][struct_insert_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*connection.context);
	auto bind_definition = [&](const Identifier &name, const vector<LogicalType> &types,
	                           scalar_function_unbind_t callback) {
		auto &entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
		    *connection.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), name));
		auto definition =
		    make_shared_ptr<ScalarFunction>(*entry.functions.GetFunctionByArguments(*connection.context, types));
		definition->SetUnbindCallback(callback);
		vector<unique_ptr<Expression>> children;
		for (auto &type : types) {
			children.push_back(Constant(type == LogicalType::INTEGER ? Value::INTEGER(-7) : Value::DOUBLE(2)));
		}
		FunctionBinder binder(*connection.context);
		return binder.BindScalarFunction(std::move(definition), std::move(children));
	};

	auto positional = bind_definition(Identifier("abs"), {LogicalType::INTEGER}, PositionalSQLUnbind);
	auto positional_result = BoundExpressionSQLExporter::Export(*positional, {});
	REQUIRE(positional_result.IsSuccess());
	auto &positional_call = positional_result.GetValue()->Cast<FunctionExpression>();
	REQUIRE(positional_call.GetArguments().size() == 1);
	REQUIRE_FALSE(positional_call.GetArguments()[0].HasName());
	REQUIRE_NO_FAIL(connection.Query("SELECT " + positional_call.ToString()));

	auto missing = bind_definition(Identifier("abs"), {LogicalType::INTEGER}, DeclineSQLUnbind);
	auto missing_result = BoundExpressionSQLExporter::Export(*missing, {});
	REQUIRE(missing_result.HasError());
	REQUIRE(missing_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION);

	auto alias_plan = BindExportQuery(connection, "SELECT struct_pack(\"named field\" := 1)");
	auto struct_pack = FindExpression(*alias_plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       candidate.Cast<BoundFunctionExpression>().Function().GetName() == "struct_pack";
	});
	REQUIRE(struct_pack);
	auto missing_alias = struct_pack->Copy();
	missing_alias->Cast<BoundFunctionExpression>().GetChildren()[0]->ClearAlias();
	auto missing_alias_result = BoundExpressionSQLExporter::Export(*missing_alias, {});
	REQUIRE(missing_alias_result.HasError());
	REQUIRE(missing_alias_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION);
	REQUIRE(StringUtil::Contains(missing_alias_result.GetIssues()[0].message, "missing a SQL argument name"));
	connection.Rollback();
}

TEST_CASE("SQL export retains bound alias names across copies and renamed inputs",
          "[sql_export][bound_expression_sql_export][alias_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE names(\"MiXeD' name\" INTEGER)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO names VALUES (1),(NULL),(1)"));
	auto plan = BindExportQuery(connection, "SELECT alias(\"MiXeD' name\") FROM names");
	auto expression = FindExpression(*plan, [](const Expression &candidate) {
		return candidate.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       candidate.Cast<BoundFunctionExpression>().Function().GetName() == "alias";
	});
	REQUIRE(expression);
	auto renamed = expression->Copy();
	renamed->Cast<BoundFunctionExpression>().GetChildren()[0]->SetAlias(Identifier("discarded display name"));
	for (bool binary : {false, true}) {
		auto copy = binary ? BinaryRoundTrip(*connection.context, *renamed) : renamed->Copy();
		auto &function = copy->Cast<BoundFunctionExpression>();
		auto binding = function.GetChildren()[0]->Cast<BoundColumnRefExpression>().Binding();
		auto context = ResolveBinding(binding, {Identifier("renamed")}, LogicalType::INTEGER);
		const string from = " FROM (SELECT \"MiXeD' name\" AS renamed FROM names)";
		RequireRoundTrip(connection, function, context, from, "'MiXeD'' name'");
		function.SetAlias(Identifier("explicit"));
		RequireRoundTrip(connection, function, context, from, "'explicit'");
	}
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export preserves aggregate ordering through copies",
          "[sql_export][bound_expression_sql_export][serialization]") {
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
	connection.Rollback();
}
