#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/planner/planner.hpp"

#include <stdexcept>
#include <type_traits>

using namespace duckdb;

namespace {

using PlanExportResult = LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>;

static_assert(!std::is_copy_constructible<LogicalPlanSQLExportRelation>::value,
              "Logical plan SQL export relations must remain move-only");

static unique_ptr<LogicalOperator> OptimizeLogicalPlanExportQuery(Connection &connection, const string &query) {
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(query);
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	Optimizer optimizer(*planner.binder, *connection.context);
	return optimizer.Optimize(std::move(planner.plan));
}

static unique_ptr<LogicalOperator> OptimizeLogicalPlanExportQueryWithRepeatedPruning(Connection &connection,
                                                                                     const string &query) {
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(query);
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	Optimizer optimizer(*planner.binder, *connection.context);
	auto plan = optimizer.Optimize(std::move(planner.plan));
	RemoveUnusedColumns remove_unused_columns(optimizer);
	remove_unused_columns.VisitOperator(plan);
	plan->ResolveOperatorTypes();
	return plan;
}

static optional_ptr<LogicalOperator> FindLogicalPlanExportOperator(LogicalOperator &op, LogicalOperatorType type) {
	if (op.type == type) {
		return op;
	}
	for (auto &child : op.children) {
		if (!child) {
			continue;
		}
		auto result = FindLogicalPlanExportOperator(*child, type);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

static void RequireLogicalPlanExportIssue(const PlanExportResult &result, LogicalPlanVerificationIssueCode code,
                                          LogicalPlanVerificationPhase phase, const LogicalPlanVerificationPath &path) {
	REQUIRE(result.IsValid());
	REQUIRE(result.HasError());
	REQUIRE_FALSE(result.IsSuccess());
	REQUIRE(result.GetIssues().size() == 1);
	INFO(result.GetIssues()[0].message);
	REQUIRE(result.GetIssues()[0].code == code);
	REQUIRE(result.GetIssues()[0].phase == phase);
	REQUIRE(result.GetIssues()[0].path == optional<LogicalPlanVerificationPath>(path));
}

static void RequirePlanExportIssue(const PlanExportResult &result, LogicalPlanVerificationIssueCode code,
                                   const LogicalPlanVerificationPath &path = {}) {
	RequireLogicalPlanExportIssue(result, code, LogicalPlanVerificationPhase::PLAN_EXPORT, path);
}

static const Value &GetPlanIssueFact(const LogicalPlanVerificationIssue &issue, const string &name) {
	for (auto &fact : issue.facts) {
		if (fact.first == name) {
			return fact.second;
		}
	}
	throw InternalException("Missing logical plan SQL export issue fact");
}

static unique_ptr<Expression> PlanIntegerConstant(int32_t value) {
	return make_uniq<BoundConstantExpression>(Value::INTEGER(value));
}

static unique_ptr<LogicalExpressionGet>
IntegerValues(TableIndex table_index, std::initializer_list<std::initializer_list<int32_t>> input_rows) {
	vector<vector<unique_ptr<Expression>>> rows;
	idx_t column_count = 0;
	for (auto &input_row : input_rows) {
		vector<unique_ptr<Expression>> row;
		for (auto value : input_row) {
			row.push_back(PlanIntegerConstant(value));
		}
		column_count = row.size();
		rows.push_back(std::move(row));
	}
	vector<LogicalType> types(column_count, LogicalType::INTEGER);
	auto result = make_uniq<LogicalExpressionGet>(table_index, std::move(types), std::move(rows));
	result->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(table_index.index + 1000)));
	return result;
}

static unique_ptr<LogicalProjection> PlanProjection(TableIndex table_index, unique_ptr<LogicalOperator> child,
                                                    vector<unique_ptr<Expression>> expressions) {
	auto result = make_uniq<LogicalProjection>(table_index, std::move(expressions));
	result->children.push_back(std::move(child));
	return result;
}

static unique_ptr<ParsedExpression> PlanParsedInteger(int32_t value, const Identifier &alias) {
	auto result = ConstantExpression::FromValue(Value::INTEGER(value));
	result->SetAlias(alias);
	return result;
}

static unique_ptr<QueryNode> PlanConstantRelation(const LogicalPlanSQLExportExtensionInput &input, int32_t value) {
	auto select = make_uniq<SelectNode>();
	for (idx_t i = 0; i < input.op.types.size(); i++) {
		select->select_list.push_back(PlanParsedInteger(value, Identifier()));
	}
	return std::move(select);
}

class SQLExportExtensionOperator : public LogicalExtensionOperator {
public:
	SQLExportExtensionOperator(string name_p, vector<ColumnBinding> bindings_p, vector<LogicalType> types_p,
	                           vector<TableIndex> table_indexes_p = {},
	                           vector<unique_ptr<Expression>> expressions_p = {}, string verification_name_p = {})
	    : LogicalExtensionOperator(std::move(expressions_p)), name(std::move(name_p)),
	      verification_name(verification_name_p.empty() ? name : std::move(verification_name_p)),
	      bindings(std::move(bindings_p)), resolved_types(std::move(types_p)),
	      table_indexes(std::move(table_indexes_p)) {
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return bindings;
	}

	vector<TableIndex> GetTableIndex() const override {
		return table_indexes;
	}

	optional_ptr<const string> GetTypeBindingVerificationIdentifier() const noexcept override {
		return verification_name;
	}

	string GetExtensionName() const override {
		return name;
	}

	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &) override {
		throw NotImplementedException("Synthetic SQL export operator cannot create a physical plan");
	}

protected:
	void ResolveTypes() override {
		types = resolved_types;
	}

private:
	string name;
	string verification_name;
	vector<ColumnBinding> bindings;
	vector<LogicalType> resolved_types;
	vector<TableIndex> table_indexes;
};

class SQLExportOpaqueProjection : public SQLExportExtensionOperator {
public:
	explicit SQLExportOpaqueProjection(vector<unique_ptr<Expression>> physical_expressions_p)
	    : SQLExportExtensionOperator(
	          "sql_export_opaque_projection",
	          {ColumnBinding(TableIndex(2001), ProjectionIndex(0)), ColumnBinding(TableIndex(2001), ProjectionIndex(1)),
	           ColumnBinding(TableIndex(2001), ProjectionIndex(2))},
	          {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}, {TableIndex(2001)}),
	      physical_expressions(std::move(physical_expressions_p)) {
	}

	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		vector<unique_ptr<Expression>> projection;
		projection.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
		for (auto &expression : physical_expressions) {
			projection.push_back(expression->Copy());
		}
		auto &result = planner.Make<PhysicalProjection>(types, std::move(projection), estimated_cardinality);
		result.children.push_back(child);
		return result;
	}

private:
	vector<unique_ptr<Expression>> physical_expressions;
};

class SQLExportOperatorExtension : public OperatorExtension {
public:
	SQLExportOperatorExtension(string name_p, logical_plan_sql_export_t callback_p)
	    : name(std::move(name_p)), callback(std::move(callback_p)) {
		Bind = nullptr;
	}

	std::string GetName() override {
		return name;
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override {
		return nullptr;
	}

	LogicalPlanSQLExportExtensionResult ExportLogicalPlanSQL(const LogicalPlanSQLExportExtensionInput &input) override {
		return callback(input);
	}

private:
	string name;
	logical_plan_sql_export_t callback;
};

class LegacyOperatorExtension : public OperatorExtension {
public:
	LegacyOperatorExtension() {
		Bind = nullptr;
	}

	std::string GetName() override {
		return "legacy_sql_export_test";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override {
		return nullptr;
	}
};

class SyntheticLogicalOperator : public LogicalOperator {
public:
	explicit SyntheticLogicalOperator(LogicalOperatorType type_p) : LogicalOperator(type_p) {
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return {};
	}

protected:
	void ResolveTypes() override {
	}
};

static unique_ptr<SQLExportExtensionOperator> LeafExtension(const string &name, TableIndex table_index) {
	auto binding = ColumnBinding(table_index, ProjectionIndex(0));
	return make_uniq<SQLExportExtensionOperator>(name, vector<ColumnBinding> {binding},
	                                             vector<LogicalType> {LogicalType::INTEGER},
	                                             vector<TableIndex> {table_index});
}

static unique_ptr<SQLExportExtensionOperator> ChildExtension(const string &name, TableIndex child_index) {
	auto binding = ColumnBinding(child_index, ProjectionIndex(0));
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(Identifier("display_name"), LogicalType::INTEGER, binding));
	auto result = make_uniq<SQLExportExtensionOperator>(name, vector<ColumnBinding> {binding},
	                                                    vector<LogicalType> {LogicalType::INTEGER},
	                                                    vector<TableIndex> {}, std::move(expressions));
	result->children.push_back(IntegerValues(child_index, {{42}}));
	return result;
}

static unique_ptr<QueryNode> PlanChildRelation(const LogicalPlanSQLExportExtensionInput &input) {
	if (input.children.size() != 1 || input.expression_count != 1) {
		throw InternalException("Unexpected synthetic extension export input");
	}
	auto expression = input.export_expression(0);
	if (expression.HasError()) {
		throw InternalException("Synthetic extension expression export failed");
	}
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(std::move(expression.GetValue()));
	select->from_table = std::move(input.children[0].table);
	return std::move(select);
}

static LogicalPlanSQLExportOptions PlanResolverOptions(logical_plan_sql_export_t callback) {
	LogicalPlanSQLExportOptions options;
	options.extension_resolver = std::move(callback);
	return options;
}

} // namespace

TEST_CASE("Logical plan SQL export identifies rejected output types", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);

	SECTION("unrepresentable output") {
		LogicalType type = LogicalType::POINTER;
		auto plan = make_uniq<SQLExportExtensionOperator>(
		    "unrepresentable_output", vector<ColumnBinding> {ColumnBinding(TableIndex(1), ProjectionIndex(0))},
		    vector<LogicalType> {type});
		plan->ResolveOperatorTypes();
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.construct == LogicalPlanVerificationConstructIdentity::ExportFeature("output_type"));
		REQUIRE(GetPlanIssueFact(issue, "column_index") == Value::UBIGINT(0));
		REQUIRE(GetPlanIssueFact(issue, "logical_type") == Value(type.ToString()));
		REQUIRE(GetPlanIssueFact(issue, "varchar_collations") == Value(SQLExportHelpers::TypeCollationSignature(type)));
	}

	SECTION("aggregate state without reconstruction parameters") {
		auto type = LogicalType::STRUCT({{Identifier("value"), LogicalType::INTEGER}}).WithAlias("AGGREGATE_STATE");
		LogicalEmptyResult plan(vector<LogicalType> {type},
		                        vector<ColumnBinding> {ColumnBinding(TableIndex(2), ProjectionIndex(0))});
		plan.ResolveOperatorTypes();
		auto result = LogicalPlanSQLExporter::Export(*connection.context, plan);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.construct ==
		        LogicalPlanVerificationConstructIdentity::ExportFeature("aggregate_state_parameters"));
	}
}

TEST_CASE("Logical plan SQL export preserves qualified identities through binary copies",
          "[sql_export][logical_plan_sql_export][serialization]") {
	unique_ptr<QueryNode> owned_query;
	{
		DuckDB db;
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=false"));
		connection.BeginTransaction();
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT abs(i) FROM (VALUES (-7), (2)) t(i)");
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_PROJECTION);
		REQUIRE(plan->expressions[0]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION);
		auto expression_copy = plan->expressions[0]->Copy();
		plan->expressions[0] = std::move(expression_copy);
		REQUIRE_NO_FAIL(connection.Query("SET debug_verify_serializer=true"));
		auto live = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		REQUIRE(live.IsSuccess());
		owned_query = live.GetValue().query->Copy();
		auto restored = plan->Copy(*connection.context);
		auto copied = LogicalPlanSQLExporter::Export(*connection.context, *restored);
		REQUIRE(copied.IsSuccess());
		REQUIRE_NO_FAIL(connection.Query(copied.GetValue().query->ToString()));
		Planner::VerifyPlan(*connection.context, plan);
		REQUIRE(LogicalPlanSQLExporter::Export(*connection.context, *plan).IsSuccess());
		connection.Rollback();
	}
	DuckDB receiving_db;
	Connection receiving_connection(receiving_db);
	auto result = receiving_connection.Query(owned_query->ToString());
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetTypes() == vector<LogicalType> {LogicalType::INTEGER});
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(7));
	REQUIRE(result->GetValue(0, 1) == Value::INTEGER(2));
}

TEST_CASE("Logical plan field types follow expression SQL type admission", "[sql_export][logical_plan_sql_export]") {
	DuckDB db;
	Connection connection(db);
	auto types = LogicalType::AllTypes();
	types.push_back(LogicalType::SQLNULL);
	types.push_back(LogicalType::POINTER);
	types.push_back(LogicalType::ANY);
	const ColumnBinding binding(TableIndex(74), ProjectionIndex(0));
	for (auto &type : types) {
		for (auto &candidate : vector<LogicalType> {type, LogicalType::LIST(type)}) {
			INFO(static_cast<uint32_t>(type.id()));
			BoundColumnRefExpression expression(candidate, binding);
			BoundExpressionSQLExportContext context;
			context.resolve_binding = [&](const ColumnBinding &) -> optional<ResolvedSQLColumnReference> {
				return ResolvedSQLColumnReference {{Identifier("c0")}, candidate};
			};
			auto expression_result = BoundExpressionSQLExporter::Export(expression, context);
			SQLExportExtensionOperator op("sql_type_admission", {binding}, {candidate}, {TableIndex(74)});
			LogicalPlanSQLExportOptions options;
			options.extension_resolver = [](const LogicalPlanSQLExportExtensionInput &input) {
				auto query = make_uniq<SelectNode>();
				for (auto &type : input.op.types) {
					query->select_list.push_back(ConstantExpression::Null());
				}
				return LogicalPlanSQLExportExtensionResult::Exported(std::move(query));
			};
			auto result = LogicalPlanSQLExporter::Export(*connection.context, op, options);
			REQUIRE(result.IsValid());
			REQUIRE(result.IsSuccess() == expression_result.IsSuccess());
		}
	}
}

TEST_CASE("Logical plan SQL export resolves bindings independently of display aliases",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto child = IntegerValues(TableIndex(20), {{10, 20}});
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundColumnRefExpression>(Identifier("same"), LogicalType::INTEGER,
	                                                          ColumnBinding(TableIndex(20), ProjectionIndex(1))));
	expressions.push_back(make_uniq<BoundColumnRefExpression>(Identifier("same"), LogicalType::INTEGER,
	                                                          ColumnBinding(TableIndex(20), ProjectionIndex(0))));
	expressions.push_back(make_uniq<BoundColumnRefExpression>(Identifier("same"), LogicalType::INTEGER,
	                                                          ColumnBinding(TableIndex(20), ProjectionIndex(1))));
	auto plan = PlanProjection(TableIndex(21), std::move(child), std::move(expressions));

	auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(result.IsSuccess());
	auto query_result = connection.Query(result.GetValue().query->ToString());
	REQUIRE_FALSE(query_result->HasError());
	auto chunk = query_result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	REQUIRE(chunk->GetValue(0, 0) == Value::INTEGER(20));
	REQUIRE(chunk->GetValue(1, 0) == Value::INTEGER(10));
	REQUIRE(chunk->GetValue(2, 0) == Value::INTEGER(20));
}

TEST_CASE("Logical plan SQL export applies filter predicates and projection maps",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto child = IntegerValues(TableIndex(30), {{1, 10, 100}, {2, 20, 200}, {3, 30, 300}});
	auto filter = make_uniq<LogicalFilter>();
	filter->expressions.push_back(BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_GREATERTHAN,
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(30), ProjectionIndex(0))),
	    PlanIntegerConstant(1)));
	filter->expressions.push_back(BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_LESSTHAN,
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(30), ProjectionIndex(1))),
	    PlanIntegerConstant(30)));
	filter->projection_map = {ProjectionIndex(2), ProjectionIndex(1)};
	filter->children.push_back(std::move(child));

	auto result = LogicalPlanSQLExporter::Export(*connection.context, *filter);
	REQUIRE(result.IsSuccess());
	REQUIRE(result.GetValue().fields.size() == 2);
	REQUIRE(result.GetValue().fields[0].source_binding == ColumnBinding(TableIndex(30), ProjectionIndex(2)));
	REQUIRE(result.GetValue().fields[1].source_binding == ColumnBinding(TableIndex(30), ProjectionIndex(1)));
	auto query_result = connection.Query(result.GetValue().query->ToString());
	REQUIRE_FALSE(query_result->HasError());
	auto chunk = query_result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	REQUIRE(chunk->GetValue(0, 0) == Value::INTEGER(200));
	REQUIRE(chunk->GetValue(1, 0) == Value::INTEGER(20));
}

TEST_CASE("Logical plan SQL export accepts an explicit empty grouping set", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();

	SECTION("one empty grouping set") {
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT sum(i) FROM (VALUES (1), (2)) t(i)");
		auto aggregate = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
		REQUIRE(aggregate);
		auto &logical_aggregate = aggregate->Cast<LogicalAggregate>();
		logical_aggregate.grouping_sets.push_back(GroupingSet());
		auto result = LogicalPlanSQLExporter::Export(*connection.context, logical_aggregate);
		REQUIRE(result.IsSuccess());
		REQUIRE(connection.Query(result.GetValue().query->ToString())->GetValue(0, 0) == Value::HUGEINT(3));
	}
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export applies requested output names", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT 1 AS a, 2 AS b");
	LogicalPlanSQLExportOptions options;
	options.output_names = vector<Identifier> {"same", "same"};
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
	REQUIRE(exported.IsSuccess());
	auto result = connection.Query(exported.GetValue().query->ToString());
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetNames() == vector<Identifier> {"same", "same"});
	REQUIRE(result->GetTypes() == vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER});
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));

	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE explain_names(i INTEGER)"));
	auto prepared =
	    connection.Prepare("EXPLAIN (SQL) SELECT i AS \"same name\", i::BIGINT AS \"same name\" FROM explain_names");
	REQUIRE(!prepared->HasError());
	vector<Value> parameters;
	auto execute_explain = [&]() {
		return unique_ptr_cast<QueryResult, MaterializedQueryResult>(prepared->Execute(parameters, false));
	};
	auto first = execute_explain();
	REQUIRE_NO_FAIL(*first);
	REQUIRE(first->GetNames() == vector<Identifier> {"explain_key", "explain_value"});
	REQUIRE(first->GetTypes() == vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR});
	REQUIRE(first->RowCount() == 1);
	REQUIRE(first->GetValue(0, 0) == Value("sql"));
	auto sql = first->GetValue(1, 0).GetValue<string>();
	auto explained = connection.Query(sql);
	REQUIRE_NO_FAIL(*explained);
	REQUIRE(explained->GetNames() == vector<Identifier> {"same name", "same name"});
	REQUIRE(explained->GetTypes() == vector<LogicalType> {LogicalType::INTEGER, LogicalType::BIGINT});
	auto repeated = execute_explain();
	REQUIRE_NO_FAIL(*repeated);
	REQUIRE(repeated->GetValue(1, 0) == Value(sql));
	REQUIRE_NO_FAIL(connection.Query("DROP TABLE explain_names"));
	REQUIRE(prepared->Execute()->HasError());
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE explain_names(i VARCHAR)"));
	auto rebound = execute_explain();
	REQUIRE_NO_FAIL(*rebound);
	auto rebound_query = connection.Query(rebound->GetValue(1, 0).GetValue<string>());
	REQUIRE_NO_FAIL(*rebound_query);
	REQUIRE(rebound_query->GetTypes() == vector<LogicalType> {LogicalType::VARCHAR, LogicalType::BIGINT});
	Parser parser;
	parser.ParseQuery("EXPLAIN (SQL) SELECT 42");
	auto copied = parser.statements[0]->Copy();
	REQUIRE(copied->ToString() == "EXPLAIN (SQL) SELECT 42");
}

TEST_CASE("Logical plan SQL export closes unsupported shapes and operator enums",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();

	SECTION("opaque source") {
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT * FROM range(1)");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		get->Cast<LogicalGet>().function.to_sql = [](ClientContext &, const LogicalGet &, unique_ptr<TableRef>,
		                                             const Identifier &) -> TableFunctionToSQLResult {
			return {nullptr, "test_opaque"};
		};
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *get);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.construct->function->name == "range");
		REQUIRE(issue.facts.size() == 1);
		REQUIRE((issue.facts[0] == pair<string, Value> {"guard", Value("test_opaque")}));
	}
	SECTION("generic source callback guard") {
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT * FROM range(2)");
		auto &get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
		get.ordinality_idx = optional_idx(0);
		auto result = LogicalPlanSQLExporter::Export(*connection.context, get);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.construct->function->name == "range");
		REQUIRE(issue.facts.size() == 1);
		REQUIRE((issue.facts[0] == pair<string, Value> {"guard", Value("ordinality")}));
	}
	SECTION("unknown future enum") {
		SyntheticLogicalOperator plan(static_cast<LogicalOperatorType>(254));
		auto result = LogicalPlanSQLExporter::Export(*connection.context, plan);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_OPERATOR);
	}
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export returns verifier failures before invoking resolvers",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	idx_t calls = 0;
	auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
		calls++;
		return LogicalPlanSQLExportExtensionResult::Exported(PlanConstantRelation(input, 42));
	});
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(nullptr);
	auto binding = ColumnBinding(TableIndex(70), ProjectionIndex(0));
	auto plan = make_uniq<SQLExportExtensionOperator>("bypassed_extension", vector<ColumnBinding> {binding},
	                                                  vector<LogicalType> {LogicalType::INTEGER},
	                                                  vector<TableIndex> {TableIndex(70)}, std::move(expressions));

	auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
	RequireLogicalPlanExportIssue(
	    result, LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, LogicalPlanVerificationPhase::VERIFY,
	    LogicalPlanVerificationPath {LogicalPlanVerificationPathRoot::LOGICAL_PLAN,
	                                 {{LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION, 0}}});
	REQUIRE(calls == 0);
}

TEST_CASE("Logical plan SQL export supports local and registered extension callbacks",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);

	SECTION("local decline then registered child export") {
		vector<string> calls;
		optional_ptr<TableRef> child_table;
		auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
			child_table = input.children[0].table.get();
			calls.push_back("local");
			return LogicalPlanSQLExportExtensionResult::NotHandled();
		});
		OperatorExtension::Register(DBConfig::GetConfig(*db.instance),
		                            make_shared_ptr<SQLExportOperatorExtension>(
		                                "child_extension", [&](const LogicalPlanSQLExportExtensionInput &input) {
			                                calls.push_back("registered");
			                                REQUIRE(input.children[0].table.get() == child_table.get());
			                                return LogicalPlanSQLExportExtensionResult::Exported(
			                                    PlanChildRelation(input));
		                                }));
		auto plan = ChildExtension("child_extension", TableIndex(80));
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		REQUIRE(result.IsSuccess());
		REQUIRE(result.GetValue().query->Cast<SelectNode>().from_table.get() == child_table.get());
		REQUIRE(calls == vector<string> {"local", "registered"});
		REQUIRE(connection.Query(result.GetValue().query->ToString())->GetValue(0, 0) == Value::INTEGER(42));
	}
	SECTION("local terminal rejection") {
		idx_t calls = 0;
		OperatorExtension::Register(DBConfig::GetConfig(*db.instance),
		                            make_shared_ptr<SQLExportOperatorExtension>(
		                                "terminal_extension", [&](const LogicalPlanSQLExportExtensionInput &) {
			                                calls++;
			                                return LogicalPlanSQLExportExtensionResult::NotHandled();
		                                }));
		auto options = PlanResolverOptions([](const LogicalPlanSQLExportExtensionInput &) {
			return LogicalPlanSQLExportExtensionResult::Unsupported("Unavailable");
		});
		auto plan = LeafExtension("terminal_extension", TableIndex(81));
		RequirePlanExportIssue(LogicalPlanSQLExporter::Export(*connection.context, *plan, options),
		                       LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION);
		REQUIRE(calls == 0);
	}
	SECTION("no handler") {
		auto plan = LeafExtension("unhandled_extension", TableIndex(82));
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION);
	}

	SECTION("child failure bypasses resolver") {
		idx_t calls = 0;
		auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &) {
			calls++;
			return LogicalPlanSQLExportExtensionResult::NotHandled();
		});
		auto plan = ChildExtension("child_failure", TableIndex(83));
		plan->children[0]->Cast<LogicalExpressionGet>().expressions[0][0] =
		    make_uniq<BoundDefaultExpression>(LogicalType::INTEGER);
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		RequireLogicalPlanExportIssue(
		    result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION,
		    LogicalPlanVerificationPhase::EXPRESSION_EXPORT,
		    LogicalPlanVerificationPath {LogicalPlanVerificationPathRoot::LOGICAL_PLAN,
		                                 {{LogicalPlanVerificationPathComponentType::OPERATOR_CHILD, 0},
		                                  {LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION, 0}}});
		REQUIRE(calls == 0);
	}
}

TEST_CASE("Logical plan SQL export transfers ready positional children to extensions",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db;
	Connection connection(db);
	vector<ColumnBinding> bindings;
	vector<unique_ptr<Expression>> expressions;
	for (idx_t i = 0; i < 2; i++) {
		bindings.emplace_back(TableIndex(85), ProjectionIndex(i));
		expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(Identifier("same"), LogicalType::INTEGER, bindings.back()));
	}
	auto plan = make_uniq<SQLExportExtensionOperator>("positional_child", bindings,
	                                                  vector<LogicalType>(2, LogicalType::INTEGER),
	                                                  vector<TableIndex> {}, std::move(expressions));
	plan->children.push_back(IntegerValues(TableIndex(85), {{10, 20}, {10, 20}}));
	optional_ptr<TableRef> child_table;
	auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
		auto select = make_uniq<SelectNode>();
		child_table = input.children[0].table.get();
		select->from_table = std::move(input.children[0].table);
		// Expression bindings remain available after taking the child.
		for (idx_t ordinal : {idx_t(1), idx_t(0)}) {
			auto expression = input.export_expression(ordinal);
			REQUIRE(expression.IsSuccess());
			select->select_list.push_back(std::move(expression.GetValue()));
		}
		REQUIRE_FALSE(input.children[0].table);
		return LogicalPlanSQLExportExtensionResult::Exported(std::move(select));
	});
	auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
	REQUIRE(result.IsSuccess());
	REQUIRE(result.GetValue().query->Cast<SelectNode>().from_table.get() == child_table.get());
	plan.reset();
	auto rows = connection.Query(result.GetValue().query->ToString());
	REQUIRE_NO_FAIL(*rows);
	REQUIRE(rows->RowCount() == 2);
	for (idx_t row = 0; row < 2; row++) {
		REQUIRE(rows->GetValue(0, row) == Value::INTEGER(20));
		REQUIRE(rows->GetValue(1, row) == Value::INTEGER(10));
	}
}

TEST_CASE("Logical plan SQL export gives invocation resolvers precedence over registration",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	idx_t registered_calls = 0;
	auto registered = make_shared_ptr<SQLExportOperatorExtension>(
	    "precedence_extension", [&](const LogicalPlanSQLExportExtensionInput &input) {
		    registered_calls++;
		    return LogicalPlanSQLExportExtensionResult::Exported(PlanConstantRelation(input, 99));
	    });
	OperatorExtension::Register(DBConfig::GetConfig(*db.instance), registered);
	Connection connection(db);
	auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
		return LogicalPlanSQLExportExtensionResult::Exported(PlanConstantRelation(input, 42));
	});
	auto plan = LeafExtension("precedence_extension", TableIndex(90));

	auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
	REQUIRE(result.IsSuccess());
	REQUIRE(registered_calls == 0);
	REQUIRE(connection.Query(result.GetValue().query->ToString())->GetValue(0, 0) == Value::INTEGER(42));
}

TEST_CASE("Logical plan SQL export uses the matching registered extension callback",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	auto registered = make_shared_ptr<SQLExportOperatorExtension>(
	    "registered_extension", [&](const LogicalPlanSQLExportExtensionInput &input) {
		    return LogicalPlanSQLExportExtensionResult::Exported(PlanConstantRelation(input, 84));
	    });
	OperatorExtension::Register(DBConfig::GetConfig(*db.instance), registered);
	Connection connection(db);
	auto plan = LeafExtension("registered_extension", TableIndex(91));

	auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(result.IsSuccess());
	REQUIRE(connection.Query(result.GetValue().query->ToString())->GetValue(0, 0) == Value::INTEGER(84));

	OperatorExtension::Register(DBConfig::GetConfig(*db.instance), make_shared_ptr<LegacyOperatorExtension>());
	auto legacy_plan = LeafExtension("legacy_sql_export_test", TableIndex(92));
	auto legacy_result = LogicalPlanSQLExporter::Export(*connection.context, *legacy_plan);
	RequirePlanExportIssue(legacy_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION);
}

TEST_CASE("Logical plan SQL export rejects missing extension query or rejection reason",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);

	SECTION("EXPORTED without relation") {
		auto plan = LeafExtension("malformed_state", TableIndex(103));
		auto options = PlanResolverOptions([](const LogicalPlanSQLExportExtensionInput &) {
			LogicalPlanSQLExportExtensionResult result;
			result.type = LogicalPlanSQLExportExtensionResultType::EXPORTED;
			return result;
		});
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::MALFORMED_EXTENSION_RESULT);
	}

	SECTION("UNSUPPORTED without reason") {
		auto plan = LeafExtension("malformed_state", TableIndex(104));
		auto options = PlanResolverOptions([](const LogicalPlanSQLExportExtensionInput &) {
			return LogicalPlanSQLExportExtensionResult::Unsupported("");
		});
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::MALFORMED_EXTENSION_RESULT);
	}
}

TEST_CASE("Logical plan SQL export does not descend through unsupported parents",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	idx_t calls = 0;
	auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
		calls++;
		return LogicalPlanSQLExportExtensionResult::Exported(PlanConstantRelation(input, 1));
	});
	auto plan = make_uniq<SyntheticLogicalOperator>(static_cast<LogicalOperatorType>(254));
	plan->children.push_back(LeafExtension("left_extension", TableIndex(110)));
	plan->children.push_back(LeafExtension("right_extension", TableIndex(111)));

	auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
	RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_OPERATOR);
	REQUIRE(calls == 0);
}

TEST_CASE("Logical plan SQL export propagates callback exceptions unchanged", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);

	SECTION("runtime exception") {
		auto plan = LeafExtension("throwing_extension", TableIndex(120));
		auto options =
		    PlanResolverOptions([](const LogicalPlanSQLExportExtensionInput &) -> LogicalPlanSQLExportExtensionResult {
			    throw std::runtime_error("synthetic callback failure");
		    });
		REQUIRE_THROWS_AS(LogicalPlanSQLExporter::Export(*connection.context, *plan, options), std::runtime_error);
	}
}

TEST_CASE("Logical plan SQL export results outlive plans, contexts, and callbacks",
          "[sql_export][logical_plan_sql_export]") {
	optional<LogicalPlanSQLExportRelation> owned_relation;
	string exported_sql;
	{
		DuckDB source_db(nullptr);
		Connection source_connection(source_db);
		auto plan = ChildExtension("owned_extension", TableIndex(130));
		auto options = PlanResolverOptions([](const LogicalPlanSQLExportExtensionInput &input) {
			return LogicalPlanSQLExportExtensionResult::Exported(PlanChildRelation(input));
		});
		auto result = LogicalPlanSQLExporter::Export(*source_connection.context, *plan, options);
		REQUIRE(result.IsSuccess());
		owned_relation = std::move(result.GetValue());
		exported_sql = owned_relation->query->ToString();
	}

	REQUIRE(owned_relation);
	REQUIRE(owned_relation->query->ToString() == exported_sql);
	DuckDB target_db(nullptr);
	Connection target_connection(target_db);
	auto result = target_connection.Query(exported_sql);
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(42));
}

TEST_CASE("Logical plan SQL export isolates extension column names and scope modifiers",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db;
	Connection connection(db);
	for (const auto &sql :
	     {"SELECT 42, 43", "SELECT 42 AS same, 43 AS same", "SELECT 42 AS \"odd.name\", 43 AS \"a b\"",
	      "SELECT v.a, v.b FROM (VALUES (1,2),(1,2),(3,4)) v(a,b)",
	      "SELECT DISTINCT v.a, v.b FROM (VALUES (1,2),(1,2),(3,4)) v(a,b)",
	      "SELECT v.a, v.b FROM (VALUES (1,2),(3,4)) v(a,b) ORDER BY v.a DESC LIMIT 1",
	      "SELECT v.a, v.b FROM (VALUES (1,2),(3,4)) v(a,b) LIMIT 0",
	      "SELECT v.a, v.b FROM (VALUES (1,2),(3,4)) v(a,b) QUALIFY row_number() OVER ()=1",
	      "WITH v(a,b) AS (VALUES (11,12)) SELECT v.a,v.b FROM v"}) {
		INFO(sql);
		auto leaf = make_uniq<SQLExportExtensionOperator>(
		    "positional",
		    vector<ColumnBinding> {{TableIndex(800), ProjectionIndex(0)}, {TableIndex(800), ProjectionIndex(1)}},
		    vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER}, vector<TableIndex> {TableIndex(800)});
		vector<unique_ptr<Expression>> expressions;
		for (auto index : {1, 0, 1}) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    LogicalType::INTEGER, ColumnBinding(TableIndex(800), ProjectionIndex(index))));
		}
		auto plan = PlanProjection(TableIndex(801), std::move(leaf), std::move(expressions));
		auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &) {
			Parser parser;
			parser.ParseQuery(sql);
			return LogicalPlanSQLExportExtensionResult::Exported(
			    std::move(parser.statements[0]->Cast<SelectStatement>().node));
		});
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		REQUIRE(exported.IsSuccess());
		REQUIRE(exported.GetValue().fields.size() == 3);
		auto direct = connection.Query("SELECT q.b,q.a,q.b FROM (" + string(sql) + ") q(a,b) ORDER BY 1,2,3");
		auto result = connection.Query(exported.GetValue().query->ToString() + " ORDER BY 1,2,3");
		REQUIRE_FALSE(direct->HasError());
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->GetTypes() == direct->GetTypes());
		REQUIRE(result->Equals(*direct, false));
		if (string(sql).find("WITH") != string::npos) {
			REQUIRE(exported.GetValue().query->Cast<SelectNode>().from_table->type == TableReferenceType::SUBQUERY);
		}
	}
}

TEST_CASE("SQL export inlines plain join sources without alias capture", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	for (auto aliases : {pair<string, string> {"v", "w"}, {"v", "v"}, {"r1", "v"}, {"v", "r0"}, {"r1", "r0"}}) {
		for (bool filtered : {false, true}) {
			CAPTURE(aliases.first, aliases.second, filtered);
			auto leaf = [](const string &name, idx_t index) {
				return make_uniq<SQLExportExtensionOperator>(
				    name, vector<ColumnBinding> {{TableIndex(index), ProjectionIndex(0)}},
				    vector<LogicalType> {LogicalType::INTEGER}, vector<TableIndex> {TableIndex(index)});
			};
			auto plan = make_uniq<LogicalCrossProduct>(leaf("left", 800), leaf("right", 801));
			plan->ResolveOperatorTypes();
			auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
				auto is_left = input.op.GetExtensionName() == "left";
				auto alias = is_left ? aliases.first : aliases.second;
				auto values = is_left ? "(VALUES(1),(2))" : "(VALUES(10),(20))";
				auto sql = "SELECT " + alias + ".x FROM " + values + " " + alias + "(x)";
				if (is_left && filtered) {
					sql += " WHERE " + alias + ".x=1";
				}
				Parser parser;
				parser.ParseQuery(sql);
				return LogicalPlanSQLExportExtensionResult::Exported(
				    std::move(parser.statements[0]->Cast<SelectStatement>().node));
			});
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
			INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
			REQUIRE(exported.IsSuccess());
			if (aliases.first == "v" && aliases.second == "w" && !filtered) {
				auto &join = exported.GetValue().query->Cast<SelectNode>().from_table->Cast<JoinRef>();
				REQUIRE(join.left->alias == Identifier("v"));
				REQUIRE(join.right->alias == Identifier("w"));
			}
			auto sql = exported.GetValue().query->ToString() + " ORDER BY 1,2";
			CAPTURE(sql);
			auto generated = connection.Query(sql);
			REQUIRE_NO_FAIL(*generated);
			if (filtered) {
				REQUIRE(CHECK_COLUMN(generated, 0, {1, 1}));
				REQUIRE(CHECK_COLUMN(generated, 1, {10, 20}));
			} else {
				REQUIRE(CHECK_COLUMN(generated, 0, {1, 1, 2, 2}));
				REQUIRE(CHECK_COLUMN(generated, 1, {10, 20, 10, 20}));
			}
		}
	}
	connection.Rollback();
}

namespace {

static vector<string> SQLExportRows(MaterializedQueryResult &result, bool ordered) {
	vector<string> rows;
	for (idx_t row = 0; row < result.RowCount(); row++) {
		string text;
		for (idx_t col = 0; col < result.ColumnCount(); col++) {
			auto value = result.GetValue(col, row).ToSQLString();
			text += to_string(value.size()) + ":" + value;
		}
		rows.push_back(std::move(text));
	}
	if (!ordered) {
		std::sort(rows.begin(), rows.end());
	}
	return rows;
}

static void RequireMarkConditionRejection(const LogicalPlanVerificationResult<LogicalPlanSQLExportRelation> &result) {
	REQUIRE(result.HasError());
	REQUIRE(result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(result.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("mark_condition_semantics"));
}

TEST_CASE("Logical plan SQL export rejects aggregate state values without parameters",
          "[sql_export][logical_plan_sql_export]") {
	auto missing_parameters = BoundExpressionSQLExporter::Export(
	    BoundConstantExpression(Value(LogicalType(LogicalType::INTEGER).WithAlias("AGGREGATE_STATE"))), {});
	REQUIRE(missing_parameters.HasError());
	REQUIRE(missing_parameters.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
}

TEST_CASE("Logical plan SQL export rejects malformed ordinality partitions", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();

	auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT * FROM range(5) WITH ORDINALITY");
	auto window = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_WINDOW);
	REQUIRE(window);
	auto &expression = window->expressions[0]->Cast<BoundWindowExpression>();
	expression.PartitionsMutable().push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, window->children[0]->GetColumnBindings()[0]));
	auto unsupported = LogicalPlanSQLExporter::Export(*connection.context, *window);
	RequirePlanExportIssue(unsupported, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	connection.Rollback();
}

static void RequirePivotStreamingEffects(Connection &connection) {
	for (idx_t route = 0; route < 4; route++) {
		CAPTURE(route);
		auto sequence = "pivot_stream_" + to_string(route);
		REQUIRE_NO_FAIL(connection.Query("CREATE SEQUENCE " + sequence));
		auto sql = "SELECT g,a_s FROM (SELECT i g, CASE WHEN i%2=0 THEN 'a' ELSE 'b' END k, i v, "
		           "nextval('" +
		           sequence +
		           "') e FROM range(5000)t(i)) t PIVOT (sum(v) AS s, max(e) AS effect FOR k IN ('a','b','z'))";
		QueryParameters parameters;
		parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
		unique_ptr<QueryResult> result;
		if (route == 0) {
			result = connection.context->Query(sql, parameters);
		} else {
			auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
			if (route == 3) {
				plan = plan->Copy(*connection.context);
			}
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			if (exported.HasError()) {
				INFO(exported.GetIssues()[0].message);
				REQUIRE(exported.IsSuccess());
			}
			if (route == 2) {
				auto statement = make_uniq<SelectStatement>();
				statement->node = std::move(exported.GetValue().query);
				result = connection.context->Query(std::move(statement), parameters);
			} else {
				result = connection.context->Query(exported.GetValue().query->ToString(), parameters);
			}
		}
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		auto first = result->Fetch();
		REQUIRE(first);
		REQUIRE(first->size() > 0);
		result->Cast<StreamQueryResult>().Close();
		REQUIRE_FALSE(result->HasError());
		auto effect = connection.Query("SELECT currval('" + sequence + "')");
		REQUIRE_NO_FAIL(*effect);
		REQUIRE(effect->GetValue(0, 0) == Value::BIGINT(5000));
	}
}

static BoundAggregateExpression &PivotAggregate(LogicalOperator &plan, idx_t aggregate_idx = 0) {
	auto pivot = FindLogicalPlanExportOperator(plan, LogicalOperatorType::LOGICAL_PIVOT);
	REQUIRE(pivot);
	auto &aggregates = pivot->Cast<LogicalPivot>().bound_pivot.aggregates;
	REQUIRE(aggregate_idx < aggregates.size());
	REQUIRE(aggregates[aggregate_idx]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
	return aggregates[aggregate_idx]->Cast<BoundAggregateExpression>();
}

static void RequirePivotSQLExportFailure(Connection &connection, unique_ptr<LogicalOperator> plan,
                                         const string &message) {
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.HasError());
	REQUIRE(exported.GetIssues().size() == 1);
	INFO(exported.GetIssues()[0].message);
	REQUIRE(exported.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(StringUtil::Contains(exported.GetIssues()[0].message, message));
}

class PivotOpaqueBindData : public FunctionData {
public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<PivotOpaqueBindData>();
	}

	bool Equals(const FunctionData &) const override {
		return false;
	}
};

struct PivotNonstandardEmptySum {
	static bool IgnoreNull() {
		return true;
	}

	static void Initialize(int64_t &state) {
		state = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state += input.GetSize();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state += input.GetSize() * count;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target += source;
	}

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &result, AggregateFinalizeData &) {
		result = state + 99;
	}
};

struct PivotNonstandardEmptyIntegerSum {
	static bool IgnoreNull() {
		return true;
	}

	static void Initialize(int64_t &state) {
		state = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state += input;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state += input * count;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target += source;
	}

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &result, AggregateFinalizeData &) {
		result = hugeint_t(state + 99);
	}
};

} // namespace

TEST_CASE("Logical plan SQL export reconstructs list PIVOT barriers",
          "[sql_export][logical_plan_sql_export][pivot_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	REQUIRE_NO_FAIL(
	    connection.Query("SET pivot_filter_threshold=0; SET threads=1; SET max_streaming_buffer_size='1b'"));

	auto optimized_sum = OptimizeLogicalPlanExportQuery(
	    connection, "FROM (VALUES ('a',1),('b',2)) t(k,v) PIVOT (sum(v) FOR k IN ('a','b','z'))");
	auto &sum = PivotAggregate(*optimized_sum);
	REQUIRE(sum.Function().GetDefinition());
	REQUIRE(sum.Function().GetDefinition()->GetName() == "sum");
	REQUIRE(sum.Function().GetName() == "sum");
	auto &catalog = Catalog::GetSystemCatalog(*connection.context);
	auto &sum_no_overflow_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    *connection.context,
	    QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier("sum_no_overflow")));
	auto sum_no_overflow = sum_no_overflow_entry.functions.GetFunctionByArguments(*connection.context,
	                                                                              sum.Function().GetLogicalArguments());
	sum.FunctionMutable().ReplaceImplementation(*sum_no_overflow);
	REQUIRE(sum.Function().GetName() == "sum_no_overflow");
	REQUIRE(LogicalPlanSQLExporter::Export(*connection.context, *optimized_sum).IsSuccess());
	RequirePivotStreamingEffects(connection);
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export rebinds PIVOT implementation metadata",
          "[sql_export][logical_plan_sql_export][pivot_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	REQUIRE_NO_FAIL(connection.Query("SET pivot_filter_threshold=0"));
	const string sql = "FROM (VALUES ('a',1),('b',2)) t(k,v) PIVOT (sum(v) FOR k IN ('a','b','z'))";
	auto require_fresh = [&](unique_ptr<LogicalOperator> plan) {
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
		REQUIRE(exported.IsSuccess());
		auto fresh = connection.Query(sql);
		auto generated = connection.Query(exported.GetValue().query->ToString());
		REQUIRE_NO_FAIL(*fresh);
		REQUIRE_NO_FAIL(*generated);
		REQUIRE(generated->GetTypes() == fresh->GetTypes());
		REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*fresh, false));
	};

	{
		auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
		auto &aggregate = PivotAggregate(*plan);
		auto fake_definition = make_shared_ptr<AggregateFunction>(*aggregate.Function().GetDefinition());
		aggregate.FunctionMutable() = BoundAggregateFunction(std::move(fake_definition));
		require_fresh(std::move(plan));
	}
	{
		auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
		auto &aggregate = PivotAggregate(*plan);
		auto callbacks = aggregate.Function().GetCallbacks();
		callbacks.initialize = nullptr;
		aggregate.FunctionMutable().SetCallbacks(callbacks);
		require_fresh(std::move(plan));
	}
	{
		auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
		auto &aggregate = PivotAggregate(*plan);
		auto properties = aggregate.Function().GetProperties();
		properties.order_dependent = properties.order_dependent == AggregateOrderDependent::ORDER_DEPENDENT
		                                 ? AggregateOrderDependent::NOT_ORDER_DEPENDENT
		                                 : AggregateOrderDependent::ORDER_DEPENDENT;
		aggregate.FunctionMutable().SetProperties(properties);
		require_fresh(std::move(plan));
	}
	{
		auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
		PivotAggregate(*plan).BindInfoMutable() = make_uniq<PivotOpaqueBindData>();
		require_fresh(std::move(plan));
	}
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export validates noncanonical PIVOT defaults and list sources",
          "[sql_export][logical_plan_sql_export][pivot_sql_export]") {
	auto require_default = [](Connection &connection, unique_ptr<LogicalOperator> plan, const string &sql) {
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
		REQUIRE(exported.IsSuccess());
		auto original = connection.Query(sql);
		REQUIRE_NO_FAIL(*original);
		auto generated = connection.Query(exported.GetValue().query->ToString());
		REQUIRE_NO_FAIL(*generated);
		REQUIRE(generated->GetTypes() == original->GetTypes());
		REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*original, false));
		auto statement = make_uniq<SelectStatement>();
		statement->node = std::move(exported.GetValue().query);
		auto ast = connection.Query(std::move(statement));
		REQUIRE_NO_FAIL(*ast);
		REQUIRE(ast->GetTypes() == original->GetTypes());
		REQUIRE(SQLExportRows(*ast, false) == SQLExportRows(*original, false));
	};

	SECTION("replaced aggregate empty result") {
		DuckDB db(nullptr);
		Connection connection(db);
		ExtensionLoader loader(*db.instance, "sql_export_pivot_default");
		auto function = AggregateFunction::UnaryAggregate<int64_t, string_t, int64_t, PivotNonstandardEmptySum>(
		    LogicalType::VARCHAR, LogicalType::BIGINT);
		function.SetName(Identifier("sum"));
		CreateAggregateFunctionInfo info(std::move(function));
		info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
		loader.RegisterFunction(std::move(info));
		REQUIRE_NO_FAIL(connection.Query("SET pivot_filter_threshold=0"));
		connection.BeginTransaction();
		const string sql = "FROM (VALUES ('a','x')) t(k,v) PIVOT(sum(v) FOR k IN ('a','z'))";
		for (bool binary : {false, true}) {
			CAPTURE(binary);
			auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
			if (binary) {
				plan = plan->Copy(*connection.context);
				plan->ResolveOperatorTypes();
			}
			require_default(connection, std::move(plan), sql);
		}
		connection.Rollback();
	}

	SECTION("modified registered aggregate empty result") {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET pivot_filter_threshold=0"));
		connection.BeginTransaction();
		auto &entry = Catalog::GetEntry<AggregateFunctionCatalogEntry>(*connection.context,
		                                                               QualifiedName("system", "main", "sum"));
		idx_t modified_overloads = 0;
		entry.functions.ApplyToFunctions([&](AggregateFunction &function) {
			auto &signature = function.GetSignature();
			if (signature.GetParameterCount() != 1 || signature.GetParameter(0).GetType() != LogicalType::INTEGER) {
				return;
			}
			auto qualified_name = function.GetQualifiedName();
			function = AggregateFunction::UnaryAggregate<int64_t, int32_t, hugeint_t, PivotNonstandardEmptyIntegerSum>(
			    LogicalType::INTEGER, LogicalType::HUGEINT);
			function.SetQualifiedName(qualified_name);
			modified_overloads++;
		});
		REQUIRE(modified_overloads == 1);
		struct PivotDefaultCase {
			string sql;
			vector<Value> expected;
		};
		for (const auto &test :
		     {PivotDefaultCase {"FROM (VALUES ('a',1)) t(k,v) PIVOT(sum(v) FOR k IN ('a','z'))",
		                        {Value::HUGEINT(hugeint_t(100)), Value::HUGEINT(hugeint_t(99))}},
		      PivotDefaultCase {
		          "FROM (VALUES ('a_b','c',1)) t(k1,k2,v) "
		          "PIVOT(sum(v) FOR (k1,k2) IN (('a_b','c'),('a','b_c'),('z','z')))",
		          {Value::HUGEINT(hugeint_t(100)), Value::HUGEINT(hugeint_t(99)), Value::HUGEINT(hugeint_t(99))}},
		      PivotDefaultCase {"FROM (SELECT 'a' k, 1 v WHERE false) t PIVOT(sum(v) FOR k IN ('a','z'))",
		                        {Value::HUGEINT(hugeint_t(99)), Value::HUGEINT(hugeint_t(99))}},
		      PivotDefaultCase {"FROM (VALUES ('g','a',1)) t(g,k,v) PIVOT(sum(v) FOR k IN ('a','z'))",
		                        {Value("g"), Value::HUGEINT(hugeint_t(100)), Value::HUGEINT(hugeint_t(99))}}}) {
			CAPTURE(test.sql);
			auto direct = connection.Query(test.sql);
			REQUIRE_NO_FAIL(*direct);
			REQUIRE(direct->RowCount() == 1);
			REQUIRE(direct->ColumnCount() == test.expected.size());
			for (idx_t column_idx = 0; column_idx < test.expected.size(); column_idx++) {
				REQUIRE(Value::NotDistinctFrom(direct->GetValue(column_idx, 0), test.expected[column_idx]));
			}
			for (bool binary : {false, true}) {
				CAPTURE(binary);
				auto plan = OptimizeLogicalPlanExportQuery(connection, test.sql);
				if (binary) {
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
				}
				require_default(connection, std::move(plan), test.sql);
			}
		}
		connection.Rollback();
	}

	SECTION("volatile and fallible empty defaults cannot move into execution") {
		for (bool fallible : {false, true}) {
			CAPTURE(fallible);
			DuckDB db(nullptr);
			Connection connection(db);
			ExtensionLoader loader(*db.instance, "sql_export_pivot_volatile_default");
			auto function = AggregateFunction::UnaryAggregate<int64_t, string_t, int64_t, PivotNonstandardEmptySum>(
			    LogicalType::VARCHAR, LogicalType::BIGINT);
			function.SetName(Identifier("volatile_default"));
			if (fallible) {
				function.SetFallible();
			} else {
				function.SetVolatile();
			}
			loader.RegisterFunction(std::move(function));
			REQUIRE_NO_FAIL(connection.Query("SET pivot_filter_threshold=0"));
			connection.BeginTransaction();
			const string sql = "FROM (VALUES ('a','x')) t(k,v) PIVOT(volatile_default(v) FOR k IN ('a','z'))";
			for (bool binary : {false, true}) {
				auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
				if (binary) {
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
				}
				RequirePivotSQLExportFailure(connection, std::move(plan), "before query execution");
			}
			connection.Rollback();
		}
	}

	SECTION("supplied aligned lists") {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET pivot_filter_threshold=0"));
		connection.BeginTransaction();
		const string pivot_sql = "FROM (VALUES ('g','a',1)) t(g,k,v) PIVOT(sum(v) FOR k IN ('a','b'))";
		for (const auto &child_sql : {
		         "SELECT 'g' g,[10::HUGEINT] v,['a'] k UNION ALL SELECT 'g',[20::HUGEINT],['b']",
		         "SELECT 'g' g,[]::HUGEINT[] v,[]::VARCHAR[] k",
		         "SELECT 'g' g,[10::HUGEINT,NULL] v,['a','a'] k",
		         "SELECT 'g1' g,[10::HUGEINT] v,['a'] k UNION ALL SELECT 'g2',[20::HUGEINT],['b']",
		         "SELECT 'g' g,[10::HUGEINT,20::HUGEINT] v,['a','a'] k",
		     }) {
			for (bool binary : {false, true}) {
				CAPTURE(child_sql, binary);
				auto original = OptimizeLogicalPlanExportQuery(connection, pivot_sql);
				auto pivot = FindLogicalPlanExportOperator(*original, LogicalOperatorType::LOGICAL_PIVOT);
				REQUIRE(pivot);
				auto plan = pivot->Copy(*connection.context);
				plan->Cast<LogicalPivot>().pivot_index = TableIndex(100000);
				plan->children[0] = OptimizeLogicalPlanExportQuery(connection, child_sql);
				plan->ResolveOperatorTypes();
				if (binary) {
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
				}
				auto native_plan = plan->Copy(*connection.context);
				native_plan->ResolveOperatorTypes();
				auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
				INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
				REQUIRE(exported.IsSuccess());
				auto text = exported.GetValue().query->ToString();
				auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native_plan)));
				REQUIRE_NO_FAIL(*native);
				auto generated = connection.Query(text);
				REQUIRE_NO_FAIL(*generated);
				REQUIRE(generated->GetTypes() == native->GetTypes());
				REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*native, false));
				auto statement = make_uniq<SelectStatement>();
				statement->node = std::move(exported.GetValue().query);
				auto ast = connection.Query(std::move(statement));
				REQUIRE_NO_FAIL(*ast);
				REQUIRE(ast->GetTypes() == native->GetTypes());
				REQUIRE(SQLExportRows(*ast, false) == SQLExportRows(*native, false));
			}
		}
		connection.Rollback();
	}

	SECTION("unaligned supplied lists") {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET pivot_filter_threshold=0"));
		connection.BeginTransaction();
		auto original = OptimizeLogicalPlanExportQuery(
		    connection, "FROM (VALUES ('g','a',1)) t(g,k,v) PIVOT(sum(v) FOR k IN ('a','b'))");
		auto pivot = FindLogicalPlanExportOperator(*original, LogicalOperatorType::LOGICAL_PIVOT);
		REQUIRE(pivot);
		for (bool binary : {false, true}) {
			auto plan = pivot->Copy(*connection.context);
			plan->Cast<LogicalPivot>().pivot_index = TableIndex(100000);
			plan->children[0] = OptimizeLogicalPlanExportQuery(connection, "SELECT 'g', [10::HUGEINT], ['a','b']");
			plan->ResolveOperatorTypes();
			if (binary) {
				plan = plan->Copy(*connection.context);
			}
			RequirePivotSQLExportFailure(connection, std::move(plan), "lists are not aligned");
		}
		connection.Rollback();
	}

	SECTION("modified canonical list aggregate") {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET pivot_filter_threshold=0"));
		connection.BeginTransaction();
		const string sql = "FROM (VALUES ('g','a',1),('g','b',2)) t(g,k,v) PIVOT(sum(v) FOR k IN ('a','b'))";
		for (idx_t variant = 0; variant < 3; variant++) {
			CAPTURE(variant);
			auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
			auto pivot = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_PIVOT);
			REQUIRE(pivot);
			auto source_op = pivot->children[0].get();
			REQUIRE(source_op->type == LogicalOperatorType::LOGICAL_PROJECTION);
			source_op = source_op->children[0].get();
			REQUIRE(source_op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
			auto &source = source_op->Cast<LogicalAggregate>();
			REQUIRE_FALSE(source.expressions.empty());
			auto &list = source.expressions[0]->Cast<BoundAggregateExpression>();
			if (variant == 0) {
				list.GetAggregateTypeMutable() = AggregateType::DISTINCT;
			} else if (variant == 1) {
				list.GetFilterMutable() = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
			} else {
				source.grouping_sets.push_back(GroupingSet());
			}
			RequirePivotSQLExportFailure(connection, std::move(plan), "PIVOT");
		}
		connection.Rollback();
	}
}

TEST_CASE("Scalar LIMIT export rejects row-dependent bounds", "[sql_export][logical_plan_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT i FROM range(5) t(i) LIMIT 2");
	auto &limit = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_LIMIT)->Cast<LogicalLimit>();
	limit.limit_val = BoundLimitNode::ExpressionValue(
	    make_uniq<BoundColumnRefExpression>(limit.children[0]->types[0], limit.children[0]->GetColumnBindings()[0]));
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.HasError());
	REQUIRE(exported.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("limit_binding"));
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export declines VALUES with observable function state",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	for (bool dependent : {false, true}) {
		for (bool empty : {false, true}) {
			for (idx_t row_count : {1, 2}) {
				CAPTURE(dependent, empty, row_count);
				REQUIRE_NO_FAIL(connection.Query("SELECT setseed(0.25)"));
				connection.BeginTransaction();
				auto sql = "SELECT " + string(dependent ? "x," : "") + "random(),random() FROM (VALUES(1),(1),(2))t(x)";
				auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
				auto &projection = plan->Cast<LogicalProjection>();
				vector<vector<unique_ptr<Expression>>> rows;
				for (idx_t i = 0; i < row_count; i++) {
					vector<unique_ptr<Expression>> row;
					for (auto &expression : projection.expressions) {
						row.push_back(expression->Copy());
					}
					rows.push_back(std::move(row));
				}
				auto native = make_uniq<LogicalExpressionGet>(TableIndex(1000), projection.types, std::move(rows));
				auto child = std::move(projection.children[0]);
				if (empty) {
					child = make_uniq<LogicalEmptyResult>(std::move(child));
				}
				native->children.push_back(std::move(child));
				native->ResolveOperatorTypes();
				auto exported = LogicalPlanSQLExporter::Export(*connection.context, *native);
				RequirePlanExportIssue(exported, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
				auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native)));
				REQUIRE_NO_FAIL(*direct);
				REQUIRE(direct->RowCount() == (empty ? 0 : 3 * row_count));
				connection.Rollback();
			}
		}
	}
}

namespace {

class SQLExportValuesCounter : public ClientContextState {
public:
	int64_t value = 0;
};

void SQLExportValuesNext(DataChunk &args, ExpressionState &state, Vector &result) {
	auto counter = state.GetContext().registered_state->Get<SQLExportValuesCounter>("sql_export_values_counter");
	auto writer = FlatVector::Writer<int64_t>(result, args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		writer.WriteValue(++counter->value);
	}
}

void SQLExportValuesPeek(DataChunk &args, ExpressionState &state, Vector &result) {
	auto counter = state.GetContext().registered_state->Get<SQLExportValuesCounter>("sql_export_values_counter");
	auto writer = FlatVector::Writer<int64_t>(result, args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		writer.WriteValue(counter->value);
	}
}

static unique_ptr<LogicalOperator> OrderValues(unique_ptr<LogicalOperator> child) {
	vector<BoundOrderByNode> orders;
	auto bindings = child->GetColumnBindings();
	for (idx_t i = 0; i < bindings.size(); i++) {
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
		                    make_uniq<BoundColumnRefExpression>(child->types[i], bindings[i]));
	}
	auto order = make_uniq<LogicalOrder>(std::move(orders));
	order->children.push_back(std::move(child));
	order->ResolveOperatorTypes();
	return std::move(order);
}

} // namespace

static void
RequireValuesEvaluationRejection(const LogicalPlanVerificationResult<LogicalPlanSQLExportRelation> &result) {
	REQUIRE(result.HasError());
	REQUIRE(result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(result.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("values_expression_evaluation"));
}

TEST_CASE("Logical plan SQL export preserves evaluation between VALUES columns",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto counter = make_shared_ptr<SQLExportValuesCounter>();
	connection.context->registered_state->Insert("sql_export_values_counter", counter);
	ExtensionLoader loader(*db.instance, "sql_export_values_counter");
	ScalarFunction next("sql_export_values_next", {}, LogicalType::BIGINT, SQLExportValuesNext);
	next.SetStability(FunctionStability::VOLATILE);
	loader.RegisterFunction(std::move(next));
	ScalarFunction peek("sql_export_values_peek", {}, LogicalType::BIGINT, SQLExportValuesPeek);
	peek.SetStability(FunctionStability::VOLATILE);
	loader.RegisterFunction(std::move(peek));
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	for (bool dependent : {false, true}) {
		for (idx_t input_count : vector<idx_t> {0, 1, 3, STANDARD_VECTOR_SIZE + 1}) {
			for (idx_t row_count : {1, 2, 3}) {
				CAPTURE(dependent, input_count, row_count);
				connection.BeginTransaction();
				auto input = StringUtil::Repeat("(1),", input_count ? input_count - 1 : 0) + "(2)";
				Parser parser(connection.context->GetParserOptions());
				auto value = string(dependent ? "x" : "42");
				parser.ParseQuery("SELECT " + value +
				                  ", sql_export_values_next(), sql_export_values_peek(), "
				                  "NULL::DECIMAL(9,2), [" +
				                  value + ",NULL], {'k': 7} FROM (VALUES " + input + ")t(x)");
				Planner planner(*connection.context);
				planner.CreatePlan(std::move(parser.statements[0]));
				planner.Optimize();
				REQUIRE(planner.properties.IsReadOnly());
				auto &projection = planner.plan->Cast<LogicalProjection>();
				vector<vector<unique_ptr<Expression>>> rows;
				for (idx_t i = 0; i < row_count; i++) {
					vector<unique_ptr<Expression>> row;
					for (auto &expression : projection.expressions) {
						row.push_back(expression->Copy());
					}
					rows.push_back(std::move(row));
				}
				unique_ptr<LogicalOperator> native =
				    make_uniq<LogicalExpressionGet>(TableIndex(1000), projection.types, std::move(rows));
				auto child = std::move(projection.children[0]);
				if (!input_count) {
					child = make_uniq<LogicalEmptyResult>(std::move(child));
				}
				native->children.push_back(std::move(child));
				native->ResolveOperatorTypes();
				if (row_count > 1) {
					RequirePlanExportIssue(LogicalPlanSQLExporter::Export(*connection.context, *native),
					                       LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
					native = OrderValues(std::move(native));
				}
				auto counter_before_export = counter->value;
				auto exported = LogicalPlanSQLExporter::Export(*connection.context, *native);
				INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
				RequireValuesEvaluationRejection(exported);
				REQUIRE(counter->value == counter_before_export);
				counter->value = 0;
				auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native)));
				REQUIRE_NO_FAIL(*direct);
				REQUIRE(direct->RowCount() == input_count * row_count);
				connection.Rollback();
			}
		}
	}
}

namespace {

static unique_ptr<LogicalExpressionGet> ValuesFromProjection(Connection &connection, const string &sql,
                                                             idx_t row_count = 1) {
	auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
	auto &projection = plan->Cast<LogicalProjection>();
	vector<vector<unique_ptr<Expression>>> rows;
	for (idx_t i = 0; i < row_count; i++) {
		vector<unique_ptr<Expression>> row;
		for (auto &expression : projection.expressions) {
			row.push_back(expression->Copy());
		}
		rows.push_back(std::move(row));
	}
	auto native = make_uniq<LogicalExpressionGet>(TableIndex(1000), projection.types, std::move(rows));
	native->children.push_back(std::move(projection.children[0]));
	native->ResolveOperatorTypes();
	return native;
}

static void CheckValuesRoundTrip(Connection &connection, unique_ptr<LogicalOperator> native, bool expected_error,
                                 bool ordered = false, bool exportable = true) {
	native->ResolveOperatorTypes();
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *native);
	INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
	if (!exportable) {
		RequireValuesEvaluationRejection(exported);
		auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native)));
		REQUIRE(direct->HasError() == expected_error);
		connection.Rollback();
		return;
	}
	REQUIRE(exported.IsSuccess());
	auto sql = exported.GetValue().query->ToString();
	INFO(sql);
	auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native)));
	REQUIRE(direct->HasError() == expected_error);
	connection.Rollback();
	auto generated = connection.Query(sql);
	auto statement = make_uniq<SelectStatement>();
	statement->node = std::move(exported.GetValue().query);
	auto ast = connection.Query(std::move(statement));
	for (auto &result_ref : vector<reference<MaterializedQueryResult>> {*generated, *ast}) {
		auto &result = result_ref.get();
		REQUIRE(result.HasError() == expected_error);
		if (expected_error) {
			REQUIRE(result.GetErrorType() == direct->GetErrorType());
		} else {
			REQUIRE(result.GetTypes() == direct->GetTypes());
			REQUIRE(SQLExportRows(result, ordered) == SQLExportRows(*direct, ordered));
		}
	}
}

static bool AddValuesRows(unique_ptr<LogicalOperator> &op, idx_t row_count) {
	if (op->type != LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		for (auto &child : op->children) {
			if (AddValuesRows(child, row_count)) {
				return true;
			}
		}
		return false;
	}
	auto &input = op->Cast<LogicalExpressionGet>();
	if (input.expr_types.size() != 2) {
		return false;
	}
	auto table_index = input.table_index;
	input.table_index = TableIndex(1000000);
	vector<vector<unique_ptr<Expression>>> rows;
	for (idx_t i = 0; i < row_count; i++) {
		vector<unique_ptr<Expression>> row;
		for (idx_t column = 0; column < input.expr_types.size(); column++) {
			row.push_back(make_uniq<BoundColumnRefExpression>(
			    input.expr_types[column], ColumnBinding(input.table_index, ProjectionIndex(column))));
		}
		REQUIRE(row.size() == 2);
		row[1] = PlanIntegerConstant(NumericCast<int32_t>(i));
		rows.push_back(std::move(row));
	}
	auto get = make_uniq<LogicalExpressionGet>(table_index, input.expr_types, std::move(rows));
	get->children.push_back(std::move(op));
	op = std::move(get);
	return true;
}

} // namespace

TEST_CASE("Logical plan SQL export retains single VALUES row evaluation before consumers",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	for (bool filter : {false, true}) {
		for (bool late_error : {false, true}) {
			CAPTURE(filter, late_error);
			connection.BeginTransaction();
			auto values = late_error ? StringUtil::Repeat("('1'),", STANDARD_VECTOR_SIZE) + "('bad')" : "('1'),('bad')";
			unique_ptr<LogicalOperator> native =
			    ValuesFromProjection(connection, "SELECT x,CAST(x AS INTEGER) FROM (VALUES " + values + ")t(x)");
			if (filter) {
				auto predicate = BoundComparisonExpression::Create(
				    ExpressionType::COMPARE_EQUAL,
				    make_uniq<BoundColumnRefExpression>(LogicalType::VARCHAR,
				                                        ColumnBinding(TableIndex(1000), ProjectionIndex(0))),
				    make_uniq<BoundConstantExpression>(Value("1")));
				auto consumer = make_uniq<LogicalFilter>(std::move(predicate));
				consumer->children.push_back(std::move(native));
				native = std::move(consumer);
			} else {
				auto consumer = make_uniq<LogicalLimit>(BoundLimitNode::ConstantValue(1), BoundLimitNode());
				consumer->children.push_back(std::move(native));
				native = std::move(consumer);
			}
			CheckValuesRoundTrip(connection, std::move(native), filter || !late_error, false, false);
		}
	}
}

TEST_CASE("Logical plan SQL export checks consumers of multirow VALUES", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	const string input = " FROM (VALUES (2,0),(NULL,1),(2,2),(1,3))t(x,r)";
	vector<pair<string, bool>> queries {
	    {"SELECT x,r" + input, true},
	    {"SELECT x,r" + input + " WHERE x=0", true},
	    {"SELECT x,r" + input + " LIMIT 2", false},
	    {"SELECT x,r" + input + " LIMIT 50%", false},
	    {"SELECT x,r" + input + " OFFSET 1", false},
	    {"SELECT x,r" + input + " ORDER BY x,r LIMIT 2", true},
	    {"SELECT x,r" + input + " ORDER BY x LIMIT 2", false},
	    {"SELECT list(x)" + input, false},
	    {"SELECT list_sort(list(x))" + input, true},
	    {"SELECT array_sort(list(x))" + input, true},
	    {"SELECT list_sort(list(x), 'DESC', 'NULLS FIRST')" + input, true},
	    {"SELECT length(list_sort(list(x)))" + input, true},
	    {"SELECT list_sort(list(x)),list(x)" + input, false},
	    {"SELECT list_transform([1],lambda y:y+length(list(x)))" + input, false},
	    {"SELECT list(x ORDER BY x)" + input, true},
	    {"SELECT min(x),count(x)" + input, true},
	    {"SELECT r,min(x)" + input + " GROUP BY r", true},
	    {"SELECT r,list_sort(list(x))" + input + " GROUP BY r", true},
	    {"SELECT r,list_sort(list(x))" + input + " GROUP BY r LIMIT 1", false},
	    {"SELECT r,min(x)" + input + " GROUP BY r LIMIT 1", false},
	    {"SELECT DISTINCT x,r" + input, true},
	    {"(SELECT x,r" + input + ") UNION ALL (SELECT 2,3)", true},
	    {"(SELECT x,r" + input + ") UNION ALL (SELECT 2,3) LIMIT 2", false},
	    {"SELECT x,r FROM (VALUES ('a' COLLATE NOCASE,0),('A',1))t(x,r) ORDER BY x,r LIMIT 2", false},
	    {"SELECT DISTINCT ON(x) x,r" + input, false},
	    {"SELECT x,r,random()" + input, false},
	    {"SELECT sum(x),sum(r)" + input + " JOIN (VALUES (1),(2))u(y) ON x=y", true},
	    {"SELECT sum(x),sum(r)" + input + " LEFT JOIN (VALUES (1),(2))u(y) ON x=y", true},
	    {"SELECT x,r" + input + " JOIN (VALUES (1),(2))u(y) ON x=y LIMIT 2", false},
	    {"WITH v AS MATERIALIZED (SELECT x,r" + input + ") SELECT sum(x),sum(r) FROM v", false},
	};
	for (auto &entry : queries) {
		CAPTURE(entry.first, entry.second);
		// Compare values only where the query fixes order-dependent results.
		auto disabled = StringUtil::Contains(entry.first, "list(x ORDER BY") ? "aggregate_function_rewriter" : "";
		REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='" + string(disabled) + "'"));
		connection.BeginTransaction();
		auto native = OptimizeLogicalPlanExportQuery(connection, entry.first);
		REQUIRE(AddValuesRows(native, 3));
		native->ResolveOperatorTypes();
		if (entry.second) {
			CheckValuesRoundTrip(connection, std::move(native), false);
		} else {
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *native);
			REQUIRE(exported.IsSuccess());
			auto generated = connection.Query(exported.GetValue().query->ToString());
			REQUIRE_NO_FAIL(*generated);
			auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native)));
			REQUIRE_NO_FAIL(*direct);
			REQUIRE(generated->GetTypes() == direct->GetTypes());
			connection.Rollback();
		}
	}
	REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers=''"));
	connection.BeginTransaction();
	auto rewritten = OptimizeLogicalPlanExportQuery(connection, "SELECT list(x ORDER BY x)" + input);
	REQUIRE(AddValuesRows(rewritten, 3));
	CheckValuesRoundTrip(connection, std::move(rewritten), false);
}

TEST_CASE("Logical plan SQL export verifies unordered VALUES list canonicalizers",
          "[sql_export][logical_plan_sql_export]") {
	SECTION("list_sort aliases") {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
		for (const auto &name : {"list_sort", "array_sort"}) {
			for (bool binary : {false, true}) {
				CAPTURE(name, binary);
				connection.BeginTransaction();
				auto plan = OptimizeLogicalPlanExportQuery(
				    connection, "SELECT " + string(name) + "(list(x)) FROM (VALUES ('b',0),('B',1))t(x,r)");
				REQUIRE(AddValuesRows(plan, 3));
				plan->ResolveOperatorTypes();
				if (binary) {
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
				}
				CheckValuesRoundTrip(connection, std::move(plan), false);
			}
		}
	}

	SECTION("modified bound functions") {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
		for (bool modify_scalar : {false, true}) {
			CAPTURE(modify_scalar);
			connection.BeginTransaction();
			auto plan = OptimizeLogicalPlanExportQuery(
			    connection, "SELECT list_sort(list(x)) FROM (VALUES (2,0),(NULL,1),(2,2),(1,3))t(x,r)");
			REQUIRE(AddValuesRows(plan, 3));
			plan->ResolveOperatorTypes();
			auto reference_plan = plan->Copy(*connection.context);
			reference_plan->ResolveOperatorTypes();
			auto reference_export = LogicalPlanSQLExporter::Export(*connection.context, *reference_plan);
			REQUIRE(reference_export.IsSuccess());
			if (modify_scalar) {
				auto &projection = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_PROJECTION)
				                       ->Cast<LogicalProjection>();
				projection.expressions[0]->Cast<BoundFunctionExpression>().FunctionMutable().SetFunctionCallback(
				    ScalarFunction::NopFunction);
			} else {
				auto &aggregate =
				    FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY)
				        ->Cast<LogicalAggregate>()
				        .expressions[0]
				        ->Cast<BoundAggregateExpression>();
				auto callbacks = aggregate.Function().GetCallbacks();
				callbacks.initialize = nullptr;
				aggregate.FunctionMutable().SetCallbacks(callbacks);
			}
			plan->ResolveOperatorTypes();
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			REQUIRE(exported.IsSuccess());
			auto reference = connection.Query(reference_export.GetValue().query->ToString());
			auto generated = connection.Query(exported.GetValue().query->ToString());
			REQUIRE_NO_FAIL(*reference);
			REQUIRE_NO_FAIL(*generated);
			REQUIRE(generated->GetTypes() == reference->GetTypes());
			REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*reference, false));
			connection.Rollback();
		}
	}

	SECTION("modified registered scalar function") {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
		connection.BeginTransaction();
		auto &entry = Catalog::GetEntry<ScalarFunctionCatalogEntry>(*connection.context,
		                                                            QualifiedName("system", "main", "list_sort"));
		entry.functions.ApplyToFunctions(
		    [](ScalarFunction &function) { function.SetFunctionCallback(ScalarFunction::NopFunction); });
		connection.Rollback();
		for (bool binary : {false, true}) {
			CAPTURE(binary);
			connection.BeginTransaction();
			auto plan =
			    OptimizeLogicalPlanExportQuery(connection, "SELECT list_sort(list(x)) FROM (VALUES (2,0),(1,1))t(x,r)");
			REQUIRE(AddValuesRows(plan, 3));
			plan->ResolveOperatorTypes();
			if (binary) {
				plan = plan->Copy(*connection.context);
				plan->ResolveOperatorTypes();
			}
			auto native_plan = plan->Copy(*connection.context);
			native_plan->ResolveOperatorTypes();
			auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native_plan)));
			REQUIRE_NO_FAIL(*direct);
			REQUIRE(Value::NotDistinctFrom(direct->GetValue(0, 0),
			                               Value::LIST({Value::INTEGER(2), Value::INTEGER(1), Value::INTEGER(2),
			                                            Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(1)})));
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			REQUIRE(exported.IsSuccess());
			auto generated =
			    connection.Query("SELECT unnest(c0) FROM (" + exported.GetValue().query->ToString() + ") v(c0)");
			REQUIRE_NO_FAIL(*generated);
			auto expected = connection.Query("SELECT * FROM (VALUES (1), (1), (1), (2), (2), (2)) t(x)");
			REQUIRE_NO_FAIL(*expected);
			REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*expected, false));
			connection.Rollback();
		}
	}

	SECTION("collation-equivalent payloads") {
		for (bool collated : {false, true}) {
			CAPTURE(collated);
			DuckDB db(nullptr);
			Connection connection(db);
			REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
			if (collated) {
				REQUIRE_NO_FAIL(connection.Query("SET default_collation='nocase'"));
			}
			for (bool binary : {false, true}) {
				CAPTURE(binary);
				connection.BeginTransaction();
				auto plan = OptimizeLogicalPlanExportQuery(
				    connection, "SELECT list_sort(list(x)) FROM (VALUES ('b',0),('B',1))t(x,r)");
				REQUIRE(AddValuesRows(plan, 3));
				plan->ResolveOperatorTypes();
				if (binary) {
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
				}
				auto native_plan = plan->Copy(*connection.context);
				native_plan->ResolveOperatorTypes();
				auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
				auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native_plan)));
				REQUIRE_NO_FAIL(*direct);
				vector<Value> expected;
				for (idx_t i = 0; i < 3; i++) {
					if (collated) {
						expected.push_back(Value("b"));
						expected.push_back(Value("B"));
					} else {
						expected.insert(expected.begin(), Value("B"));
						expected.push_back(Value("b"));
					}
				}
				REQUIRE(Value::NotDistinctFrom(direct->GetValue(0, 0),
				                               Value::LIST(LogicalType::VARCHAR, std::move(expected))));
				connection.Rollback();
				INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
				REQUIRE(exported.IsSuccess());
				auto text = exported.GetValue().query->ToString();
				auto generated = connection.Query(text);
				REQUIRE_NO_FAIL(*generated);
				REQUIRE(generated->GetTypes() == direct->GetTypes());
				auto statement = make_uniq<SelectStatement>();
				statement->node = std::move(exported.GetValue().query);
				auto ast = connection.Query(std::move(statement));
				REQUIRE_NO_FAIL(*ast);
				REQUIRE(ast->GetTypes() == direct->GetTypes());
				for (auto result : {generated.get(), ast.get()}) {
					auto value = result->GetValue(0, 0);
					auto &values = ListValue::GetChildren(value);
					REQUIRE(values.size() == 6);
					REQUIRE(std::count(values.begin(), values.end(), Value("b")) == 3);
					REQUIRE(std::count(values.begin(), values.end(), Value("B")) == 3);
				}
			}
		}
	}

	SECTION("collation setting changes after binding") {
		const string sql = "SELECT list_sort(list(x)) FROM (VALUES ('a',0),('B',1))t(x,r)";
		auto expected_value = [](bool bind_nocase, bool multirow_values) {
			vector<Value> expected;
			idx_t row_count = multirow_values ? 3 : 1;
			for (idx_t i = 0; i < row_count; i++) {
				if (bind_nocase) {
					expected.insert(expected.begin(), Value("a"));
					expected.push_back(Value("B"));
				} else {
					expected.insert(expected.begin(), Value("B"));
					expected.push_back(Value("a"));
				}
			}
			return Value::LIST(LogicalType::VARCHAR, std::move(expected));
		};

		SECTION("bind data copy retains the sort key") {
			for (bool bind_nocase : {false, true}) {
				for (bool multirow_values : {false, true}) {
					CAPTURE(bind_nocase, multirow_values);
					DuckDB db(nullptr);
					Connection connection(db);
					REQUIRE_NO_FAIL(connection.Query(bind_nocase ? "SET threads=1; SET default_collation='nocase'"
					                                             : "SET threads=1; SET default_collation=''"));
					connection.BeginTransaction();
					auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
					if (multirow_values) {
						REQUIRE(AddValuesRows(plan, 3));
					}
					plan->ResolveOperatorTypes();
					REQUIRE_NO_FAIL(
					    connection.Query(bind_nocase ? "SET default_collation=''" : "SET default_collation='nocase'"));
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
					REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
					auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
					REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=false"));
					REQUIRE_NO_FAIL(*direct);
					REQUIRE(
					    Value::NotDistinctFrom(direct->GetValue(0, 0), expected_value(bind_nocase, multirow_values)));
					connection.Rollback();
				}
			}
		}

		SECTION("export retains the bound sort key") {
			for (bool bind_nocase : {false, true}) {
				for (bool binary : {false, true}) {
					CAPTURE(bind_nocase, binary);
					DuckDB db(nullptr);
					Connection connection(db);
					REQUIRE_NO_FAIL(connection.Query(bind_nocase ? "SET threads=1; SET default_collation='nocase'"
					                                             : "SET threads=1; SET default_collation=''"));
					connection.BeginTransaction();
					auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
					REQUIRE(AddValuesRows(plan, 3));
					plan->ResolveOperatorTypes();
					if (binary) {
						plan = plan->Copy(*connection.context);
						plan->ResolveOperatorTypes();
					}
					REQUIRE_NO_FAIL(
					    connection.Query(bind_nocase ? "SET default_collation=''" : "SET default_collation='nocase'"));
					auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
					INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
					REQUIRE(exported.IsSuccess());

					auto generated = connection.Query(exported.GetValue().query->ToString());
					REQUIRE_NO_FAIL(*generated);
					REQUIRE(Value::NotDistinctFrom(generated->GetValue(0, 0), expected_value(bind_nocase, true)));
					connection.Rollback();
				}
			}
		}
	}
}

TEST_CASE("Logical plan SQL export retains unused VALUES field evaluation", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	auto values = ValuesFromProjection(connection, "SELECT x,CAST(x AS INTEGER) FROM (VALUES('1'),('bad'))t(x)");
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::VARCHAR, ColumnBinding(TableIndex(1000), ProjectionIndex(0))));
	auto projection = PlanProjection(TableIndex(1001), std::move(values), std::move(expressions));
	CheckValuesRoundTrip(connection, std::move(projection), true, false, false);
}

namespace {

struct SQLExportValuesLocalState : FunctionLocalState {
	int64_t value = 0;
};

static unique_ptr<FunctionLocalState> SQLExportValuesInitState(ExpressionState &, const BoundFunctionExpression &,
                                                               FunctionData *) {
	return make_uniq<SQLExportValuesLocalState>();
}

static void SQLExportValuesLocalNext(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<SQLExportValuesLocalState>();
	auto writer = FlatVector::Writer<int64_t>(result, args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		writer.WriteValue(++local.value);
	}
}

} // namespace

TEST_CASE("Logical plan SQL export diagnoses VALUES executor state lifetime", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "sql_export_values_local_state");
	ScalarFunction function("sql_export_values_local_next", {}, LogicalType::BIGINT, SQLExportValuesLocalNext);
	function.SetVolatile();
	function.SetInitStateCallback(SQLExportValuesInitState);
	loader.RegisterFunction(std::move(function));
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	connection.BeginTransaction();
	auto input = StringUtil::Repeat("(1),", STANDARD_VECTOR_SIZE) + "(2)";
	auto native =
	    ValuesFromProjection(connection, "SELECT x,sql_export_values_local_next() FROM (VALUES " + input + ")t(x)");
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *native);
	RequirePlanExportIssue(exported, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(native)));
	REQUIRE_NO_FAIL(*direct);
	REQUIRE(direct->GetValue(1, 0) == Value::BIGINT(1));
	REQUIRE(direct->GetValue(1, STANDARD_VECTOR_SIZE - 1) == Value::BIGINT(STANDARD_VECTOR_SIZE));
	REQUIRE(direct->GetValue(1, STANDARD_VECTOR_SIZE) == Value::BIGINT(1));
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export declines limits that observe multirow VALUES chunks",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	for (auto &shape : vector<pair<idx_t, idx_t>> {{1, 2}, {1024, 2}, {1025, 2}, {1024, 12}}) {
		CAPTURE(shape.first, shape.second);
		connection.BeginTransaction();
		auto input = StringUtil::Repeat("(1),", shape.first - 1) + "(1)";
		auto values = ValuesFromProjection(
		    connection, "SELECT x,error('VALUES row reached')::INTEGER FROM (VALUES " + input + ")t(x)", shape.second);
		for (idx_t i = 0; i + 1 < shape.second; i++) {
			values->expressions[i][1] = values->expressions[i][0]->Copy();
		}
		auto limit =
		    make_uniq<LogicalLimit>(BoundLimitNode::ConstantValue(shape.second == 12 ? 10241 : 1), BoundLimitNode());
		limit->children.push_back(std::move(values));
		limit->ResolveOperatorTypes();
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *limit);
		RequirePlanExportIssue(exported, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE,
		                       {LogicalPlanVerificationPathRoot::LOGICAL_PLAN,
		                        {{LogicalPlanVerificationPathComponentType::OPERATOR_CHILD, 0}}});
		auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(limit)));
		REQUIRE(direct->HasError() == (shape.first != 1025));
		if (direct->HasError()) {
			REQUIRE(StringUtil::Contains(direct->GetError(), "VALUES row reached"));
		} else {
			REQUIRE(direct->RowCount() == 1);
		}
		connection.Rollback();
	}
}

TEST_CASE("Logical plan SQL export checks partial consumers of effectful VALUES",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_streaming_buffer_size='1b'"));
	const idx_t input_count = STANDARD_VECTOR_SIZE / 2;
	const idx_t requested_rows = input_count * 9;
	for (bool ordered : {false, true}) {
		CAPTURE(ordered);
		connection.BeginTransaction();
		auto input = StringUtil::Repeat("(1),", input_count - 1) + "(1)";
		auto values = ValuesFromProjection(
		    connection, "SELECT x,error('VALUES row reached')::INTEGER FROM (VALUES " + input + ")t(x)", 12);
		for (idx_t i = 0; i + 1 < values->expressions.size(); i++) {
			values->expressions[i][1] = values->expressions[i][0]->Copy();
		}
		unique_ptr<LogicalOperator> native = std::move(values);
		if (ordered) {
			native = OrderValues(std::move(native));
		}
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *native);
		RequireValuesEvaluationRejection(exported);
		QueryParameters parameters;
		parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
		auto check_prefix = [&](unique_ptr<QueryResult> result) {
			idx_t count = 0;
			bool valid = true;
			while (count < requested_rows && !result->HasError()) {
				auto chunk = result->Fetch();
				if (!chunk) {
					break;
				}
				for (idx_t i = 0; i < chunk->size(); i++) {
					valid &= chunk->GetValue(0, i) == Value::INTEGER(1);
					valid &= chunk->GetValue(1, i) == Value::INTEGER(1);
				}
				count += chunk->size();
			}
			REQUIRE(valid);
			REQUIRE(result->HasError());
			REQUIRE(StringUtil::Contains(result->GetError(), "VALUES row reached"));
			REQUIRE(count == (ordered ? 0 : STANDARD_VECTOR_SIZE * 4));
		};
		check_prefix(connection.context->Query(make_uniq<LogicalPlanStatement>(std::move(native)), parameters));
		connection.Rollback();
		REQUIRE_NO_FAIL(connection.Query("SELECT 42"));
	}
}

TEST_CASE("Logical plan SQL export admits blocking consumers of effectful VALUES",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	for (const auto &sql : {"SELECT min(x),min(r) FROM (VALUES(1,1),(2,2))t(x,r)",
	                        "SELECT x,min(r) FROM (VALUES(1,1),(2,2))t(x,r) GROUP BY x",
	                        "SELECT DISTINCT x,r FROM (VALUES(1,1),(2,2))t(x,r)"}) {
		CAPTURE(sql);
		connection.BeginTransaction();
		auto native = OptimizeLogicalPlanExportQuery(connection, sql);
		REQUIRE(AddValuesRows(native, 3));
		auto get = FindLogicalPlanExportOperator(*native, LogicalOperatorType::LOGICAL_EXPRESSION_GET);
		REQUIRE(get);
		auto throwing =
		    ValuesFromProjection(connection, "SELECT x,error('VALUES row reached')::INTEGER FROM (VALUES(1))t(x)");
		get->Cast<LogicalExpressionGet>().expressions.back()[1] = std::move(throwing->expressions[0][1]);
		CheckValuesRoundTrip(connection, std::move(native), true, false, false);
	}
}

TEST_CASE("Logical plan SQL export retains partial single-row VALUES streams",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_streaming_buffer_size='1b'"));
	for (bool finish : {false, true}) {
		CAPTURE(finish);
		connection.BeginTransaction();
		auto input = StringUtil::Repeat("('1'),", STANDARD_VECTOR_SIZE * 2) + "('bad')";
		auto native = ValuesFromProjection(connection, "SELECT x,CAST(x AS INTEGER) FROM (VALUES " + input + ")t(x)");
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *native);
		RequireValuesEvaluationRejection(exported);
		QueryParameters parameters;
		parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
		auto check = [&](unique_ptr<QueryResult> result) {
			REQUIRE_FALSE(result->HasError());
			REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
			auto first = result->Fetch();
			REQUIRE(first);
			REQUIRE(first->size() == STANDARD_VECTOR_SIZE);
			REQUIRE(first->GetValue(1, 0) == Value::INTEGER(1));
			REQUIRE_FALSE(result->HasError());
			if (finish) {
				while (result->Fetch()) {
				}
				REQUIRE(result->HasError());
				REQUIRE(result->GetErrorType() == ExceptionType::CONVERSION);
			} else {
				result->Cast<StreamQueryResult>().Close();
				REQUIRE_FALSE(result->HasError());
			}
		};
		check(connection.context->Query(make_uniq<LogicalPlanStatement>(std::move(native)), parameters));
		connection.Rollback();
		REQUIRE_NO_FAIL(connection.Query("SELECT 42"));
	}
}

TEST_CASE("Retained source SQL invocation survives plan serialization",
          "[sql_export][logical_plan_sql_export][table_source_sql]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT sum(x) FROM range(10) t(x)");
	auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
	REQUIRE(get);
	auto parameters = get->Cast<LogicalGet>().parameters;
	auto copy = plan->Copy(*connection.context);
	auto copied_get = FindLogicalPlanExportOperator(*copy, LogicalOperatorType::LOGICAL_GET);
	REQUIRE(copied_get);
	REQUIRE(copied_get->Cast<LogicalGet>().parameters == parameters);
	REQUIRE(LogicalPlanSQLExporter::Export(*connection.context, *copy).IsSuccess());
	copied_get->Cast<LogicalGet>().function.to_sql = nullptr;
	auto unsupported = LogicalPlanSQLExporter::Export(*connection.context, *copy);
	REQUIRE(unsupported.HasError());
	REQUIRE(unsupported.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export retains bound source arguments and reopens files",
          "[sql_export][logical_plan_sql_export][table_source_sql]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET VARIABLE sql_export_count=4"));
	connection.BeginTransaction();
	const string variable_sql = "SELECT * FROM range(getvariable('sql_export_count')::BIGINT) ORDER BY ALL";
	auto plan = OptimizeLogicalPlanExportQuery(connection, variable_sql);
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.IsSuccess());
	auto generated_sql = exported.GetValue().query->ToString();
	REQUIRE_FALSE(StringUtil::Contains(generated_sql, "getvariable"));
	REQUIRE_NO_FAIL(connection.Query("SET VARIABLE sql_export_count=2"));
	auto original = connection.Query(variable_sql);
	auto generated = connection.Query(generated_sql);
	REQUIRE_NO_FAIL(*original);
	REQUIRE_NO_FAIL(*generated);
	REQUIRE(original->RowCount() == 2);
	REQUIRE(generated->RowCount() == 4);

	auto csv_path = TestCreatePath("sql_export_generic_source.csv");
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE csv_source(i INTEGER, s VARCHAR);"
	                                 "INSERT INTO csv_source VALUES (1, 'a'), (2, NULL);"
	                                 "COPY csv_source TO " +
	                                 Value(csv_path).ToSQLString() + " (HEADER, DELIMITER '|')"));
	const auto count_sql = "SELECT count(*) FROM read_csv(" + Value(csv_path).ToSQLString() +
	                       ", header := true, delim := '|', auto_detect := true)";
	auto count_plan = OptimizeLogicalPlanExportQueryWithRepeatedPruning(connection, count_sql);
	auto &count_get = FindLogicalPlanExportOperator(*count_plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
	REQUIRE(count_get.GetColumnIds().size() == 1);
	REQUIRE(count_get.GetColumnIds()[0].IsEmptyColumn());
	auto count_export = LogicalPlanSQLExporter::Export(*connection.context, *count_plan);
	REQUIRE(count_export.IsSuccess());
	auto count_direct = connection.Query(count_sql);
	auto count_rebound = connection.Query(count_export.GetValue().query->ToString());
	REQUIRE_NO_FAIL(*count_direct);
	REQUIRE_NO_FAIL(*count_rebound);
	REQUIRE(SQLExportRows(*count_rebound, true) == SQLExportRows(*count_direct, true));
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export retains consumed file predicates",
          "[sql_export][logical_plan_sql_export][table_source_sql]") {
	DuckDB db(nullptr);
	Connection connection(db);
	for (const auto &format : {string("CSV"), string("parquet")}) {
		auto directory = TestCreatePath("sql_export_file_predicates_" + format);
		TestDeleteDirectory(directory);
		REQUIRE_NO_FAIL(
		    connection.Query("COPY (SELECT * FROM (VALUES (10,1), (10,1), (NULL,1), (20,2), (30,NULL)) t(i,p)) TO " +
		                     Value(directory).ToSQLString() + " (FORMAT " + format + ", PARTITION_BY(p)" +
		                     (format == "CSV" ? ", HEADER)" : ")")));
		connection.BeginTransaction();
		auto source = (format == "CSV" ? "read_csv(" : "read_parquet(") +
		              Value(directory + "/*/*." + (format == "CSV" ? "csv" : "parquet")).ToSQLString() +
		              ", hive_partitioning := true)";
		for (const auto &predicate :
		     vector<string> {"p = 1", "p IS NULL", "p IN (1,2) AND i > 15", "p >= 1 OR p IS NULL",
		                     "array_to_string([p::VARCHAR, 'x'], '/') = '1/x'"}) {
			auto sql = "SELECT i FROM " + source + " WHERE " + predicate + " ORDER BY i";
			INFO(sql);
			auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
			auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
			REQUIRE(get);
			auto &info = get->Cast<LogicalGet>().extra_info;
			REQUIRE(info.file_filter_expressions);
			REQUIRE_FALSE(info.file_filter_expressions->empty());
			if (format == "parquet") {
				auto copy = plan->Copy(*connection.context);
				auto copied_get = FindLogicalPlanExportOperator(*copy, LogicalOperatorType::LOGICAL_GET);
				REQUIRE(copied_get);
				auto &copied_info = copied_get->Cast<LogicalGet>().extra_info;
				REQUIRE(copied_info.file_filter_expressions);
				REQUIRE(copied_info.file_filter_expressions->size() == info.file_filter_expressions->size());
				for (idx_t index = 0; index < info.file_filter_expressions->size(); index++) {
					REQUIRE(
					    (*copied_info.file_filter_expressions)[index]->Equals(*(*info.file_filter_expressions)[index]));
				}
				REQUIRE(LogicalPlanSQLExporter::Export(*connection.context, *copy).IsSuccess());
			}
			info.file_filter_expressions.reset();
			auto missing = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			REQUIRE(missing.HasError());
			REQUIRE(missing.GetIssues()[0].construct ==
			        LogicalPlanVerificationConstructIdentity::ExportFeature("file_filter_residual"));
		}
		connection.Rollback();
		TestDeleteDirectory(directory);
	}
}

TEST_CASE("Logical plan SQL export rejects missing relational input types",
          "[sql_export][logical_plan_sql_export][table_source_sql]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	auto incomplete = OptimizeLogicalPlanExportQuery(connection, "SELECT * FROM range(3) t(i), range(i) u(j)");
	optional_ptr<LogicalGet> incomplete_get;
	std::function<void(LogicalOperator &)> find_incomplete = [&](LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_GET && !op.children.empty()) {
			incomplete_get = op.Cast<LogicalGet>();
		}
		for (auto &child : op.children) {
			find_incomplete(*child);
		}
	};
	find_incomplete(*incomplete);
	REQUIRE(incomplete_get);
	incomplete_get->input_table_types.clear();
	auto rejected = LogicalPlanSQLExporter::Export(*connection.context, *incomplete);
	REQUIRE(rejected.HasError());
	REQUIRE(rejected.GetIssues().size() == 1);
	REQUIRE(rejected.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
	REQUIRE((rejected.GetIssues()[0].facts[0] == pair<string, Value> {"guard", Value("input_columns")}));
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export copies owned chunk data and selected columns",
          "[sql_export][logical_plan_sql_export][chunk_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	for (idx_t rows : {0, 5, 4097}) {
		for (bool binary : {false, true}) {
			CAPTURE(rows, binary);
			auto result = connection.Query("SELECT i::SMALLINT, CASE WHEN i%3=0 THEN NULL ELSE [i,NULL] END xs, "
			                               "CASE WHEN i%2=0 THEN NULL ELSE 'a''b' END s FROM range(" +
			                               to_string(rows) + ") t(i)");
			REQUIRE_NO_FAIL(*result);
			connection.BeginTransaction();
			auto get = make_uniq<LogicalColumnDataGet>(TableIndex(1001), result->GetTypes(), result->TakeCollection());
			get->SetColumnIds({2, 0, 2, 1});
			unique_ptr<LogicalOperator> plan = std::move(get);
			if (binary) {
				plan = plan->Copy(*connection.context);
				plan->ResolveOperatorTypes();
			}
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			REQUIRE(exported.IsSuccess());
			auto text = exported.GetValue().query->ToString();
			auto statement = make_uniq<SelectStatement>();
			statement->node = std::move(exported.GetValue().query);
			REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
			auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
			REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
			auto generated = connection.Query(text);
			auto ast = connection.Query(std::move(statement));
			REQUIRE_NO_FAIL(*direct);
			REQUIRE_NO_FAIL(*generated);
			REQUIRE_NO_FAIL(*ast);
			REQUIRE(direct->RowCount() == rows);
			REQUIRE(generated->GetTypes() == direct->GetTypes());
			REQUIRE(ast->GetTypes() == direct->GetTypes());
			REQUIRE(SQLExportRows(*generated, true) == SQLExportRows(*direct, true));
			REQUIRE(SQLExportRows(*ast, true) == SQLExportRows(*direct, true));
			connection.Rollback();
		}
	}
	auto result = connection.Query("SELECT 42::BIGINT");
	REQUIRE_NO_FAIL(*result);
	connection.BeginTransaction();
	auto borrowed = make_uniq<LogicalColumnDataGet>(TableIndex(1002), result->GetTypes(), result->Collection());
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *borrowed);
	RequirePlanExportIssue(exported, LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
	REQUIRE(exported.GetIssues()[0].construct->function->name == "logical_source");
	REQUIRE(exported.GetIssues()[0].facts.size() == 1);
	REQUIRE((exported.GetIssues()[0].facts[0] == pair<string, Value> {"guard", Value("borrowed_chunk_collection")}));
	auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(borrowed)));
	REQUIRE_NO_FAIL(*native);
	REQUIRE(native->GetValue(0, 0) == Value::BIGINT(42));
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export preserves join output maps and empty sides",
          "[sql_export][logical_plan_sql_export][join_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	for (auto type : {JoinType::INNER, JoinType::LEFT, JoinType::RIGHT, JoinType::OUTER, JoinType::SEMI, JoinType::ANTI,
	                  JoinType::RIGHT_SEMI, JoinType::RIGHT_ANTI, JoinType::MARK, JoinType::SINGLE}) {
		for (idx_t empty_mask : {0, 1, 2, 3}) {
			CAPTURE(type, empty_mask);
			unique_ptr<LogicalOperator> left = IntegerValues(TableIndex(1010), {{1, 10}, {2, 20}, {2, 21}});
			unique_ptr<LogicalOperator> right = IntegerValues(TableIndex(1011), {{2, 100}, {3, 200}});
			if (empty_mask & 1) {
				left = make_uniq<LogicalEmptyResult>(std::move(left));
			}
			if (empty_mask & 2) {
				right = make_uniq<LogicalEmptyResult>(std::move(right));
			}
			auto join = make_uniq<LogicalComparisonJoin>(type);
			join->mark_index = TableIndex(1012);
			join->left_projection_map = {ProjectionIndex(1)};
			join->right_projection_map = {ProjectionIndex(1), ProjectionIndex(0)};
			join->conditions.emplace_back(
			    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
			                                        ColumnBinding(TableIndex(1010), ProjectionIndex(0))),
			    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
			                                        ColumnBinding(TableIndex(1011), ProjectionIndex(0))),
			    ExpressionType::COMPARE_EQUAL);
			join->children.push_back(std::move(left));
			join->children.push_back(std::move(right));
			join->ResolveOperatorTypes();
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *join);
			REQUIRE(exported.IsSuccess());
			REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
			auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(join)));
			REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
			auto generated = connection.Query(exported.GetValue().query->ToString());
			REQUIRE_NO_FAIL(*direct);
			REQUIRE_NO_FAIL(*generated);
			REQUIRE(generated->GetTypes() == direct->GetTypes());
			REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*direct, false));
		}
	}
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export preserves empty filter projection maps", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE filter_input(x INTEGER,y VARCHAR,z INTEGER);"
	                                 "INSERT INTO filter_input VALUES(1,'a',10),(2,'b',20),(3,NULL,30)"));
	for (auto projection :
	     vector<vector<ProjectionIndex>> {{},
	                                      {ProjectionIndex(2), ProjectionIndex(0)},
	                                      {ProjectionIndex(1), ProjectionIndex(1), ProjectionIndex(0)}}) {
		connection.BeginTransaction();
		auto filter = make_uniq<LogicalFilter>();
		filter->children.push_back(OptimizeLogicalPlanExportQuery(connection, "SELECT * FROM filter_input WHERE x>1"));
		filter->projection_map = projection;
		filter->ResolveOperatorTypes();
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *filter);
		REQUIRE(exported.IsSuccess());
		auto copied = filter->Copy(*connection.context);
		auto copied_export = LogicalPlanSQLExporter::Export(*connection.context, *copied);
		REQUIRE(copied_export.IsSuccess());
		auto text = exported.GetValue().query->ToString();
		REQUIRE(text == copied_export.GetValue().query->ToString());
		REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
		auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(filter)));
		REQUIRE_NO_FAIL(*native);
		REQUIRE(native->RowCount() == 2);
		REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=false"));
		auto generated = connection.Query(text);
		REQUIRE_NO_FAIL(*generated);
		REQUIRE(generated->GetTypes() == native->GetTypes());
		REQUIRE(SQLExportRows(*generated, true) == SQLExportRows(*native, true));
		auto statement = make_uniq<SelectStatement>();
		statement->node = std::move(exported.GetValue().query);
		auto ast = connection.Query(std::move(statement));
		REQUIRE_NO_FAIL(*ast);
		REQUIRE(ast->GetTypes() == native->GetTypes());
		REQUIRE(SQLExportRows(*ast, true) == SQLExportRows(*native, true));
		connection.Rollback();
	}
}

TEST_CASE("Owned chunk export preserves observable execution groups",
          "[sql_export][logical_plan_sql_export][chunk_sql_export]") {
	for (idx_t count : vector<idx_t> {1, STANDARD_VECTOR_SIZE}) {
		for (bool combined : {false, true}) {
			CAPTURE(count, combined);
			DuckDB db(nullptr);
			Connection connection(db);
			REQUIRE_NO_FAIL(connection.Query("SET threads=1; CREATE TABLE chunk_export_input(x BIGINT)"));
			auto counter = make_shared_ptr<SQLExportValuesCounter>();
			connection.context->registered_state->Insert("sql_export_values_counter", counter);
			ExtensionLoader loader(*db.instance, "chunk_export_counter");
			ScalarFunction next("sql_export_values_next", {}, LogicalType::BIGINT, SQLExportValuesNext);
			next.SetVolatile();
			loader.RegisterFunction(std::move(next));
			ScalarFunction peek("sql_export_values_peek", {}, LogicalType::BIGINT, SQLExportValuesPeek);
			peek.SetVolatile();
			loader.RegisterFunction(std::move(peek));
			auto first = connection.Query("SELECT * FROM range(" + to_string(combined ? count : count * 2) + ")");
			REQUIRE_NO_FAIL(*first);
			auto collection = first->TakeCollection();
			if (combined) {
				auto second =
				    connection.Query("SELECT * FROM range(" + to_string(count) + "," + to_string(count * 2) + ")");
				REQUIRE_NO_FAIL(*second);
				auto other = second->TakeCollection();
				collection->Combine(*other);
			}
			REQUIRE(collection->ChunkCount() ==
			        (combined ? 2 : (count * 2 + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE));
			connection.BeginTransaction();
			auto plan = OptimizeLogicalPlanExportQuery(
			    connection, "SELECT x,sql_export_values_next(),sql_export_values_peek() FROM chunk_export_input");
			auto &projection = plan->Cast<LogicalProjection>();
			auto table_index = projection.children[0]->Cast<LogicalGet>().table_index;
			projection.children[0] = make_uniq<LogicalColumnDataGet>(
			    table_index, vector<LogicalType> {LogicalType::BIGINT}, std::move(collection));
			plan->ResolveOperatorTypes();
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			if (combined && count == 1) {
				REQUIRE(exported.HasError());
				plan = OrderValues(std::move(plan));
				auto ordered = LogicalPlanSQLExporter::Export(*connection.context, *plan);
				REQUIRE(ordered.HasError());
				REQUIRE(*ordered.GetIssues()[0].construct ==
				        LogicalPlanVerificationConstructIdentity::ExportFeature("chunk_consumer_evaluation"));
			}
			REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
			counter->value = 0;
			auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
			REQUIRE_NO_FAIL(*native);
			REQUIRE(native->RowCount() == count * 2);
			REQUIRE(native->GetValue(2, 0) ==
			        Value::BIGINT(combined ? count : MinValue<idx_t>(count * 2, STANDARD_VECTOR_SIZE)));
			connection.Rollback();
			REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
			if (combined && count == 1) {
				REQUIRE(exported.HasError());
				REQUIRE(*exported.GetIssues()[0].construct ==
				        LogicalPlanVerificationConstructIdentity::ExportFeature("chunk_consumer_evaluation"));
				continue;
			}
			REQUIRE(exported.IsSuccess());
			auto text = exported.GetValue().query->ToString();
			auto ast = make_uniq<SelectStatement>();
			ast->node = std::move(exported.GetValue().query);
			for (bool direct_ast : {false, true}) {
				counter->value = 0;
				auto generated = direct_ast ? connection.Query(std::move(ast)) : connection.Query(text);
				REQUIRE_NO_FAIL(*generated);
				REQUIRE(generated->RowCount() == native->RowCount());
				REQUIRE(SQLExportRows(*generated, true) == SQLExportRows(*native, true));
			}
		}
	}
}

TEST_CASE("Owned chunk SQL export retains delivered rows before conversion errors",
          "[sql_export][logical_plan_sql_export][chunk_sql_export]") {
	for (idx_t count : vector<idx_t> {STANDARD_VECTOR_SIZE, STANDARD_VECTOR_SIZE + STANDARD_VECTOR_SIZE / 2}) {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(
		    connection.Query("SET threads=1; SET max_streaming_buffer_size='1b'; CREATE TABLE chunk_input(x VARCHAR)"));
		auto good = connection.Query("SELECT '1'::VARCHAR FROM range(" + to_string(count) + ")");
		auto bad = connection.Query("SELECT 'bad'::VARCHAR FROM range(" + to_string(count) + ")");
		REQUIRE_NO_FAIL(*good);
		REQUIRE_NO_FAIL(*bad);
		auto collection = good->TakeCollection();
		auto second = bad->TakeCollection();
		collection->Combine(*second);
		connection.BeginTransaction();
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT x::INTEGER FROM chunk_input");
		auto &projection = plan->Cast<LogicalProjection>();
		auto table_index = projection.children[0]->Cast<LogicalGet>().table_index;
		projection.children[0] = make_uniq<LogicalColumnDataGet>(
		    table_index, vector<LogicalType> {LogicalType::VARCHAR}, std::move(collection));
		plan->ResolveOperatorTypes();
		auto pure = LogicalPlanSQLExporter::Export(*connection.context, *projection.children[0]);
		REQUIRE(pure.IsSuccess());
		auto pure_result = connection.Query(pure.GetValue().query->ToString());
		REQUIRE_NO_FAIL(*pure_result);
		REQUIRE(pure_result->RowCount() == count * 2);
		REQUIRE(pure_result->GetValue(0, count - 1) == Value("1"));
		REQUIRE(pure_result->GetValue(0, count) == Value("bad"));
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		if (count == STANDARD_VECTOR_SIZE) {
			REQUIRE(exported.IsSuccess());
		} else {
			REQUIRE(exported.HasError());
			REQUIRE(*exported.GetIssues()[0].construct ==
			        LogicalPlanVerificationConstructIdentity::ExportFeature("chunk_consumer_evaluation"));
		}
		QueryParameters parameters;
		parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
		auto drain = [&](unique_ptr<QueryResult> result) {
			idx_t rows = 0;
			while (!result->HasError()) {
				auto chunk = result->Fetch();
				if (!chunk) {
					break;
				}
				for (idx_t row = 0; row < chunk->size(); row++) {
					REQUIRE(chunk->GetValue(0, row) == Value::INTEGER(1));
				}
				rows += chunk->size();
			}
			REQUIRE(result->HasError());
			REQUIRE(StringUtil::Contains(result->GetError(), "Could not convert string"));
			return rows;
		};
		REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
		auto native_rows =
		    drain(connection.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), parameters));
		connection.Rollback();
		REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
		if (count == STANDARD_VECTOR_SIZE) {
			auto text_rows = drain(connection.context->Query(exported.GetValue().query->ToString(), parameters));
			auto statement = make_uniq<SelectStatement>();
			statement->node = std::move(exported.GetValue().query);
			auto ast_rows = drain(connection.context->Query(std::move(statement), parameters));
			REQUIRE(text_rows == native_rows);
			REQUIRE(ast_rows == native_rows);
		} else {
			REQUIRE(native_rows == count);
		}
		REQUIRE_NO_FAIL(connection.Query("SELECT 42"));
	}
}

TEST_CASE("Owned chunk SQL export requires an explicit consumer evaluation contract",
          "[sql_export][logical_plan_sql_export][chunk_sql_export]") {
	for (bool combined : {false, true}) {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
		auto counter = make_shared_ptr<SQLExportValuesCounter>();
		connection.context->registered_state->Insert("sql_export_values_counter", counter);
		ExtensionLoader loader(*db.instance, "opaque_chunk_export_counter");
		ScalarFunction next("sql_export_values_next", {}, LogicalType::BIGINT, SQLExportValuesNext);
		next.SetVolatile();
		loader.RegisterFunction(std::move(next));
		ScalarFunction peek("sql_export_values_peek", {}, LogicalType::BIGINT, SQLExportValuesPeek);
		peek.SetVolatile();
		loader.RegisterFunction(std::move(peek));
		auto first = connection.Query(combined ? "SELECT 0::BIGINT" : "SELECT * FROM range(2)");
		REQUIRE_NO_FAIL(*first);
		auto collection = first->TakeCollection();
		if (combined) {
			auto second = connection.Query("SELECT 1::BIGINT");
			REQUIRE_NO_FAIL(*second);
			auto part = second->TakeCollection();
			collection->Combine(*part);
		}
		connection.BeginTransaction();
		auto seed =
		    OptimizeLogicalPlanExportQuery(connection, "SELECT sql_export_values_next(),sql_export_values_peek()");
		auto op = make_uniq<SQLExportOpaqueProjection>(std::move(seed->expressions));
		op->children.push_back(make_uniq<LogicalColumnDataGet>(
		    TableIndex(2000), vector<LogicalType> {LogicalType::BIGINT}, std::move(collection)));
		op->ResolveOperatorTypes();
		REQUIRE(op->expressions.empty());
		LogicalPlanSQLExportOptions options;
		idx_t callbacks = 0;
		options.extension_resolver = [&](const LogicalPlanSQLExportExtensionInput &input) {
			callbacks++;
			REQUIRE(input.expression_count == 0);
			auto select = make_uniq<SelectNode>();
			auto column = input.binding_context.resolve_binding(ColumnBinding(TableIndex(2000), ProjectionIndex(0)));
			REQUIRE(column);
			select->select_list.push_back(make_uniq<ColumnRefExpression>(column->names));
			for (auto name : {"sql_export_values_next", "sql_export_values_peek"}) {
				vector<unique_ptr<ParsedExpression>> arguments;
				select->select_list.push_back(make_uniq<FunctionExpression>(Identifier(name), std::move(arguments)));
			}
			select->from_table = std::move(input.children[0].table);
			return LogicalPlanSQLExportExtensionResult::Exported(std::move(select));
		};
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *op, options);
		if (combined) {
			REQUIRE(exported.HasError());
			REQUIRE(*exported.GetIssues()[0].construct ==
			        LogicalPlanVerificationConstructIdentity::ExportFeature("chunk_consumer_evaluation"));
			REQUIRE(callbacks == 0);
		} else {
			REQUIRE(exported.IsSuccess());
			REQUIRE(callbacks == 1);
		}
		REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
		counter->value = 0;
		auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(op)));
		REQUIRE_NO_FAIL(*native);
		REQUIRE(native->RowCount() == 2);
		REQUIRE(native->GetValue(2, 0) == Value::BIGINT(combined ? 1 : 2));
		connection.Rollback();
		REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
		if (!combined) {
			auto text = exported.GetValue().query->ToString();
			auto ast = make_uniq<SelectStatement>();
			ast->node = std::move(exported.GetValue().query);
			for (bool direct_ast : {false, true}) {
				counter->value = 0;
				auto generated = direct_ast ? connection.Query(std::move(ast)) : connection.Query(text);
				REQUIRE_NO_FAIL(*generated);
				REQUIRE(SQLExportRows(*generated, true) == SQLExportRows(*native, true));
			}
		}
	}
}

TEST_CASE("Owned chunk SQL export accounts for intrinsic SINGLE join errors",
          "[sql_export][logical_plan_sql_export][chunk_sql_export]") {
	for (idx_t count : vector<idx_t> {STANDARD_VECTOR_SIZE, STANDARD_VECTOR_SIZE + STANDARD_VECTOR_SIZE / 2}) {
		for (bool combined : {false, true}) {
			for (bool error_on_multiple : {false, true}) {
				CAPTURE(count, combined, error_on_multiple);
				DuckDB db(nullptr);
				Connection connection(db);
				REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_streaming_buffer_size='1b'"));
				REQUIRE_NO_FAIL(connection.Query(string("SET scalar_subquery_error_on_multiple_rows=") +
				                                 (error_on_multiple ? "true" : "false")));
				auto first = connection.Query(combined ? "SELECT 0::BIGINT FROM range(" + to_string(count) + ")"
				                                       : "SELECT (i=" + to_string(count) + ")::BIGINT FROM range(" +
				                                             to_string(count + 1) + ")t(i)");
				REQUIRE_NO_FAIL(*first);
				auto left = first->TakeCollection();
				if (combined) {
					auto last = connection.Query("SELECT 1::BIGINT");
					REQUIRE_NO_FAIL(*last);
					auto part = last->TakeCollection();
					left->Combine(*part);
				}
				auto right = connection.Query("SELECT x::BIGINT FROM (VALUES(0),(1),(1))t(x)");
				REQUIRE_NO_FAIL(*right);
				connection.BeginTransaction();
				auto join = make_uniq<LogicalComparisonJoin>(JoinType::SINGLE);
				join->children.push_back(make_uniq<LogicalColumnDataGet>(
				    TableIndex(1001), vector<LogicalType> {LogicalType::BIGINT}, std::move(left)));
				join->children.push_back(make_uniq<LogicalColumnDataGet>(
				    TableIndex(1002), vector<LogicalType> {LogicalType::BIGINT}, right->TakeCollection()));
				join->conditions.emplace_back(
				    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT,
				                                        ColumnBinding(TableIndex(1001), ProjectionIndex(0))),
				    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT,
				                                        ColumnBinding(TableIndex(1002), ProjectionIndex(0))),
				    ExpressionType::COMPARE_EQUAL);
				join->ResolveOperatorTypes();
				REQUIRE_FALSE(join->conditions[0].GetLHS().CanThrow());
				REQUIRE_FALSE(join->conditions[0].GetRHS().CanThrow());
				auto exported = LogicalPlanSQLExporter::Export(*connection.context, *join);
				bool sensitive = combined && count != STANDARD_VECTOR_SIZE && error_on_multiple;
				if (sensitive) {
					REQUIRE(exported.HasError());
					REQUIRE(*exported.GetIssues()[0].construct ==
					        LogicalPlanVerificationConstructIdentity::ExportFeature("chunk_consumer_evaluation"));
				} else {
					REQUIRE(exported.IsSuccess());
				}
				QueryParameters parameters;
				parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
				auto drain = [&](unique_ptr<QueryResult> result) {
					idx_t rows = 0;
					while (!result->HasError()) {
						auto chunk = result->Fetch();
						if (!chunk) {
							break;
						}
						bool equal = true;
						for (idx_t i = 0; i < chunk->size(); i++) {
							auto expected = Value::BIGINT(rows + i == count ? 1 : 0);
							equal &= chunk->GetValue(0, i) == expected && chunk->GetValue(1, i) == expected;
						}
						REQUIRE(equal);
						rows += chunk->size();
					}
					REQUIRE(result->HasError() == error_on_multiple);
					if (error_on_multiple) {
						REQUIRE(result->GetErrorType() == ExceptionType::INVALID_INPUT);
						REQUIRE(StringUtil::Contains(result->GetError(), "More than one row returned"));
					} else {
						REQUIRE(rows == count + 1);
					}
					return rows;
				};
				REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
				auto native_rows =
				    drain(connection.context->Query(make_uniq<LogicalPlanStatement>(std::move(join)), parameters));
				connection.Rollback();
				REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
				if (sensitive) {
					REQUIRE(native_rows == count);
				} else {
					auto text_rows =
					    drain(connection.context->Query(exported.GetValue().query->ToString(), parameters));
					auto statement = make_uniq<SelectStatement>();
					statement->node = std::move(exported.GetValue().query);
					auto ast_rows = drain(connection.context->Query(std::move(statement), parameters));
					REQUIRE(text_rows == native_rows);
					REQUIRE(ast_rows == native_rows);
				}
				REQUIRE_NO_FAIL(connection.Query("SELECT 42"));
			}
		}
	}
}

TEST_CASE("Owned chunk SQL export preserves selected rows after a cross product",
          "[sql_export][logical_plan_sql_export][chunk_sql_export]") {
	for (bool combined : {false, true}) {
		for (idx_t consumer = 0; consumer < 3; consumer++) {
			CAPTURE(combined, consumer);
			DuckDB db(nullptr);
			Connection connection(db);
			REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET disabled_optimizers='compressed_materialization'"));
			auto first = connection.Query(combined ? "SELECT 0::BIGINT" : "SELECT * FROM range(2)");
			REQUIRE_NO_FAIL(*first);
			auto left = first->TakeCollection();
			if (combined) {
				auto last = connection.Query("SELECT 1::BIGINT");
				REQUIRE_NO_FAIL(*last);
				auto part = last->TakeCollection();
				left->Combine(*part);
			}
			auto right = connection.Query("SELECT range+10 FROM range(2)");
			REQUIRE_NO_FAIL(*right);
			connection.BeginTransaction();
			unique_ptr<LogicalOperator> plan = make_uniq<LogicalCrossProduct>(
			    make_uniq<LogicalColumnDataGet>(TableIndex(1001), vector<LogicalType> {LogicalType::BIGINT},
			                                    std::move(left)),
			    make_uniq<LogicalColumnDataGet>(TableIndex(1002), vector<LogicalType> {LogicalType::BIGINT},
			                                    right->TakeCollection()));
			plan->ResolveOperatorTypes();
			if (consumer == 2) {
				plan = OrderValues(std::move(plan));
			}
			if (consumer != 0) {
				auto limit = make_uniq<LogicalLimit>(BoundLimitNode::ConstantValue(2), BoundLimitNode());
				limit->children.push_back(std::move(plan));
				plan = std::move(limit);
				plan->ResolveOperatorTypes();
			}
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			bool sensitive = combined && consumer == 1;
			if (sensitive) {
				REQUIRE(exported.HasError());
				REQUIRE(*exported.GetIssues()[0].construct ==
				        LogicalPlanVerificationConstructIdentity::ExportFeature("chunk_consumer_evaluation"));
			} else {
				REQUIRE(exported.IsSuccess());
			}
			REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
			auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
			REQUIRE_NO_FAIL(*native);
			REQUIRE(native->RowCount() == (consumer == 0 ? 4 : 2));
			if (sensitive) {
				REQUIRE(CHECK_COLUMN(native, 0, {0, 0}));
				REQUIRE(CHECK_COLUMN(native, 1, {10, 11}));
			}
			REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=false"));
			if (!sensitive) {
				auto generated = connection.Query(exported.GetValue().query->ToString());
				auto statement = make_uniq<SelectStatement>();
				statement->node = std::move(exported.GetValue().query);
				auto ast = connection.Query(std::move(statement));
				REQUIRE_NO_FAIL(*generated);
				REQUIRE_NO_FAIL(*ast);
				REQUIRE(generated->GetTypes() == native->GetTypes());
				REQUIRE(ast->GetTypes() == native->GetTypes());
				REQUIRE(SQLExportRows(*generated, consumer != 0) == SQLExportRows(*native, consumer != 0));
				REQUIRE(SQLExportRows(*ast, consumer != 0) == SQLExportRows(*native, consumer != 0));
			}
			connection.Rollback();
		}
	}
}

TEST_CASE("Window SQL range origins survive copies and optional serialization fields",
          "[sql_export][logical_plan_sql_export][window_sql_export][serialization]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	const vector<string> queries {
	    "SELECT count(*) OVER (ORDER BY x RANGE BETWEEN 0 PRECEDING AND CURRENT ROW) FROM (VALUES (1),(2)) t(x)",
	    "SELECT count(*) OVER (ORDER BY x RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) "
	    "FROM (VALUES (DATE '2024-02-28'),(DATE '2024-02-29')) t(x)",
	    "SELECT count(*) OVER (ORDER BY CAST(x AS TIMESTAMP) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) "
	    "FROM (VALUES (DATE '2024-02-28'),(DATE '2024-02-29')) t(x)",
	    "SELECT count(*) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1),(2)) t(x)"};
	const vector<LogicalType> order_types {LogicalType::INTEGER, LogicalType::DATE, LogicalType::TIMESTAMP,
	                                       LogicalType::INTEGER};
	for (idx_t i = 0; i < queries.size(); i++) {
		for (idx_t route = 0; route < 3; route++) {
			CAPTURE(i, route);
			auto plan = OptimizeLogicalPlanExportQuery(connection, queries[i]);
			auto op = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_WINDOW);
			REQUIRE(op);
			if (route == 1) {
				op->expressions[0] = op->expressions[0]->Copy();
			} else if (route == 2) {
				plan = plan->Copy(*connection.context);
				op = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_WINDOW);
			}
			auto &window = op->expressions[0]->Cast<BoundWindowExpression>();
			REQUIRE(window.SQLRangeOrderType() == order_types[i]);
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			REQUIRE(exported.IsSuccess());
			window.RetainSQLRange(nullptr, nullptr, LogicalType::INVALID);
			auto legacy = plan->Copy(*connection.context);
			auto without_origin = LogicalPlanSQLExporter::Export(*connection.context, *legacy);
			if (i < 2) {
				REQUIRE(without_origin.HasError());
				REQUIRE(without_origin.GetIssues()[0].construct->identifier == "window_range_offset");
			} else {
				REQUIRE(without_origin.IsSuccess());
			}
		}
	}
	connection.Rollback();
}

TEST_CASE("Window SQL export rejects unrelated RANGE endpoints",
          "[sql_export][logical_plan_sql_export][window_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	auto plan = OptimizeLogicalPlanExportQuery(
	    connection, "SELECT sum(x) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) "
	                "FROM (VALUES (1),(2),(4)) t(x)");
	auto op = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_WINDOW);
	REQUIRE(op);
	auto &window = op->expressions[0]->Cast<BoundWindowExpression>();
	auto endpoint = window.StartExpr()->Copy();
	for (bool missing_offset : {false, true}) {
		window.StartExprMutable() = endpoint->Copy();
		auto &arithmetic = window.StartExprMutable()->Cast<BoundFunctionExpression>();
		if (missing_offset) {
			window.StartExprMutable() = window.OrderBy()[0].expression->Copy();
		} else {
			arithmetic.GetChildrenMutable()[0] = make_uniq<BoundConstantExpression>(Value::INTEGER(100));
		}
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		REQUIRE(exported.HasError());
		REQUIRE(exported.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
		REQUIRE(exported.GetIssues()[0].construct->identifier == "window_range_offset");
	}
	window.StartExprMutable() = endpoint->Copy();
	window.StartExprMutable()->Cast<BoundFunctionExpression>().GetChildrenMutable()[1] =
	    make_uniq<BoundConstantExpression>(Value::INTEGER(2));
	auto changed = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(changed.IsSuccess());
	auto generated = connection.Query(changed.GetValue().query->ToString());
	auto original = connection.Query("SELECT sum(x) OVER (ORDER BY x RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) "
	                                 "FROM (VALUES (1),(2),(4)) t(x)");
	REQUIRE_NO_FAIL(*generated);
	REQUIRE_NO_FAIL(*original);
	REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*original, false));
	connection.Rollback();
}

TEST_CASE("Window SQL export retains logical signatures and context boundaries",
          "[sql_export][logical_plan_sql_export][window_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT lead(x) OVER (ORDER BY x) FROM (VALUES(1),(2))t(x)");
	auto op = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_WINDOW);
	REQUIRE(op);
	auto &window = op->expressions[0]->Cast<BoundWindowExpression>();
	auto &function = *window.WindowFunctionMutable();
	const vector<LogicalType> arguments {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::INTEGER};
	REQUIRE(function.GetLogicalArguments() == arguments);
	REQUIRE(function.GetLogicalReturnType() == LogicalType::INTEGER);
	auto copy = plan->Copy(*connection.context);
	auto copied_op = FindLogicalPlanExportOperator(*copy, LogicalOperatorType::LOGICAL_WINDOW);
	REQUIRE(copied_op);
	auto &copied = *copied_op->expressions[0]->Cast<BoundWindowExpression>().WindowFunction();
	REQUIRE(copied.GetLogicalArguments() == arguments);
	REQUIRE(copied.GetLogicalReturnType() == LogicalType::INTEGER);
	auto binding = window.GetChildren()[0]->Cast<BoundColumnRefExpression>().Binding();
	BoundExpressionSQLExportContext context;
	context.resolve_binding = [binding](const ColumnBinding &candidate) -> optional<ResolvedSQLColumnReference> {
		if (candidate == binding) {
			return ResolvedSQLColumnReference {{Identifier("x")}, LogicalType::INTEGER};
		}
		return {};
	};
	auto standalone = BoundExpressionSQLExporter::Export(window, context);
	REQUIRE(standalone.HasError());
	REQUIRE(standalone.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION);
	LogicalPlanVerificationPath path;
	path.components.push_back({LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION, 0});
	auto exported = BoundExpressionSQLExporter::ExportWindowAtPath(window, context, path);
	REQUIRE(exported.IsSuccess());
	auto text = exported.GetValue()->ToString();
	function.GetArguments()[0] = LogicalType::VARCHAR;
	function.SetReturnType(LogicalType::VARCHAR);
	REQUIRE(function.GetLogicalArguments() == arguments);
	REQUIRE(function.GetLogicalReturnType() == LogicalType::INTEGER);
	auto retained = BoundExpressionSQLExporter::ExportWindowAtPath(window, context, path);
	REQUIRE(retained.IsSuccess());
	REQUIRE(retained.GetValue()->ToString() == text);
	auto result = connection.Query("SELECT " + text + " FROM (VALUES(1),(2))t(x)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {2, Value()}));
	window.GetChildrenMutable()[2] = make_uniq<BoundConstantExpression>(Value("lost logical argument"));
	auto changed = BoundExpressionSQLExporter::ExportWindowAtPath(window, context, path);
	REQUIRE(changed.IsSuccess());
	auto changed_result = connection.Query("SELECT " + changed.GetValue()->ToString() + " FROM (VALUES(1),(2))t(x)");
	REQUIRE(changed_result->HasError());
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export preserves partial CTE streams and errors",
          "[sql_export][logical_plan_sql_export][cte_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_streaming_buffer_size='1b'"));
	REQUIRE_NO_FAIL(connection.Query(
	    "CREATE TABLE cte_stream AS SELECT CASE WHEN i=4096 THEN 'bad' ELSE '1' END s FROM range(4097)t(i)"));
	for (bool producer : {false, true}) {
		for (bool finish : {false, true}) {
			CAPTURE(producer, finish);
			connection.BeginTransaction();
			auto plan = OptimizeLogicalPlanExportQuery(
			    connection, producer
			                    ? "WITH c AS MATERIALIZED (SELECT CAST(s AS INTEGER) x FROM cte_stream) SELECT x FROM c"
			                    : "WITH c AS MATERIALIZED (SELECT s FROM cte_stream) SELECT CAST(s AS INTEGER) FROM c");
			REQUIRE(FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_MATERIALIZED_CTE));
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			REQUIRE(exported.IsSuccess());
			QueryParameters parameters;
			parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
			auto check = [&](unique_ptr<QueryResult> result) {
				if (producer) {
					REQUIRE(result->HasError());
					REQUIRE(result->GetErrorType() == ExceptionType::CONVERSION);
					return idx_t(0);
				}
				REQUIRE_FALSE(result->HasError());
				REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
				idx_t count = 0;
				while (auto chunk = result->Fetch()) {
					REQUIRE(chunk->size() > 0);
					REQUIRE(chunk->GetValue(0, 0) == Value::INTEGER(1));
					count += chunk->size();
					if (!finish) {
						result->Cast<StreamQueryResult>().Close();
						break;
					}
				}
				REQUIRE(result->HasError() == finish);
				if (finish) {
					REQUIRE(result->GetErrorType() == ExceptionType::CONVERSION);
				}
				REQUIRE(count > 0);
				return count;
			};
			REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
			auto native_count =
			    check(connection.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), parameters));
			connection.Rollback();
			REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=false"));
			REQUIRE(check(connection.context->Query(exported.GetValue().query->ToString(), parameters)) ==
			        native_count);
			auto statement = make_uniq<SelectStatement>();
			statement->node = std::move(exported.GetValue().query);
			REQUIRE(check(connection.context->Query(std::move(statement), parameters)) == native_count);
			REQUIRE_NO_FAIL(connection.Query("SELECT 42"));
		}
	}
}

TEST_CASE("Logical plan SQL export rejects recursive wrappers when inlining is unavailable",
          "[sql_export][logical_plan_sql_export][recursive_cte_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_execution_time=5000"));
	connection.BeginTransaction();
	auto plan = OptimizeLogicalPlanExportQuery(
	    connection, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<3) SELECT x FROM c LIMIT 1");
	REQUIRE(FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_RECURSIVE_CTE));
	REQUIRE_FALSE(FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_MATERIALIZED_CTE));
	REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='cte_inlining'"));
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.HasError());
	REQUIRE(*exported.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("recursive_cte_materialization"));
	REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
	auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
	REQUIRE_NO_FAIL(*native);
	REQUIRE(CHECK_COLUMN(native, 0, {1}));
	connection.Rollback();
}

TEST_CASE("Copied UNION SQL outlives its plan and original exported AST",
          "[sql_export][logical_plan_sql_export][set_operation_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_execution_time=5000; CREATE SEQUENCE seq"));
	connection.BeginTransaction();
	auto plan = OptimizeLogicalPlanExportQuery(connection,
	                                           "SELECT x,nextval('seq') y FROM (SELECT nextval('seq') x FROM range(2) "
	                                           "UNION ALL SELECT 0 WHERE false LIMIT 1)t");
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.IsSuccess());
	auto statement = make_uniq<SelectStatement>();
	statement->node = exported.GetValue().query->Copy();
	plan.reset();
	exported.GetValue().query.reset();
	auto result = connection.Query(std::move(statement));
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(result->GetValue(1, 0) == Value::BIGINT(3));
	connection.Rollback();
}

namespace {

void RequireDecorrelatedSQLExportInput(LogicalOperator &op) {
	REQUIRE(op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN);
	REQUIRE(op.type != LogicalOperatorType::LOGICAL_DELIM_GET);
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expression) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    **expression, [&](const BoundColumnRefExpression &ref) { REQUIRE(ref.Depth() == 0); });
	});
	for (auto &child : op.children) {
		RequireDecorrelatedSQLExportInput(*child);
	}
}

} // namespace

TEST_CASE("Grouped MARK SQL export rejects inconsistent group metadata",
          "[sql_export][logical_plan_sql_export][join_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_execution_time=5000"));
	connection.BeginTransaction();
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE list_l(g INTEGER,x INTEGER[]); "
	                                 "CREATE TABLE list_r(g INTEGER,y INTEGER[]); "
	                                 "INSERT INTO list_l VALUES (1,[10,NULL]); INSERT INTO list_r VALUES (1,NULL)"));
	auto plan =
	    OptimizeLogicalPlanExportQuery(connection, "SELECT x=ANY(SELECT y FROM list_r r WHERE r.g=l.g) FROM list_l l");
	auto &list_join = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_COMPARISON_JOIN)
	                      ->Cast<LogicalComparisonJoin>();
	auto &list_condition = list_join.conditions.back();
	list_condition =
	    JoinCondition(list_condition.GetLHS().Copy(), list_condition.GetRHS().Copy(), ExpressionType::COMPARE_LESSTHAN);
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.HasError());
	REQUIRE(exported.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("mark_condition_semantics"));

	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE collated_l(g VARCHAR COLLATE nocase,x INTEGER); "
	                                 "CREATE TABLE collated_r(g VARCHAR COLLATE nocase,y INTEGER); "
	                                 "INSERT INTO collated_l VALUES ('A',1); INSERT INTO collated_r VALUES ('a',1)"));
	plan = OptimizeLogicalPlanExportQuery(connection,
	                                      "SELECT x=ANY(SELECT y FROM collated_r r WHERE r.g=l.g) FROM collated_l l");
	auto &collated_join = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_COMPARISON_JOIN)
	                          ->Cast<LogicalComparisonJoin>();
	REQUIRE(collated_join.mark_types.size() == 1);
	REQUIRE(StringType::GetCollation(collated_join.mark_types[0]) == "nocase");
	collated_join.mark_types[0] = LogicalType::VARCHAR;
	exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.HasError());
	REQUIRE(exported.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("mark_condition_semantics"));
	for (auto &condition : collated_join.conditions) {
		condition = JoinCondition(condition.GetLHS().Copy(), condition.GetRHS().Copy(), ExpressionType::COMPARE_EQUAL);
	}
	exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.HasError());
	REQUIRE(exported.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("mark_group_null_semantics"));
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export rejects inconsistent sampling metadata",
          "[sql_export][logical_plan_sql_export][sample_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	for (idx_t variant = 0; variant < 2; variant++) {
		auto plan =
		    OptimizeLogicalPlanExportQuery(connection, "SELECT * FROM range(10000) USING SAMPLE 31 (reservoir,42)");
		auto sample_op = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_SAMPLE);
		REQUIRE(sample_op);
		auto &sample = sample_op->Cast<LogicalSample>();
		string feature;
		if (variant == 0) {
			sample.sample_options->repeatable = false;
			feature = "sample_repeatability";
		} else {
			sample.sample_options->SetSeed(idx_t(NumericLimits<int64_t>::Maximum()) + 1);
			feature = "sample_seed";
		}
		for (bool binary : {false, true}) {
			CAPTURE(variant, binary);
			auto copy = binary ? sample.Copy(*connection.context) : nullptr;
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, copy ? *copy : sample);
			REQUIRE(exported.HasError());
			REQUIRE(exported.IsValid());
			REQUIRE(exported.GetIssues()[0].phase == LogicalPlanVerificationPhase::PLAN_EXPORT);
			REQUIRE(exported.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
			REQUIRE(exported.GetIssues()[0].construct ==
			        LogicalPlanVerificationConstructIdentity::ExportFeature(feature));
			if (variant == 1) {
				// Native binary serialization stores a signed seed.
				break;
			}
		}
	}
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export discards extracted scan order hints",
          "[sql_export][logical_plan_sql_export][sample_sql_export][table_source_sql]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE sample_source AS SELECT i FROM range(10000) t(i)"));
	connection.BeginTransaction();

	auto ordered_plan =
	    OptimizeLogicalPlanExportQuery(connection, "SELECT i FROM sample_source ORDER BY i DESC LIMIT 5");
	auto &ordered_get =
	    FindLogicalPlanExportOperator(*ordered_plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
	REQUIRE(ordered_get.row_group_order_options);
	for (bool binary : {false, true}) {
		auto copy = binary ? ordered_get.Copy(*connection.context) : nullptr;
		auto ordered_export = LogicalPlanSQLExporter::Export(*connection.context, copy ? *copy : ordered_get);
		INFO((ordered_export.HasError() ? ordered_export.GetIssues()[0].message : string()));
		REQUIRE(ordered_export.IsSuccess());
		auto expected = connection.Query("SELECT i FROM sample_source");
		auto actual = connection.Query(ordered_export.GetValue().query->ToString());
		REQUIRE_NO_FAIL(*expected);
		REQUIRE_NO_FAIL(*actual);
		REQUIRE(SQLExportRows(*actual, true) == SQLExportRows(*expected, true));
	}
	connection.Rollback();
}

TEST_CASE("SQL export retains unpruned offsets through LIMIT and TopN copies",
          "[sql_export][logical_plan_sql_export][table_source_sql]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE offset_rows AS SELECT i::INTEGER AS i FROM range(245760) t(i)"));
	REQUIRE_NO_FAIL(connection.Query("CHECKPOINT"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO offset_rows VALUES (-1)"));
	REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='statistics_propagation'"));
	connection.BeginTransaction();
	for (idx_t offset : {idx_t(1), idx_t(122880), idx_t(200000)}) {
		auto sql = string("SELECT i FROM offset_rows ORDER BY i ") + (offset == 200000 ? "" : "LIMIT 1 ") + "OFFSET " +
		           to_string(offset);
		CAPTURE(sql);
		auto original = OptimizeLogicalPlanExportQuery(connection, sql);
		for (bool binary : {false, true}) {
			auto copy = binary ? original->Copy(*connection.context) : nullptr;
			auto &plan = copy ? *copy : *original;
			auto &get = FindLogicalPlanExportOperator(plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
			REQUIRE(get.row_group_order_options);
			REQUIRE(get.row_group_order_options->row_group_offset > 0);
			auto top_n = FindLogicalPlanExportOperator(plan, LogicalOperatorType::LOGICAL_TOP_N);
			auto limit = FindLogicalPlanExportOperator(plan, LogicalOperatorType::LOGICAL_LIMIT);
			REQUIRE((top_n || limit));
			REQUIRE(bool(top_n) == (offset == 1));
			auto &retained =
			    top_n ? top_n->Cast<LogicalTopN>().unpruned_offset : limit->Cast<LogicalLimit>().unpruned_offset;
			REQUIRE(retained.IsValid());
			REQUIRE(retained.GetIndex() == offset);
			auto reduced =
			    top_n ? top_n->Cast<LogicalTopN>().offset : limit->Cast<LogicalLimit>().offset_val.GetConstantValue();
			REQUIRE(reduced < offset);
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, plan);
			REQUIRE(exported.IsSuccess());
			if (binary) {
				retained.SetInvalid();
				auto missing = LogicalPlanSQLExporter::Export(*connection.context, plan);
				REQUIRE(missing.HasError());
				REQUIRE(missing.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
				REQUIRE(missing.GetIssues()[0].construct->identifier == "pruned_offset");
			}
		}
	}
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export retains sampling errors and partial consumption",
          "[sql_export][logical_plan_sql_export][sample_sql_export]") {
	for (const auto &clause : {"31 (reservoir,42)", "50% (bernoulli,42)", "50% (system,42)"}) {
		for (idx_t consumption = 0; consumption < 3; consumption++) {
			for (bool late_error : {false, true}) {
				auto sql = string("SELECT * FROM (SELECT i,nextval('seq') v,") +
				           (late_error ? "CASE WHEN i<8192 THEN i ELSE error('sample input reached') END" : "i") +
				           " e FROM range(10000)t(i)) USING SAMPLE " + clause + (consumption == 1 ? " LIMIT 1" : "");
				vector<string> expected_rows;
				Value expected_sequence;
				bool expected_error = false;
				for (idx_t route = 0; route < 5; route++) {
					CAPTURE(clause, consumption, late_error, route);
					DuckDB db(nullptr);
					Connection connection(db);
					REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_execution_time=5000; "
					                                 "SET max_streaming_buffer_size='1b'; CREATE SEQUENCE seq"));
					connection.BeginTransaction();
					auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
					if (route == 2) {
						plan = plan->Copy(*connection.context);
						plan->ResolveOperatorTypes();
					}
					auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
					REQUIRE(exported.IsSuccess());
					QueryParameters parameters;
					parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
					unique_ptr<QueryResult> result;
					if (route == 1 || route == 2) {
						REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
						result =
						    connection.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), parameters);
					} else if (route == 0) {
						result = connection.context->Query(sql, parameters);
					} else if (route == 3) {
						result = connection.context->Query(exported.GetValue().query->ToString(), parameters);
					} else {
						auto statement = make_uniq<SelectStatement>();
						statement->node = std::move(exported.GetValue().query);
						result = connection.context->Query(std::move(statement), parameters);
					}
					vector<string> rows;
					while (!result->HasError()) {
						auto chunk = result->Fetch();
						if (!chunk) {
							break;
						}
						for (idx_t row = 0; row < chunk->size(); row++) {
							rows.push_back(chunk->GetValue(0, row).ToString() + ":" +
							               chunk->GetValue(1, row).ToString() + ":" +
							               chunk->GetValue(2, row).ToString());
						}
						if (consumption == 2) {
							break;
						}
					}
					auto has_error = result->HasError();
					if (has_error) {
						REQUIRE(late_error);
						REQUIRE(StringUtil::Contains(result->GetError(), "sample input reached"));
					}
					result.reset();
					connection.Rollback();
					auto sequence = connection.Query("SELECT currval('seq')");
					REQUIRE_NO_FAIL(*sequence);
					if (route == 0) {
						expected_rows = std::move(rows);
						expected_sequence = sequence->GetValue(0, 0);
						expected_error = has_error;
					} else {
						REQUIRE(rows == expected_rows);
						REQUIRE(has_error == expected_error);
						REQUIRE(sequence->GetValue(0, 0) == expected_sequence);
					}
				}
			}
		}
	}
}

TEST_CASE("Table row number SQL export retains stream effects",
          "[sql_export][logical_plan_sql_export][table_row_number_sql_export]") {
	for (auto consumption : {0, 1, 2}) {
		for (bool late_error : {false, true}) {
			vector<string> expected_rows;
			Value expected_sequence;
			bool expected_error = false;
			for (idx_t route = 0; route < 6; route++) {
				CAPTURE(consumption, late_error, route);
				DuckDB db(nullptr);
				Connection connection(db);
				REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_execution_time=5000; "
				                                 "SET max_streaming_buffer_size='1b'; CREATE SEQUENCE seq; "
				                                 "CREATE TABLE stream_numbers AS SELECT i FROM range(10000)t(i); "
				                                 "DELETE FROM stream_numbers WHERE i%5=1"));
				connection.BeginTransaction();
				auto sql = string("SELECT i,n,nextval('seq'),") +
				           (late_error ? "CASE WHEN n>4096 THEN error('row number stream') ELSE 'ok' END" : "'ok'") +
				           " FROM (SELECT i,row_number() OVER () n FROM stream_numbers)" +
				           (consumption == 1 ? " LIMIT 1" : "");
				auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
				if (route == 2 || route == 4) {
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
				}
				auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
				REQUIRE(exported.IsSuccess());
				QueryParameters parameters;
				parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
				unique_ptr<QueryResult> result;
				if (route == 1 || route == 2) {
					REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
					result = connection.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), parameters);
				} else {
					plan.reset();
					if (route == 0) {
						result = connection.context->Query(sql, parameters);
					} else if (route == 3 || route == 4) {
						result = connection.context->Query(exported.GetValue().query->ToString(), parameters);
					} else {
						auto statement = make_uniq<SelectStatement>();
						statement->node = std::move(exported.GetValue().query);
						result = connection.context->Query(std::move(statement), parameters);
					}
				}
				vector<string> rows;
				while (!result->HasError()) {
					auto chunk = result->Fetch();
					if (!chunk) {
						break;
					}
					for (idx_t row = 0; row < chunk->size(); row++) {
						string value;
						for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
							value += chunk->GetValue(col, row).ToSQLString() + "|";
						}
						rows.push_back(std::move(value));
					}
					if (consumption == 2) {
						break;
					}
				}
				auto has_error = result->HasError();
				if (has_error) {
					REQUIRE(StringUtil::Contains(result->GetError(), "row number stream"));
				}
				result.reset();
				connection.Rollback();
				auto sequence = connection.Query("SELECT last_value FROM duckdb_sequences() WHERE sequence_name='seq'");
				REQUIRE_NO_FAIL(*sequence);
				if (route == 0) {
					expected_rows = std::move(rows);
					expected_sequence = sequence->GetValue(0, 0);
					expected_error = has_error;
				} else {
					REQUIRE(rows == expected_rows);
					REQUIRE(has_error == expected_error);
					REQUIRE(Value::NotDistinctFrom(sequence->GetValue(0, 0), expected_sequence));
				}
			}
		}
	}
}

TEST_CASE("Table row number SQL export guards filtered numbering",
          "[sql_export][logical_plan_sql_export][table_row_number_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE filtered_numbers AS SELECT i FROM range(100)t(i)"));
	connection.BeginTransaction();
	for (bool copy : {false, true}) {
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT i,row_number() OVER () FROM filtered_numbers");
		auto &get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
		auto filtered = OptimizeLogicalPlanExportQuery(connection, "SELECT i FROM filtered_numbers WHERE i>50");
		auto &filtered_get =
		    FindLogicalPlanExportOperator(*filtered, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
		REQUIRE(filtered_get.table_filters.HasFilters());
		get.table_filters = std::move(filtered_get.table_filters);
		if (copy) {
			plan = plan->Copy(*connection.context);
			plan->ResolveOperatorTypes();
		}
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		REQUIRE(exported.HasError());
		REQUIRE(exported.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
		REQUIRE(exported.GetIssues()[0].construct->function->name == "seq_scan");
		REQUIRE(exported.GetIssues()[0].facts.size() == 1);
		REQUIRE((exported.GetIssues()[0].facts[0] == pair<string, Value> {"guard", Value("row_number_with_filters")}));
		REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
		auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
		REQUIRE_NO_FAIL(*native);
		REQUIRE(native->RowCount() == 49);
		REQUIRE(native->GetValue(0, 0) == Value::BIGINT(51));
		REQUIRE(native->GetValue(1, 0) == Value::BIGINT(1));
		REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=false"));
		auto outside =
		    connection.Query("SELECT i,n FROM (SELECT i,row_number() OVER () n FROM filtered_numbers) WHERE i>50");
		REQUIRE_NO_FAIL(*outside);
		REQUIRE(outside->GetValue(1, 0) == Value::BIGINT(52));
	}
	auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT i,row_number() OVER () FROM filtered_numbers");
	auto &get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
	get.dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
	REQUIRE_FALSE(get.function.to_sql(*connection.context, get, nullptr, Identifier("scan")).query);
	connection.Rollback();
}

TEST_CASE("Recursive payload aggregates resolve qualified schemas", "[sql_export][recursive_cte_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE SCHEMA payload_schema; CREATE SCHEMA outer_schema; "
	                                 "CREATE SCHEMA outer_schema.inner_schema"));
	connection.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*connection.context, Identifier("memory"));
	MetaTransaction::Get(*connection.context)
	    .ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::CREATE_CATALOG_ENTRY);
	for (idx_t schema = 0; schema < 3; schema++) {
		auto &entry = Catalog::GetEntry<AggregateFunctionCatalogEntry>(
		    *connection.context, QualifiedName("system", "main", schema == 0 ? "min" : "max"));
		auto functions = entry.functions;
		functions.SetName("payload_choice");
		functions.ApplyToFunctions([](AggregateFunction &function) { function.SetName("payload_choice"); });
		CreateAggregateFunctionInfo info(std::move(functions));
		info.SetQualifiedName(
		    schema == 2 ? QualifiedName(vector<Identifier> {"memory", "outer_schema", "inner_schema"}, "payload_choice")
		                : QualifiedName("memory", schema == 0 ? "main" : "payload_schema", "payload_choice"));
		info.internal = false;
		catalog.CreateFunction(*connection.context, info);
	}
	connection.Commit();
	REQUIRE_NO_FAIL(connection.Query("SET search_path='payload_schema,main'"));
	for (auto name : {"memory.main.payload_choice", "memory.payload_schema.payload_choice",
	                  "memory.outer_schema.inner_schema.payload_choice", "payload_schema.payload_choice",
	                  "payload_choice", "outer_schema.inner_schema.payload_choice", "memory.payload_choice"}) {
		CAPTURE(name);
		auto expected = string(name) == "memory.main.payload_choice" || string(name) == "memory.payload_choice" ? 1 : 3;
		auto ordinary = connection.Query("SELECT " + string(name) + "(v) FROM (VALUES(1),(3))t(v)");
		REQUIRE_NO_FAIL(*ordinary);
		REQUIRE(ordinary->GetValue(0, 0) == Value::INTEGER(expected));
		auto recursive = connection.Query("WITH RECURSIVE r(k,v) USING KEY(k," + string(name) +
		                                  "(v)) AS (SELECT * FROM (VALUES(1,1),(1,3))t(k,v) UNION ALL "
		                                  "SELECT k+1,v FROM r WHERE k<2) SELECT k,v FROM r ORDER BY k");
		REQUIRE_NO_FAIL(*recursive);
		REQUIRE(recursive->RowCount() == 2);
		REQUIRE(recursive->GetValue(1, 0) == Value::INTEGER(expected));
		REQUIRE(recursive->GetValue(1, 1) == Value::INTEGER(expected));
	}
}

TEST_CASE("Table SQL export distinguishes pruning hints from row filters",
          "[sql_export][logical_plan_sql_export][table_row_number_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE hinted_scan AS SELECT i,i%7 v FROM range(100)t(i)"));
	connection.BeginTransaction();
	for (bool numbered : {false, true}) {
		for (bool binary : {false, true}) {
			auto sql = string("SELECT i,v") + (numbered ? ",row_number() OVER ()" : "") + " FROM hinted_scan";
			auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
			auto &get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
			auto donor = OptimizeLogicalPlanExportQuery(connection, "SELECT i,v FROM hinted_scan WHERE i>v");
			auto &donor_get =
			    FindLogicalPlanExportOperator(*donor, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
			REQUIRE(donor_get.table_filters.HasMultiColumnFilters());
			REQUIRE_FALSE(donor_get.table_filters.HasFilters());
			get.table_filters = std::move(donor_get.table_filters);
			if (binary) {
				plan = plan->Copy(*connection.context);
				plan->ResolveOperatorTypes();
			}
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			if (!numbered) {
				REQUIRE(exported.IsSuccess());
			} else {
				REQUIRE(exported.HasError());
				REQUIRE(exported.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
				REQUIRE((exported.GetIssues()[0].facts[0] ==
				         pair<string, Value> {"guard", Value("row_number_with_filters")}));
			}
			REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
			auto native = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
			REQUIRE_NO_FAIL(*native);
			REQUIRE(native->RowCount() == 100);
			REQUIRE(native->GetValue(0, 0) == Value::BIGINT(0));
			if (numbered) {
				REQUIRE(native->GetValue(2, 0) == Value::BIGINT(1));
			}
			REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=false"));
			if (!numbered) {
				auto generated = connection.Query(exported.GetValue().query->ToString());
				REQUIRE_NO_FAIL(*generated);
				REQUIRE(generated->GetTypes() == native->GetTypes());
				REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*native, false));
			}
		}
	}
	auto file_filtered = OptimizeLogicalPlanExportQuery(connection, "SELECT i,v FROM hinted_scan");
	auto &file_filtered_get =
	    FindLogicalPlanExportOperator(*file_filtered, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
	file_filtered_get.extra_info.file_filters = "i > 50";
	auto file_filtered_export = LogicalPlanSQLExporter::Export(*connection.context, *file_filtered);
	REQUIRE(file_filtered_export.HasError());
	REQUIRE(file_filtered_export.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("file_filter_residual"));
	REQUIRE_NO_FAIL(connection.Query("CREATE SEQUENCE hint_calls"));
	for (const auto &sql : {"SELECT i,v FROM hinted_scan WHERE i>v AND nextval('hint_calls')>0",
	                        "SELECT i,v FROM hinted_scan WHERE i<v"}) {
		auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
		auto &get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
		auto donor = OptimizeLogicalPlanExportQuery(connection, "SELECT i,v FROM hinted_scan WHERE i>v");
		auto &donor_get = FindLogicalPlanExportOperator(*donor, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
		get.table_filters = std::move(donor_get.table_filters);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		REQUIRE(exported.IsSuccess());
		auto generated = connection.Query(exported.GetValue().query->ToString());
		auto fresh = connection.Query(sql);
		REQUIRE_NO_FAIL(*generated);
		REQUIRE_NO_FAIL(*fresh);
		REQUIRE(generated->GetTypes() == fresh->GetTypes());
		REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*fresh, false));
	}
	connection.Rollback();
}

static void SQLExportCompressionNameProbe(DataChunk &input, ExpressionState &, Vector &result) {
	result.Reference(Value::BIGINT(99), count_t(input.size()));
}

TEST_CASE("SQL export preserves ordinary functions with compression-like names",
          "[sql_export][logical_plan_sql_export][compressed_materialization]") {
	DuckDB db(nullptr);
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "sql_export_compression_name_probe");
	loader.UseDefaultSchema();
	for (auto name : {"__internal_compress_integral_probe", "__internal_decompress_integral_probe"}) {
		loader.RegisterFunction(
		    ScalarFunction(name, {LogicalType::BIGINT}, LogicalType::BIGINT, SQLExportCompressionNameProbe));
	}
	connection.BeginTransaction();
	for (auto name : {"__internal_compress_integral_probe", "__internal_decompress_integral_probe"}) {
		for (bool binary_copy : {false, true}) {
			CAPTURE(name, binary_copy);
			auto sql = string("SELECT ") + name + "(i) FROM range(2)t(i)";
			auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
			if (binary_copy) {
				plan = plan->Copy(*connection.context);
			}
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
			INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
			REQUIRE(exported.IsSuccess());
			auto generated_sql = exported.GetValue().query->ToString();
			REQUIRE(StringUtil::Contains(generated_sql, name));
			auto direct = connection.Query(sql);
			auto generated = connection.Query(generated_sql);
			REQUIRE_NO_FAIL(*direct);
			REQUIRE_NO_FAIL(*generated);
			REQUIRE(CHECK_COLUMN(direct, 0, {99, 99}));
			REQUIRE(generated->GetTypes() == direct->GetTypes());
			REQUIRE(SQLExportRows(*generated, true) == SQLExportRows(*direct, true));
		}
	}
	connection.Rollback();
}

TEST_CASE("Grouped MARK SQL export preserves stream effects and late errors",
          "[sql_export][logical_plan_sql_export][join_sql_export]") {
	for (bool throwing : {false, true}) {
		for (idx_t consumption = 0; consumption < 3; consumption++) {
			vector<string> expected_rows;
			vector<idx_t> expected_chunks;
			Value expected_sequence;
			bool expected_error = false;
			for (idx_t route = 0; route < 3; route++) {
				CAPTURE(throwing, consumption, route);
				DuckDB db(nullptr);
				Connection connection(db);
				REQUIRE_NO_FAIL(connection.Query(
				    "SET threads=1; SET max_execution_time=5000; SET max_streaming_buffer_size='1b'; CREATE SEQUENCE "
				    "seq; "
				    "CREATE TABLE l AS SELECT i,(i%5)::DOUBLE g,CASE WHEN i%11=0 THEN NULL ELSE i%13 END x "
				    "FROM range(8192)t(i); CREATE TABLE r AS SELECT (i%4)::DOUBLE g, "
				    "CASE WHEN i%7=0 THEN NULL ELSE i%13 END y FROM range(64)t(i)"));
				connection.BeginTransaction();
				string value = throwing ? "CASE WHEN i>=4096 THEN CAST(error('grouped mark stream') AS BIGINT) "
				                          "ELSE nextval('seq') END"
				                        : "nextval('seq')";
				auto sql = "SELECT i," + value + ",x=ANY(SELECT y FROM r WHERE r.g=l.g) m FROM l";
				if (consumption == 1) {
					sql += " LIMIT 1";
				}
				auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
				RequireDecorrelatedSQLExportInput(*plan);
				auto &join = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_COMPARISON_JOIN)
				                 ->Cast<LogicalComparisonJoin>();
				REQUIRE(join.join_type == JoinType::MARK);
				REQUIRE(join.mark_types.size() == 1);
				REQUIRE(join.conditions.size() == 2);
				if (route == 2) {
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
				}
				auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
				RequireMarkConditionRejection(exported);
				QueryParameters parameters;
				parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
				unique_ptr<QueryResult> result;
				if (route == 1 || route == 2) {
					REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
					result = connection.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), parameters);
				} else {
					plan.reset();
					result = connection.context->Query(sql, parameters);
				}
				vector<string> rows;
				vector<idx_t> chunks;
				while (!result->HasError()) {
					auto chunk = result->Fetch();
					if (!chunk) {
						break;
					}
					chunks.push_back(chunk->size());
					for (idx_t row = 0; row < chunk->size(); row++) {
						string text;
						for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
							text += chunk->GetValue(col, row).ToSQLString() + "|";
						}
						rows.push_back(std::move(text));
					}
					if (consumption == 2) {
						break;
					}
				}
				auto has_error = result->HasError();
				if (has_error) {
					REQUIRE(StringUtil::Contains(result->GetError(), "grouped mark stream"));
				}
				result.reset();
				connection.Rollback();
				auto sequence = connection.Query("SELECT last_value FROM duckdb_sequences() WHERE sequence_name='seq'");
				REQUIRE_NO_FAIL(*sequence);
				if (route == 0) {
					expected_rows = std::move(rows);
					expected_chunks = std::move(chunks);
					expected_sequence = sequence->GetValue(0, 0);
					expected_error = has_error;
				} else {
					REQUIRE(rows == expected_rows);
					REQUIRE(chunks == expected_chunks);
					REQUIRE(has_error == expected_error);
					REQUIRE(Value::NotDistinctFrom(sequence->GetValue(0, 0), expected_sequence));
				}
			}
			REQUIRE(expected_error == (throwing && consumption == 0));
			REQUIRE(expected_rows.size() == (consumption == 1 ? 1 : consumption == 2 ? 2048 : throwing ? 4096 : 8192));
		}
	}
}

TEST_CASE("Logical plan SQL export preserves secure view casts through repeated pruning",
          "[sql_export][logical_plan_sql_export][secure_view_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE SCHEMA secret"));
	REQUIRE_NO_FAIL(connection.Query(
	    "CREATE TABLE secret.path_data(s STRUCT(a INTEGER, b STRUCT(c VARCHAR, d VARCHAR)), t INTEGER)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO secret.path_data VALUES ({'a':1,'b':{'c':'bad','d':'2'}},10), "
	                                 "({'a':NULL,'b':{'c':'3','d':NULL}},20), (NULL,30)"));
	REQUIRE_NO_FAIL(
	    connection.Query("CREATE SECURE VIEW secret.secure_paths(first, second) AS SELECT s, t FROM secret.path_data"));
	REQUIRE_NO_FAIL(
	    connection.Query("CREATE SECURE VIEW secret.secure_paths_twice AS SELECT * FROM secret.secure_paths"));
	connection.BeginTransaction();

	const string sql = "SELECT (first.b::STRUCT(c BIGINT,d BIGINT)).d FROM secret.secure_paths_twice ORDER BY second";
	auto plan = OptimizeLogicalPlanExportQueryWithRepeatedPruning(connection, sql);
	REQUIRE(FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_SECURE_VIEW));
	auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(exported.IsSuccess());
	auto text = exported.GetValue().query->ToString();
	plan.reset();
	connection.Rollback();
	auto direct = connection.Query(sql);
	auto generated = connection.Query(text);
	INFO(direct->GetError());
	INFO(generated->GetError());
	REQUIRE(direct->HasError());
	REQUIRE(generated->HasError());
	REQUIRE(StringUtil::Contains(direct->GetError(), "bad"));
	REQUIRE(StringUtil::Contains(generated->GetError(), "bad"));
	auto statement = make_uniq<SelectStatement>();
	statement->node = exported.GetValue().query->Copy();
	auto ast = connection.Query(std::move(statement));
	REQUIRE(ast->HasError());
	REQUIRE(StringUtil::Contains(ast->GetError(), "bad"));
}

TEST_CASE("Logical plan SQL export rejects incomplete secure view metadata",
          "[sql_export][logical_plan_sql_export][secure_view_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);

	SECTION("legacy display-only node") {
		auto view = make_uniq<LogicalSecureView>("legacy", IntegerValues(TableIndex(8400), {{1}}));
		view->ResolveOperatorTypes();
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *view);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	}
	SECTION("incomplete caller predicates") {
		REQUIRE_NO_FAIL(connection.Query("CREATE TABLE source_data(a INTEGER, b INTEGER)"));
		REQUIRE_NO_FAIL(connection.Query("INSERT INTO source_data VALUES (1,2),(2,3),(NULL,4)"));
		REQUIRE_NO_FAIL(connection.Query("CREATE SECURE VIEW secure_data AS SELECT a,b FROM source_data"));
		connection.BeginTransaction();
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT b FROM secure_data WHERE a=1");
		auto &view =
		    FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_SECURE_VIEW)->Cast<LogicalSecureView>();
		REQUIRE(view.source_filters.size() == 1);
		REQUIRE(view.pushed_filters.size() == 1);
		REQUIRE(LogicalPlanSQLExporter::Export(*connection.context, *plan).IsSuccess());
		view.source_filters[0].reset();
		auto missing_mapping = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		REQUIRE(missing_mapping.HasError());
		REQUIRE(*missing_mapping.GetIssues()[0].construct->identifier == "secure_view_filter");
		view.source_filters.clear();
		auto missing_predicate = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		REQUIRE(missing_predicate.HasError());
		REQUIRE(*missing_predicate.GetIssues()[0].construct->identifier == "secure_view_filter");
		connection.Rollback();
	}

	SECTION("incomplete current mapping") {
		REQUIRE_NO_FAIL(connection.Query("CREATE TABLE source_data(a INTEGER, b INTEGER)"));
		REQUIRE_NO_FAIL(connection.Query("CREATE SECURE VIEW secure_data AS SELECT a,b FROM source_data"));
		connection.BeginTransaction();
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT b FROM secure_data");
		auto &view =
		    FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_SECURE_VIEW)->Cast<LogicalSecureView>();
		REQUIRE_FALSE(view.output_expressions.empty());
		view.output_expressions.pop_back();
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE,
		                       {LogicalPlanVerificationPathRoot::LOGICAL_PLAN,
		                        {{LogicalPlanVerificationPathComponentType::OPERATOR_CHILD, 0}}});
		connection.Rollback();
	}
}
