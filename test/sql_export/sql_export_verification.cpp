#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/sql_export_verification.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"

using namespace duckdb;

namespace {

SQLExportVerificationRecord TakeSQLExportRecord(SQLExportVerificationState &observer) {
	auto records = observer.TakeRecords();
	REQUIRE(records.size() == 1);
	return std::move(records[0]);
}

void SetSQLExportMode(Connection &con, SQLExportVerificationState &observer, const string &mode) {
	REQUIRE_NO_FAIL(con.Query("SET debug_verify_sql_export='" + mode + "'"));
	observer.TakeRecords();
}

void RegisterSQLExportOpaqueSource(DuckDB &db, Connection &con) {
	ExtensionLoader loader(*db.instance, "sql_export_opaque_source");
	auto &range = loader.GetTableFunction("range");
	auto function = *range.functions.GetFunctionByArguments(*con.context, {LogicalType::BIGINT});
	function.name = Identifier("sql_export_opaque_source");
	function.to_sql = [](ClientContext &, const LogicalGet &, unique_ptr<TableRef>,
	                     const Identifier &) -> TableFunctionToSQLResult {
		return {nullptr, "test_opaque"};
	};
	loader.RegisterFunction(std::move(function));
}

struct SQLExportOptimizerCounter : public OptimizerExtensionInfo {
	idx_t calls = 0;
	bool throw_on_second = false;
	bool throw_on_first = false;
	bool throw_on_third = false;
};

void CountSQLExportOptimization(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &) {
	auto &counter = static_cast<SQLExportOptimizerCounter &>(*input.info);
	counter.calls++;
	if (counter.throw_on_third && counter.calls == 3) {
		throw BinderException(QueryLocation(0, 6), "Final original optimizer hook failure");
	}
	if ((counter.throw_on_second && counter.calls == 2) || (counter.throw_on_first && counter.calls == 1)) {
		throw BinderException("Generated optimizer hook failure");
	}
}

} // namespace

TEST_CASE("SQL export modes classify unsupported plans and preserve off", "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterSQLExportOpaqueSource(db, con);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	REQUIRE_NO_FAIL(con.Query("VALUES (1)"));
	REQUIRE(observer->TakeRecords().empty());

	SetSQLExportMode(con, *observer, "report");
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM sql_export_opaque_source(3) ORDER BY 1"));
	auto report = TakeSQLExportRecord(*observer);
	REQUIRE(report.eligible);
	REQUIRE(report.outcome == SQLExportOutcome::UNSUPPORTED_SOURCE);
	REQUIRE(report.route == SQLExportExecutionRoute::ORIGINAL_FALLBACK);
	REQUIRE(report.outcome != SQLExportOutcome::STRUCTURALLY_VALIDATED);
	bool found_source = false;
	for (auto &entry : report.inventory) {
		found_source |= entry.kind == "source" && entry.construct == "sql_export_opaque_source";
	}
	REQUIRE(found_source);

	SetSQLExportMode(con, *observer, "strict");
	REQUIRE_FAIL(con.Query("SELECT * FROM sql_export_opaque_source(3) ORDER BY 1"));
	auto strict = TakeSQLExportRecord(*observer);
	REQUIRE(strict.strict_failure);
	REQUIRE(strict.route == SQLExportExecutionRoute::NONE);
	REQUIRE(strict.code == report.code);
	REQUIRE(strict.phase == report.phase);
	REQUIRE(strict.path == report.path);
	REQUIRE(strict.issues == report.issues);
	REQUIRE_NO_FAIL(con.Query("VALUES (9)"));
	REQUIRE(TakeSQLExportRecord(*observer).outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
}

TEST_CASE("SQL export verification excludes control and prepare paths", "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "strict");
	for (auto sql : {"BEGIN", "CREATE TEMP TABLE sql_export_temp(i INTEGER)", "INSERT INTO sql_export_temp VALUES (3)",
	                 "PREPARE sql_export_p AS SELECT * FROM sql_export_temp", "EXECUTE sql_export_p", "COMMIT"}) {
		REQUIRE_NO_FAIL(con.Query(sql));
		auto record = TakeSQLExportRecord(*observer);
		REQUIRE(record.outcome == SQLExportOutcome::NOT_APPLICABLE);
		REQUIRE(record.route == SQLExportExecutionRoute::ORIGINAL_NOT_APPLICABLE);
		REQUIRE(record.export_count == 0);
	}
	auto prepared = con.Prepare("VALUES (?)");
	REQUIRE_FALSE(prepared->HasError());
	REQUIRE(TakeSQLExportRecord(*observer).outcome == SQLExportOutcome::NOT_APPLICABLE);
	auto prepared_result = prepared->Execute(42);
	REQUIRE_NO_FAIL(*prepared_result);
	if (prepared_result->GetResultType() == QueryResultType::STREAM_RESULT) {
		REQUIRE_NO_FAIL(prepared_result->Cast<StreamQueryResult>().Materialize());
	}
	REQUIRE(TakeSQLExportRecord(*observer).outcome == SQLExportOutcome::NOT_APPLICABLE);
	REQUIRE_NO_FAIL(con.Query("VALUES (42)"));
	REQUIRE(TakeSQLExportRecord(*observer).outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
}

TEST_CASE("SQL export verification propagates optimizer exceptions and restores invocation state",
          "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "report");
	auto counter = make_shared_ptr<SQLExportOptimizerCounter>();
	counter->throw_on_second = true;
	OptimizerExtension extension;
	extension.optimizer_info = counter;
	extension.optimize_function = CountSQLExportOptimization;
	OptimizerExtension::Register(DBConfig::GetConfig(*con.context), extension);
	REQUIRE_FAIL(con.Query("VALUES (1)"));
	auto record = TakeSQLExportRecord(*observer);
	REQUIRE(counter->calls == 2);
	REQUIRE(record.export_count == 1);
	REQUIRE(record.outcome == SQLExportOutcome::REOPTIMIZE_ERROR);
	REQUIRE(record.propagated_error);
	REQUIRE_FALSE(record.strict_failure);
	REQUIRE(record.route == SQLExportExecutionRoute::NONE);
	counter->throw_on_second = false;
	counter->calls = 0;
	REQUIRE_NO_FAIL(con.Query("VALUES (2)"));
	REQUIRE(counter->calls == 2);
	REQUIRE(TakeSQLExportRecord(*observer).outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
}

namespace {

class SQLExportRetryState : public ClientContextState {
public:
	idx_t finalizations = 0;
	idx_t errors = 0;
	bool retry_errors = false;

	bool CanRequestRebind() override {
		return true;
	}
	RebindQueryInfo OnFinalizePrepare(ClientContext &, PreparedStatementData &statement,
	                                  PreparedStatementMode) override {
		if (statement.statement_type != StatementType::SELECT_STATEMENT) {
			return RebindQueryInfo::DO_NOT_REBIND;
		}
		REQUIRE(statement.physical_plan);
		finalizations++;
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
	RebindQueryInfo OnPlanningError(ClientContext &, SQLStatement &, ErrorData &) override {
		errors++;
		return retry_errors ? RebindQueryInfo::ATTEMPT_TO_REBIND : RebindQueryInfo::DO_NOT_REBIND;
	}
};

class SQLExportEvaluationState : public ClientContextState {
public:
	idx_t values = 0;
};

void SQLExportCounter(DataChunk &args, ExpressionState &state, Vector &result) {
	auto counter = state.GetContext().registered_state->Get<SQLExportEvaluationState>("sql_export_counter");
	auto writer = FlatVector::Writer<int64_t>(result, args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		writer.WriteValue(NumericCast<int64_t>(++counter->values));
	}
}

struct SQLExportEffectingSourceState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> BindSQLExportEffectingSource(ClientContext &, TableFunctionBindInput &,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("value");
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> InitSQLExportEffectingSource(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<SQLExportEffectingSourceState>();
}

void SQLExportEffectingSource(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<SQLExportEffectingSourceState>();
	if (state.emitted) {
		return;
	}
	auto counter = context.registered_state->Get<SQLExportEvaluationState>("sql_export_counter");
	auto writer = FlatVector::Writer<int64_t>(output.data[0], 1);
	writer.WriteValue(NumericCast<int64_t>(++counter->values));
	output.SetChildCardinality(1);
	state.emitted = true;
}

struct SQLExportBindHook : public PlannerExtensionInfo {
	idx_t calls = 0;
	bool throw_on_second = false;
};

void SQLExportChangeGeneratedSchema(PlannerExtensionInput &input, BoundStatement &statement) {
	auto &info = static_cast<SQLExportBindHook &>(*input.info);
	if (++info.calls != 2) {
		return;
	}
	if (info.throw_on_second) {
		throw BinderException("SQL export bind hook exception");
	}
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(99)));
	auto projection = make_uniq<LogicalProjection>(input.binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(make_uniq<LogicalDummyScan>(input.binder.GenerateTableIndex()));
	statement.plan = std::move(projection);
	statement.names = {"generated"};
	statement.types = {LogicalType::BIGINT};
}

struct SQLExportParseHook : public ParserExtensionInfo {
	idx_t calls = 0;
	string replacement;
	bool throw_on_second = false;
};

ParserOverrideResult SQLExportReplaceGeneratedParse(ParserExtensionInfo *info_p, const string &, ParserOptions &) {
	auto &info = info_p->Cast<SQLExportParseHook>();
	if (++info.calls != 2) {
		return ParserOverrideResult();
	}
	if (info.throw_on_second) {
		throw ParserException("SQL export parser hook exception");
	}
	Parser parser;
	parser.ParseQuery(info.replacement);
	return ParserOverrideResult(std::move(parser.statements));
}

} // namespace

TEST_CASE("SQL export retry accounting preserves physical finalize hooks", "[sql_export][sql_export_verification]") {
	for (auto mode : {"report", "strict"}) {
		DuckDB db(nullptr);
		Connection con(db);
		auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
		SetSQLExportMode(con, *observer, mode);
		auto retry = make_shared_ptr<SQLExportRetryState>();
		con.context->registered_state->Insert("sql_export_retry", retry);
		auto result = con.Query("VALUES (7)");
		auto record = TakeSQLExportRecord(*observer);
		REQUIRE(retry->finalizations == 1);
		REQUIRE(retry->errors == 0);
		REQUIRE(record.export_count == 1);
		REQUIRE(record.code == "PLANNING_RETRY_AFTER_EXPORT");
		REQUIRE(record.outcome != SQLExportOutcome::STRUCTURALLY_VALIDATED);
		if (string(mode) == "strict") {
			REQUIRE(result->HasError());
			REQUIRE(record.strict_failure);
			REQUIRE(record.route == SQLExportExecutionRoute::NONE);
		} else {
			REQUIRE_NO_FAIL(*result);
			REQUIRE(CHECK_COLUMN(result, 0, {7}));
			REQUIRE(record.route == SQLExportExecutionRoute::ORIGINAL_FALLBACK);
		}
	}
}

TEST_CASE("SQL export executes volatile read-only functions once", "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto counter = make_shared_ptr<SQLExportEvaluationState>();
	con.context->registered_state->Insert("sql_export_counter", counter);
	ExtensionLoader loader(*db.instance, "sql_export_counter");
	ScalarFunction function("sql_export_counter", {}, LogicalType::BIGINT, SQLExportCounter);
	function.SetStability(FunctionStability::VOLATILE);
	loader.RegisterFunction(std::move(function));
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	for (auto mode : {"off", "strict"}) {
		SetSQLExportMode(con, *observer, mode);
		counter->values = 0;
		auto result = con.Query("SELECT x,x FROM (SELECT sql_export_counter() AS x FROM (VALUES (1),(2),(2)) t(i)) q");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(counter->values == 3);
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
		if (string(mode) == "off") {
			REQUIRE(observer->TakeRecords().empty());
		} else {
			auto record = TakeSQLExportRecord(*observer);
			REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
			REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
			REQUIRE(record.comparability == SQLExportComparability::NON_REPEATABLE);
			REQUIRE(record.execution == SQLExportExecutionStatus::SUCCEEDED);
		}
	}
}

TEST_CASE("SQL export structurally validates effecting table sources without repeating them",
          "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto counter = make_shared_ptr<SQLExportEvaluationState>();
	con.context->registered_state->Insert("sql_export_counter", counter);
	ExtensionLoader loader(*db.instance, "sql_export_effecting_source");
	TableFunction function("sql_export_effecting_source", {}, SQLExportEffectingSource, BindSQLExportEffectingSource,
	                       InitSQLExportEffectingSource);
	function.to_sql = TableFunction::ToSQLFunctionCall;
	function.is_repeatable = [](optional_ptr<const FunctionData>) {
		return false;
	};
	loader.RegisterFunction(std::move(function));
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "strict");
	counter->values = 0;
	auto result = con.Query("SELECT * FROM sql_export_effecting_source()");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(counter->values == 1);
	auto record = TakeSQLExportRecord(*observer);
	REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
	REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
	REQUIRE(record.comparability == SQLExportComparability::NON_REPEATABLE);
	REQUIRE(record.execution == SQLExportExecutionStatus::SUCCEEDED);
}

TEST_CASE("SQL export schema failure preserves the original plan and transaction",
          "[sql_export][sql_export_verification]") {
	for (auto mode : {"report", "strict"}) {
		DuckDB db(nullptr);
		Connection con(db);
		auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
		SetSQLExportMode(con, *observer, mode);
		REQUIRE_NO_FAIL(con.Query("BEGIN"));
		observer->TakeRecords();
		auto info = make_shared_ptr<SQLExportBindHook>();
		PlannerExtension extension;
		extension.planner_info = info;
		extension.post_bind_function = SQLExportChangeGeneratedSchema;
		PlannerExtension::Register(DBConfig::GetConfig(*con.context), extension);
		auto result = con.Query("VALUES (42)");
		auto record = TakeSQLExportRecord(*observer);
		REQUIRE(record.outcome == SQLExportOutcome::OUTPUT_SCHEMA_MISMATCH);
		REQUIRE(record.export_count == 1);
		if (string(mode) == "report") {
			REQUIRE_NO_FAIL(*result);
			REQUIRE(CHECK_COLUMN(result, 0, {42}));
			REQUIRE_NO_FAIL(con.Query("VALUES (43)"));
			REQUIRE(TakeSQLExportRecord(*observer).route == SQLExportExecutionRoute::GENERATED);
		} else {
			REQUIRE(result->HasError());
			REQUIRE(record.route == SQLExportExecutionRoute::NONE);
		}
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	}
}

TEST_CASE("SQL export streaming observations are published only on completion",
          "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "strict");
	QueryParameters parameters;
	parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
	auto result = con.context->Query("VALUES (1),(2)", parameters);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	REQUIRE(observer->TakeRecords().empty());
	auto materialized = result->Cast<StreamQueryResult>().Materialize();
	REQUIRE_NO_FAIL(*materialized);
	REQUIRE(materialized->RowCount() == 2);
	REQUIRE(TakeSQLExportRecord(*observer).route == SQLExportExecutionRoute::GENERATED);
	REQUIRE_NO_FAIL(con.Query("VALUES (3); CREATE TEMP TABLE sql_export_tmp(i INTEGER); VALUES (4)"));
	auto records = observer->TakeRecords();
	REQUIRE(records.size() == 3);
	REQUIRE(records[0].route == SQLExportExecutionRoute::GENERATED);
	REQUIRE(records[1].route == SQLExportExecutionRoute::ORIGINAL_NOT_APPLICABLE);
	REQUIRE(records[2].route == SQLExportExecutionRoute::GENERATED);
	for (idx_t i = 0; i < records.size(); i++) {
		REQUIRE(records[i].statement_index == i);
	}
}

TEST_CASE("SQL export propagates parser and binder hook exceptions", "[sql_export][sql_export_verification]") {
	for (auto mode : {"report", "strict"}) {
		for (bool parse_error : {false, true}) {
			DuckDB db(nullptr);
			Connection con(db);
			REQUIRE_NO_FAIL(con.Query("SET allow_parser_override_extension='fallback'"));
			auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
			SetSQLExportMode(con, *observer, mode);
			if (parse_error) {
				auto info = make_shared_ptr<SQLExportParseHook>();
				info->throw_on_second = true;
				ParserExtension extension;
				extension.parser_info = info;
				extension.parser_override = SQLExportReplaceGeneratedParse;
				ParserExtension::Register(DBConfig::GetConfig(*con.context), extension);
			} else {
				auto info = make_shared_ptr<SQLExportBindHook>();
				info->throw_on_second = true;
				PlannerExtension extension;
				extension.planner_info = info;
				extension.post_bind_function = SQLExportChangeGeneratedSchema;
				PlannerExtension::Register(DBConfig::GetConfig(*con.context), extension);
			}
			auto retry = make_shared_ptr<SQLExportRetryState>();
			retry->retry_errors = true;
			con.context->registered_state->Insert("sql_export_retry", retry);
			REQUIRE_FAIL(con.Query("VALUES (42)"));
			auto record = TakeSQLExportRecord(*observer);
			REQUIRE(record.propagated_error);
			REQUIRE(record.export_count == 1);
			REQUIRE(record.route == SQLExportExecutionRoute::NONE);
			REQUIRE(record.outcome == (parse_error ? SQLExportOutcome::REPARSE_ERROR : SQLExportOutcome::REBIND_ERROR));
			REQUIRE(record.strict_failure == (string(mode) == "strict"));
			REQUIRE(retry->errors == 0);
			con.context->registered_state->Remove("sql_export_retry");
			REQUIRE_NO_FAIL(con.Query("VALUES (43)"));
			REQUIRE(TakeSQLExportRecord(*observer).route == SQLExportExecutionRoute::GENERATED);
		}
	}
}

TEST_CASE("SQL export thrown binding failures propagate without speculative fallback",
          "[sql_export][sql_export_verification]") {
	for (auto mode : {"report", "strict"}) {
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET allow_parser_override_extension='fallback'"));
		auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
		SetSQLExportMode(con, *observer, mode);
		auto info = make_shared_ptr<SQLExportParseHook>();
		info->replacement = "SELECT missing_column";
		ParserExtension extension;
		extension.parser_info = info;
		extension.parser_override = SQLExportReplaceGeneratedParse;
		ParserExtension::Register(DBConfig::GetConfig(*con.context), extension);
		auto result = con.Query("VALUES (42)");
		auto record = TakeSQLExportRecord(*observer);
		REQUIRE(record.outcome == SQLExportOutcome::REBIND_ERROR);
		REQUIRE(record.code == "REBIND_EXCEPTION");
		REQUIRE(record.propagated_error);
		REQUIRE(result->HasError());
		REQUIRE(record.route == SQLExportExecutionRoute::NONE);
		REQUIRE(record.strict_failure == (string(mode) == "strict"));
		REQUIRE_NO_FAIL(con.Query("VALUES (43)"));
		REQUIRE(TakeSQLExportRecord(*observer).route == SQLExportExecutionRoute::GENERATED);
	}
}

namespace {

struct SQLExportSourceInfo : public OptimizerExtensionInfo {
	idx_t optimizations = 0;
	idx_t exports = 0;
	idx_t original_physical_plans = 0;
	bool unsupported = false;
	bool throw_export = false;
};

class SQLExportPassThrough : public LogicalExtensionOperator {
public:
	SQLExportPassThrough(unique_ptr<LogicalOperator> child, SQLExportSourceInfo &info_p) : info(info_p) {
		children.push_back(std::move(child));
	}
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}
	string GetExtensionName() const override {
		return name;
	}
	optional_ptr<const string> GetTypeBindingVerificationIdentifier() const noexcept override {
		return name;
	}
	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
		info.original_physical_plans++;
		return planner.CreatePlan(*children[0]);
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}

private:
	SQLExportSourceInfo &info;
	string name = "sql_export_passthrough";
};

class SQLExportSourceExtension : public OperatorExtension {
public:
	explicit SQLExportSourceExtension(shared_ptr<SQLExportSourceInfo> info_p) : info(std::move(info_p)) {
		Bind = [](ClientContext &, Binder &, OperatorExtensionInfo *, SQLStatement &) {
			return BoundStatement();
		};
	}
	std::string GetName() override {
		return "sql_export_passthrough";
	}
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override {
		throw NotImplementedException("SQL export test source does not implement binary serialization");
	}
	LogicalPlanSQLExportExtensionResult ExportLogicalPlanSQL(const LogicalPlanSQLExportExtensionInput &input) override {
		info->exports++;
		if (info->throw_export) {
			throw BinderException("SQL export source hook exception");
		}
		if (info->unsupported) {
			return LogicalPlanSQLExportExtensionResult::Unsupported("SQL export source disabled by test");
		}
		auto select = make_uniq<SelectNode>();
		select->from_table = std::move(input.children[0].table);
		select->select_list.push_back(make_uniq<StarExpression>());
		return LogicalPlanSQLExportExtensionResult::Exported(std::move(select));
	}

private:
	shared_ptr<SQLExportSourceInfo> info;
};

void SQLExportWrapSource(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &info = static_cast<SQLExportSourceInfo &>(*input.info);
	if (++info.optimizations == 1) {
		plan = make_uniq<SQLExportPassThrough>(std::move(plan), info);
	}
}

} // namespace

TEST_CASE("SQL export extension source selects exactly one executable plan", "[sql_export][sql_export_verification]") {
	for (auto mode : {"off", "report", "strict"}) {
		for (idx_t behavior = 0; behavior < (string(mode) == "off" ? 1 : 3); behavior++) {
			DuckDB db(nullptr);
			Connection con(db);
			auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
			SetSQLExportMode(con, *observer, mode);
			auto info = make_shared_ptr<SQLExportSourceInfo>();
			info->unsupported = behavior == 1;
			info->throw_export = behavior == 2;
			OperatorExtension::Register(DBConfig::GetConfig(*con.context),
			                            make_shared_ptr<SQLExportSourceExtension>(info));
			OptimizerExtension optimizer;
			optimizer.optimizer_info = info;
			optimizer.optimize_function = SQLExportWrapSource;
			OptimizerExtension::Register(DBConfig::GetConfig(*con.context), optimizer);
			auto result = con.Query("VALUES (42),(42),(NULL)");
			if (string(mode) == "off") {
				REQUIRE_NO_FAIL(*result);
				REQUIRE(info->exports == 0);
				REQUIRE(info->original_physical_plans == 1);
				REQUIRE(observer->TakeRecords().empty());
				continue;
			}
			REQUIRE(info->exports == 1);
			auto record = TakeSQLExportRecord(*observer);
			if (info->throw_export) {
				REQUIRE(result->HasError());
				REQUIRE(record.outcome == SQLExportOutcome::EXPORT_ERROR);
				REQUIRE(record.propagated_error);
				REQUIRE(record.route == SQLExportExecutionRoute::NONE);
				REQUIRE(info->original_physical_plans == 0);
			} else if (!info->unsupported) {
				REQUIRE_NO_FAIL(*result);
				REQUIRE(result->RowCount() == 3);
				REQUIRE(result->GetValue(0, 2).IsNull());
				REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
				REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
				REQUIRE(record.comparability == SQLExportComparability::NON_REPEATABLE);
				REQUIRE(info->original_physical_plans == 0);
			} else if (string(mode) == "strict") {
				REQUIRE(result->HasError());
				REQUIRE(record.outcome == SQLExportOutcome::UNSUPPORTED_EXTENSION);
				REQUIRE(record.route == SQLExportExecutionRoute::NONE);
				REQUIRE(info->original_physical_plans == 0);
			} else {
				REQUIRE_NO_FAIL(*result);
				REQUIRE(record.outcome == SQLExportOutcome::UNSUPPORTED_EXTENSION);
				REQUIRE(record.route == SQLExportExecutionRoute::ORIGINAL_FALLBACK);
				REQUIRE(info->original_physical_plans == 1);
			}
		}
	}
}

TEST_CASE("SQL export fallback and strict rejection do not duplicate source evaluation",
          "[sql_export][sql_export_verification]") {
	for (auto mode : {"report", "strict"}) {
		DuckDB db(nullptr);
		Connection con(db);
		RegisterSQLExportOpaqueSource(db, con);
		auto counter = make_shared_ptr<SQLExportEvaluationState>();
		con.context->registered_state->Insert("sql_export_counter", counter);
		ExtensionLoader loader(*db.instance, "sql_export_counter");
		ScalarFunction function("sql_export_counter", {}, LogicalType::BIGINT, SQLExportCounter);
		function.SetStability(FunctionStability::VOLATILE);
		loader.RegisterFunction(std::move(function));
		auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
		SetSQLExportMode(con, *observer, mode);
		auto result = con.Query("SELECT sql_export_counter() FROM sql_export_opaque_source(3)");
		auto record = TakeSQLExportRecord(*observer);
		REQUIRE(record.outcome == SQLExportOutcome::UNSUPPORTED_SOURCE);
		REQUIRE(record.export_count == 1);
		if (string(mode) == "strict") {
			REQUIRE(result->HasError());
			REQUIRE(counter->values == 0);
			REQUIRE(record.route == SQLExportExecutionRoute::NONE);
		} else {
			REQUIRE_NO_FAIL(*result);
			REQUIRE(counter->values == 3);
			REQUIRE(record.route == SQLExportExecutionRoute::ORIGINAL_FALLBACK);
		}
		counter->values = 0;
		result = con.Query("SELECT CASE WHEN i=1 THEN sql_export_counter() ELSE 42 END "
		                   "FROM (VALUES (0),(1),(0)) t(i)");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(counter->values == 1);
		REQUIRE(CHECK_COLUMN(result, 0, {42, 1, 42}));
		auto volatile_record = TakeSQLExportRecord(*observer);
		REQUIRE(volatile_record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
		REQUIRE(volatile_record.route == SQLExportExecutionRoute::GENERATED);
		REQUIRE(volatile_record.comparability == SQLExportComparability::NON_REPEATABLE);
	}
}

TEST_CASE("SQL export retries original planning errors before its one export",
          "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "strict");
	auto counter = make_shared_ptr<SQLExportOptimizerCounter>();
	counter->throw_on_first = true;
	OptimizerExtension optimizer;
	optimizer.optimizer_info = counter;
	optimizer.optimize_function = CountSQLExportOptimization;
	OptimizerExtension::Register(DBConfig::GetConfig(*con.context), optimizer);
	auto retry = make_shared_ptr<SQLExportRetryState>();
	retry->retry_errors = true;
	con.context->registered_state->Insert("sql_export_retry", retry);
	auto result = con.Query("VALUES (7)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {7}));
	REQUIRE(counter->calls == 3);
	REQUIRE(retry->errors == 1);
	REQUIRE(retry->finalizations == 0);
	auto record = TakeSQLExportRecord(*observer);
	REQUIRE(record.export_count == 1);
	REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
}

TEST_CASE("SQL export inventories lambda bodies and window callables", "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterSQLExportOpaqueSource(db, con);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "report");
	for (auto sql : {"SELECT list_transform([i], lambda x: abs(x)) FROM (VALUES (-1),(2)) t(i)",
	                 "SELECT row_number() OVER (),sum(i) OVER () FROM sql_export_opaque_source(2) t(i)"}) {
		REQUIRE_NO_FAIL(con.Query(sql));
		auto record = TakeSQLExportRecord(*observer);
		REQUIRE(record.eligible);
		unordered_set<string> functions;
		for (auto &entry : record.inventory) {
			if (entry.kind == "function") {
				functions.insert(entry.construct);
			}
		}
		if (StringUtil::Contains(sql, "list_transform")) {
			REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
			REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
			REQUIRE(record.comparability == SQLExportComparability::COMPARABLE);
			REQUIRE(functions.count("abs") == 1);
			REQUIRE(functions.count("list_transform") == 1);
		} else {
			REQUIRE(record.outcome != SQLExportOutcome::STRUCTURALLY_VALIDATED);
			REQUIRE(functions.count("sum") == 1);
			REQUIRE(functions.count("row_number") == 1);
		}
	}
}

TEST_CASE("SQL export covers parameter-free statement and pending APIs", "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "strict");
	auto statements = con.ExtractStatements("VALUES (42)");
	REQUIRE_NO_FAIL(con.Query(std::move(statements[0])));
	REQUIRE(TakeSQLExportRecord(*observer).route == SQLExportExecutionRoute::GENERATED);
	auto pending = con.PendingQuery("VALUES (43)");
	REQUIRE_FALSE(pending->HasError());
	REQUIRE_NO_FAIL(pending->Execute());
	REQUIRE(TakeSQLExportRecord(*observer).route == SQLExportExecutionRoute::GENERATED);
	vector<Value> parameters {Value::INTEGER(44)};
	pending = con.PendingQuery("VALUES (?)", parameters);
	REQUIRE_FALSE(pending->HasError());
	REQUIRE_NO_FAIL(pending->Execute());
	auto record = TakeSQLExportRecord(*observer);
	REQUIRE(record.code == "PARAMETERS");
	REQUIRE(record.route == SQLExportExecutionRoute::ORIGINAL_NOT_APPLICABLE);
}

namespace {

struct SQLExportNestedPlanning : public OptimizerExtensionInfo {
	bool nested = false;
	idx_t calls = 0;
};

void SQLExportPlanNested(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &) {
	auto &info = static_cast<SQLExportNestedPlanning &>(*input.info);
	info.calls++;
	if (info.nested) {
		return;
	}
	info.nested = true;
	Parser parser(input.context.GetParserOptions());
	parser.ParseQuery("VALUES (99)");
	Planner planner(input.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	planner.Optimize();
	info.nested = false;
}

} // namespace

TEST_CASE("SQL export nested logical planning cannot recurse into execution verification",
          "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "strict");
	auto info = make_shared_ptr<SQLExportNestedPlanning>();
	OptimizerExtension extension;
	extension.optimizer_info = info;
	extension.optimize_function = SQLExportPlanNested;
	OptimizerExtension::Register(DBConfig::GetConfig(*con.context), extension);
	for (idx_t i = 0; i < 2; i++) {
		REQUIRE_NO_FAIL(con.Query("VALUES (42)"));
		auto record = TakeSQLExportRecord(*observer);
		REQUIRE(record.export_count == 1);
		REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
		REQUIRE(info->calls == 4 * (i + 1));
	}
}

TEST_CASE("SQL export inventories pushed single and multi-column scan predicates",
          "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT * FROM (VALUES (-3,-1),(-2,2),(1,-3),(2,2)) v(i,j)"));
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	for (auto sql :
	     {"SELECT i FROM t WHERE abs(i)=2", "SELECT i FROM t WHERE abs(i)+abs(j)=4", "SELECT i FROM t WHERE i<j"}) {
		SetSQLExportMode(con, *observer, "report");
		auto result = con.Query(sql);
		REQUIRE_NO_FAIL(*result);
		auto record = TakeSQLExportRecord(*observer);
		idx_t absolute_values = 0;
		REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
		REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
		for (auto &entry : record.inventory) {
			absolute_values += entry.kind == "function" && entry.construct == "abs";
		}
		REQUIRE(absolute_values == (StringUtil::Contains(sql, "i<j")      ? 0
		                            : StringUtil::Contains(sql, "abs(j)") ? 2
		                                                                  : 1));
		if (StringUtil::Contains(sql, "i<j")) {
			optional<LogicalPlanVerificationPath> scan_path;
			for (auto &entry : record.inventory) {
				if (entry.kind == "operator" && entry.construct == "GET") {
					scan_path = entry.path;
				}
			}
			REQUIRE(scan_path);
			bool scan_comparison = false;
			for (auto &entry : record.inventory) {
				if (entry.kind != "function" || entry.construct != "<" ||
				    entry.path.components.size() <= scan_path->components.size()) {
					continue;
				}
				bool below_scan = true;
				for (idx_t i = 0; i < scan_path->components.size(); i++) {
					below_scan &= entry.path.components[i] == scan_path->components[i];
				}
				scan_comparison |= below_scan;
			}
			REQUIRE(scan_comparison);
		}
		SetSQLExportMode(con, *observer, "strict");
		auto strict_result = con.Query(sql);
		REQUIRE_NO_FAIL(*strict_result);
		auto strict = TakeSQLExportRecord(*observer);
		REQUIRE(strict.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
		REQUIRE(strict.route == SQLExportExecutionRoute::GENERATED);
		REQUIRE(strict.inventory.size() == record.inventory.size());
		for (idx_t i = 0; i < record.inventory.size(); i++) {
			REQUIRE(strict.inventory[i].construct == record.inventory[i].construct);
			REQUIRE(strict.inventory[i].path == record.inventory[i].path);
		}
	}
}

namespace {

struct SQLExportOptimizerReplacement : public OptimizerExtensionInfo {
	idx_t calls = 0;
	string replacement;
	bool require_rebind = false;
};

void SQLExportReplaceDuringOptimization(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &info = static_cast<SQLExportOptimizerReplacement &>(*input.info);
	if (++info.calls != 2) {
		return;
	}
	if (info.require_rebind) {
		auto &properties = input.optimizer.binder.GetStatementProperties();
		properties.always_require_rebind = true;
		properties.RegisterDBRead(Catalog::GetCatalog(input.context, Identifier("temp")), input.context);
	}
	if (!info.replacement.empty()) {
		Parser parser(input.context.GetParserOptions());
		parser.ParseQuery(info.replacement);
		auto bound = input.optimizer.binder.Bind(*parser.statements[0]);
		plan = std::move(bound.plan);
	}
}

} // namespace

TEST_CASE("SQL export retains final properties after generated optimization", "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "strict");
	auto info = make_shared_ptr<SQLExportOptimizerReplacement>();
	info->require_rebind = true;
	OptimizerExtension extension;
	extension.optimizer_info = info;
	extension.optimize_function = SQLExportReplaceDuringOptimization;
	OptimizerExtension::Register(DBConfig::GetConfig(*con.context), extension);

	auto result = con.Query("VALUES (42::BIGINT)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(info->calls == 2);
	REQUIRE(result->GetStatementProperties().always_require_rebind);
	REQUIRE(result->GetStatementProperties().read_databases.count(Identifier("temp")) == 1);
	auto record = TakeSQLExportRecord(*observer);
	REQUIRE(record.export_count == 1);
	REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
	REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
}

TEST_CASE("SQL export checks final type annotations after generated optimization",
          "[sql_export][sql_export_verification]") {
	for (auto mode : {"off", "report", "strict"}) {
		CAPTURE(mode);
		DuckDB db(nullptr);
		Connection con(db);
		auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
		SetSQLExportMode(con, *observer, mode);
		auto info = make_shared_ptr<SQLExportOptimizerReplacement>();
		info->replacement = "SELECT 'A' AS value";
		OptimizerExtension extension;
		extension.optimizer_info = info;
		extension.optimize_function = SQLExportReplaceDuringOptimization;
		OptimizerExtension::Register(DBConfig::GetConfig(*con.context), extension);

		auto result = con.Query("SELECT 'A' COLLATE nocase AS value");
		if (string(mode) == "off") {
			REQUIRE_NO_FAIL(*result);
			REQUIRE(info->calls == 1);
			REQUIRE(observer->TakeRecords().empty());
		} else {
			REQUIRE(info->calls == 2);
			auto record = TakeSQLExportRecord(*observer);
			REQUIRE(record.outcome == SQLExportOutcome::OUTPUT_SCHEMA_MISMATCH);
			REQUIRE(record.code == "GENERATED_SCHEMA_OR_PROPERTIES");
			REQUIRE(record.phase == "SCHEMA");
			REQUIRE(record.outcome != SQLExportOutcome::STRUCTURALLY_VALIDATED);
			REQUIRE(record.strict_failure == (string(mode) == "strict"));
			REQUIRE(record.route == (string(mode) == "strict" ? SQLExportExecutionRoute::NONE
			                                                  : SQLExportExecutionRoute::ORIGINAL_FALLBACK));
			if (string(mode) == "strict") {
				REQUIRE(result->HasError());
			} else {
				REQUIRE_NO_FAIL(*result);
			}
		}
		if (!result->HasError()) {
			REQUIRE(result->GetTypes().size() == 1);
			REQUIRE(StringType::GetCollation(result->GetTypes()[0]) == "nocase");
			REQUIRE(CHECK_COLUMN(result, 0, {"A"}));
		}
	}
}

namespace {

struct SQLExportRetryBindFailure : public PlannerExtensionInfo {
	idx_t calls = 0;
};

void SQLExportFailRetriedBinding(PlannerExtensionInput &input, BoundStatement &) {
	auto &info = static_cast<SQLExportRetryBindFailure &>(*input.info);
	if (++info.calls == 3) {
		throw BinderException(QueryLocation(0, 6), "Final original bind hook failure");
	}
}

} // namespace

TEST_CASE("SQL export retires discarded success before retry binding or optimization",
          "[sql_export][sql_export_verification]") {
	for (auto mode : {"report", "strict"}) {
		for (auto fail_binding : {false, true}) {
			CAPTURE(mode, fail_binding);
			DuckDB db(nullptr);
			Connection con(db);
			auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
			observer->retain_failure_sql = true;
			SetSQLExportMode(con, *observer, mode);
			auto retry = make_shared_ptr<SQLExportRetryState>();
			con.context->registered_state->Insert("sql_export_retry", retry);
			auto counter = make_shared_ptr<SQLExportOptimizerCounter>();
			counter->throw_on_third = !fail_binding;
			OptimizerExtension optimizer_extension;
			optimizer_extension.optimizer_info = counter;
			optimizer_extension.optimize_function = CountSQLExportOptimization;
			OptimizerExtension::Register(DBConfig::GetConfig(*con.context), optimizer_extension);
			auto bind_info = make_shared_ptr<SQLExportRetryBindFailure>();
			if (fail_binding) {
				PlannerExtension bind_extension;
				bind_extension.planner_info = bind_info;
				bind_extension.post_bind_function = SQLExportFailRetriedBinding;
				PlannerExtension::Register(DBConfig::GetConfig(*con.context), bind_extension);
			}

			auto result = con.Query("VALUES (42::BIGINT)");
			REQUIRE(result->HasError());
			REQUIRE(StringUtil::Contains(result->GetError(), fail_binding ? "Final original bind hook failure"
			                                                              : "Final original optimizer hook failure"));
			REQUIRE(StringUtil::Contains(result->GetError(), "LINE 1: VALUES (42::BIGINT)"));
			REQUIRE(StringUtil::Contains(result->GetError(), "^^^^^^"));
			auto record = TakeSQLExportRecord(*observer);
			REQUIRE(retry->finalizations == 1);
			REQUIRE(retry->errors == 0);
			REQUIRE(counter->calls == (fail_binding ? 2 : 3));
			REQUIRE(bind_info->calls == (fail_binding ? 3 : 0));
			REQUIRE(record.export_count == 1);
			REQUIRE(record.route == SQLExportExecutionRoute::NONE);
			REQUIRE(record.code == "ORIGINAL_PLANNING");
			REQUIRE(record.phase == "ORIGINAL_PLANNING");
			REQUIRE(record.outcome == SQLExportOutcome::NOT_APPLICABLE);
			REQUIRE(record.propagated_error);
			REQUIRE(record.query_error);
			REQUIRE_FALSE(record.eligible);
			REQUIRE_FALSE(record.strict_failure);
			REQUIRE(record.outcome != SQLExportOutcome::STRUCTURALLY_VALIDATED);
			REQUIRE(record.inventory.empty());
			REQUIRE(record.issues.empty());
			REQUIRE_FALSE(record.path);
			REQUIRE(record.generated_sql.empty());

			con.context->registered_state->Remove("sql_export_retry");
			REQUIRE_NO_FAIL(con.Query("VALUES (43::BIGINT)"));
			record = TakeSQLExportRecord(*observer);
			REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
			REQUIRE(record.export_count == 1);
			REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
		}
	}
}

TEST_CASE("SQL export retains completed verification when physical planning fails without retry",
          "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	for (auto mode : {"report", "strict"}) {
		SetSQLExportMode(con, *observer, mode);
		auto result = con.Query("VALUES (3), ('hello')");
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "Could not convert string 'hello' to INT32"));
		REQUIRE(StringUtil::Contains(result->GetError(), "^"));
		auto record = TakeSQLExportRecord(*observer);
		REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
		REQUIRE(record.export_count == 1);
		REQUIRE(record.route == SQLExportExecutionRoute::NONE);
		REQUIRE(record.propagated_error);
		REQUIRE(record.query_error);
		REQUIRE_FALSE(record.strict_failure);
		REQUIRE_FALSE(record.inventory.empty());
	}
}

TEST_CASE("SQL export renders error positions against their generated source",
          "[sql_export][sql_export_verification]") {
	for (bool collect : {false, true}) {
		for (bool streaming : {false, true}) {
			CAPTURE(collect, streaming);
			DuckDB db(nullptr);
			Connection con(db);
			REQUIRE_NO_FAIL(con.Query("SET threads=1; SET max_streaming_buffer_size='1b'"));
			shared_ptr<SQLExportVerificationState> observer;
			if (collect) {
				observer = SQLExportVerificationState::GetOrCreate(*con.context);
				REQUIRE_FALSE(observer->retain_failure_sql);
			}
			REQUIRE_NO_FAIL(con.Query("SET debug_verify_sql_export='strict'"));
			if (observer) {
				observer->TakeRecords();
			}
			QueryParameters parameters;
			parameters.output_type =
			    streaming ? QueryResultOutputType::ALLOW_STREAMING : QueryResultOutputType::FORCE_MATERIALIZED;
			auto rows = streaming ? StringUtil::Repeat("('1'),", STANDARD_VECTOR_SIZE * 2) : string();
			auto result =
			    con.context->Query("SELECT CAST(x AS UTINYINT) FROM (VALUES " + rows + "('hello'))t(x)", parameters);
			if (streaming) {
				REQUIRE_NO_FAIL(*result);
				REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
				result = result->Cast<StreamQueryResult>().Materialize();
			}
			REQUIRE(result->HasError());
			REQUIRE(StringUtil::Contains(result->GetError(), "Could not convert string 'hello' to UINT8"));
			REQUIRE(StringUtil::Contains(result->GetError(), "r0.c0"));
			REQUIRE(StringUtil::Contains(result->GetError(), "^"));
			if (observer) {
				auto record = TakeSQLExportRecord(*observer);
				REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
				REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
				REQUIRE(record.generated_sql.empty());
			}
			REQUIRE_NO_FAIL(con.Query("SET debug_verify_sql_export='off'"));
			auto original = con.Query("SELECT 'hello'::INTEGER");
			REQUIRE(original->HasError());
			REQUIRE(StringUtil::Contains(original->GetError(), "LINE 1: SELECT 'hello'::INTEGER"));
			REQUIRE(StringUtil::Contains(original->GetError(), "^"));
		}
	}
}

TEST_CASE("SQL export keeps auxiliary parse errors independent of active streams",
          "[sql_export][sql_export_verification]") {
	for (auto mode : {"off", "strict"}) {
		for (bool collect : {false, true}) {
			CAPTURE(mode, collect);
			DuckDB db(nullptr);
			Connection con(db);
			REQUIRE_NO_FAIL(con.Query("SET threads=1; SET max_streaming_buffer_size='1b'"));
			shared_ptr<SQLExportVerificationState> observer;
			if (collect) {
				observer = SQLExportVerificationState::GetOrCreate(*con.context);
			}
			REQUIRE_NO_FAIL(con.Query("SET debug_verify_sql_export='" + string(mode) + "'"));
			if (observer) {
				observer->TakeRecords();
			}
			const idx_t count = STANDARD_VECTOR_SIZE * 2 + 1;
			auto sql = "SELECT x AS original_name FROM (VALUES " + StringUtil::Repeat("(1),", count - 1) + "(2))t(x)";
			QueryParameters parameters;
			parameters.output_type = QueryResultOutputType::ALLOW_STREAMING;
			auto stream = con.context->Query(sql, parameters);
			REQUIRE_NO_FAIL(*stream);
			REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
			REQUIRE(stream->GetNames() == vector<Identifier> {Identifier("original_name")});
			REQUIRE(con.context->GetCurrentQuery() == sql);
			string error;
			try {
				auto plan = con.ExtractPlan("SELECT 1 + ;");
			} catch (std::exception &ex) {
				error = ex.what();
			}
			REQUIRE(StringUtil::Contains(error, "LINE 1: SELECT 1 + ;"));
			REQUIRE(StringUtil::Contains(error, "^"));
			REQUIRE_FALSE(StringUtil::Contains(error, "LINE 1: SELECT r0.c0"));
			REQUIRE(con.context->GetCurrentQuery() == sql);
			auto result = stream->Cast<StreamQueryResult>().Materialize();
			REQUIRE_NO_FAIL(*result);
			REQUIRE(result->RowCount() == count);
			if (observer && string(mode) != "off") {
				auto record = TakeSQLExportRecord(*observer);
				REQUIRE(record.route == SQLExportExecutionRoute::GENERATED);
				REQUIRE(record.outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
			}
		}
	}
}
