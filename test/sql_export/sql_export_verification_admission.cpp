#include "catch.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/sql_export_verification.hpp"
#include "sql_export_test_helpers.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "sql_export_verification_test_helpers.hpp"

using namespace duckdb;

namespace sql_export_verification_test {

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

	SetSQLExportMode(con, *observer, "supported");
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM sql_export_opaque_source(3) ORDER BY 1"));
	auto supported = TakeSQLExportRecord(*observer);
	REQUIRE(supported.outcome == SQLExportOutcome::UNSUPPORTED_SOURCE);
	REQUIRE(supported.route == SQLExportExecutionRoute::ORIGINAL_FALLBACK);
	REQUIRE_FALSE(supported.strict_failure);

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
	REQUIRE(TakeSQLExportRecord(*observer).outcome == SQLExportOutcome::NOT_APPLICABLE);
	REQUIRE_NO_FAIL(con.Query("VALUES (42)"));
	REQUIRE(TakeSQLExportRecord(*observer).outcome == SQLExportOutcome::STRUCTURALLY_VALIDATED);
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

TEST_CASE("SQL export covers parameter-free statement and submission APIs", "[sql_export][sql_export_verification]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto observer = SQLExportVerificationState::GetOrCreate(*con.context);
	SetSQLExportMode(con, *observer, "strict");
	auto statements = con.ExtractStatements("VALUES (42)");
	REQUIRE_NO_FAIL(con.Query(std::move(statements[0])));
	REQUIRE(TakeSQLExportRecord(*observer).route == SQLExportExecutionRoute::GENERATED);
	auto pending = con.Submit("VALUES (43)");
	REQUIRE_FALSE(pending->HasError());
	pending->Complete();
	REQUIRE_NO_FAIL(*pending);
	REQUIRE(TakeSQLExportRecord(*observer).route == SQLExportExecutionRoute::GENERATED);
	vector<Value> parameters {Value::INTEGER(44)};
	pending = con.Submit("VALUES (?)", parameters);
	REQUIRE_FALSE(pending->HasError());
	pending->Complete();
	REQUIRE_NO_FAIL(*pending);
	auto record = TakeSQLExportRecord(*observer);
	REQUIRE(record.code == "PARAMETERS");
	REQUIRE(record.route == SQLExportExecutionRoute::ORIGINAL_NOT_APPLICABLE);
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
	for (auto mode : {"off", "report", "supported", "strict"}) {
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
			REQUIRE(record.strict_failure == (string(mode) != "report"));
			REQUIRE(record.route == (string(mode) != "report" ? SQLExportExecutionRoute::NONE
			                                                  : SQLExportExecutionRoute::ORIGINAL_FALLBACK));
			if (string(mode) != "report") {
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
			parameters.result_eagerness = streaming ? ResultEagerness::AUTO : ResultEagerness::FORCED;
			auto rows = streaming ? StringUtil::Repeat("('1'),", STANDARD_VECTOR_SIZE * 2) : string();
			auto result = SubmitSQLExportResult(
			    *con.context, "SELECT CAST(x AS UTINYINT) FROM (VALUES " + rows + "('hello'))t(x)", parameters);
			if (streaming) {
				REQUIRE_NO_FAIL(*result);
				REQUIRE(result->IsOpen());
				result = MaterializeSQLExportStream(std::move(result));
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
			parameters.result_eagerness = ResultEagerness::AUTO;
			auto stream = SubmitSQLExportResult(*con.context, sql, parameters);
			REQUIRE_NO_FAIL(*stream);
			REQUIRE(stream->IsOpen());
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
			auto result = MaterializeSQLExportStream(std::move(stream));
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

} // namespace sql_export_verification_test
