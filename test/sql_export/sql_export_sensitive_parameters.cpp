#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "logical_plan_sql_export_test_helpers.hpp"

using namespace duckdb;

namespace logical_plan_sql_export_test {

// SQL cannot register a table function or a copy format, so the sensitivity marking of their
// parameters is only reachable through probes registered from C++. The SQL-reachable protection
// coverage (ordinary parameterized scans keep exporting) lives in test/sql/sql_export

static unique_ptr<FunctionData> SensitiveProbeBind(ClientContext &, TableFunctionBindInput &,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("i");
	return make_uniq<TableFunctionData>();
}

static void SensitiveProbeScan(ClientContext &, TableFunctionInput &, DataChunk &output) {
	output.SetCardinality(0);
}

static unique_ptr<FunctionData> SensitiveCopyBind(ClientContext &, CopyFunctionBindInput &, const vector<Identifier> &,
                                                  const vector<LogicalType> &) {
	return make_uniq<TableFunctionData>();
}

static void SensitiveCopyOptions(ClientContext &, CopyOptionsInput &input) {
	input.options[Identifier("plain")] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::WRITE_ONLY);
	CopyOption key_material(LogicalType::VARCHAR, CopyOptionMode::WRITE_ONLY);
	key_material.sensitive = true;
	input.options[Identifier("key_material")] = key_material;
}

static void RegisterSensitiveProbes(DuckDB &db) {
	ExtensionLoader loader(*db.instance, "sensitive_export_probe");
	// a scan with a sensitive keyword-only parameter next to benign parameters
	auto signature = FunctionSignature()
	                     .AddParameter(Identifier("endpoint"), LogicalType::VARCHAR)
	                     .AddKeywordOnly(Identifier("api_key"), LogicalType::VARCHAR, Value(""))
	                     .Sensitive()
	                     .AddKeywordOnly(Identifier("region"), LogicalType::VARCHAR, Value("local"));
	TableFunction scan(Identifier("sensitive_probe_scan"), signature, SensitiveProbeScan, SensitiveProbeBind);
	loader.RegisterFunction(std::move(scan));
	// a scan whose typed "**kwargs" schema marks one option sensitive
	auto kwargs_schema = TypedKwargs()
	                         .Add(Identifier("plain"), LogicalType::VARCHAR)
	                         .Add(Identifier("secret_token"), LogicalType::VARCHAR)
	                         .Alias(Identifier("token"))
	                         .Sensitive();
	auto kwargs_signature = FunctionSignature()
	                            .AddParameter(Identifier("endpoint"), LogicalType::VARCHAR)
	                            .AddTypedKwargs(Identifier("options"), std::move(kwargs_schema));
	TableFunction kwargs_scan(Identifier("sensitive_probe_options_scan"), kwargs_signature, SensitiveProbeScan,
	                          SensitiveProbeBind);
	loader.RegisterFunction(std::move(kwargs_scan));
	// a scan with a sensitive positional parameter
	auto positional_signature = FunctionSignature()
	                                .AddParameter(Identifier("endpoint"), LogicalType::VARCHAR)
	                                .AddParameter(Identifier("credential"), LogicalType::VARCHAR)
	                                .Sensitive();
	TableFunction positional_scan(Identifier("sensitive_probe_positional_scan"), positional_signature,
	                              SensitiveProbeScan, SensitiveProbeBind);
	loader.RegisterFunction(std::move(positional_scan));
	// a copy format with a sensitive option next to a benign option
	CopyFunction copy_probe(Identifier("sensitive_copy_probe"));
	copy_probe.copy_to_bind = SensitiveCopyBind;
	copy_probe.copy_options = SensitiveCopyOptions;
	copy_probe.extension = "sensitive_copy_probe";
	loader.RegisterFunction(std::move(copy_probe));
}

static void RequireSensitiveParameterIssue(const PlanExportResult &result, const string &function_name,
                                           const string &parameter_name) {
	RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	auto &issue = result.GetIssues()[0];
	REQUIRE(*issue.construct == LogicalPlanVerificationConstructIdentity::ExportFeature("sensitive_parameter"));
	REQUIRE(issue.facts.size() == 2);
	REQUIRE((issue.facts[0] == pair<string, Value> {"function", Value(function_name)}));
	REQUIRE((issue.facts[1] == pair<string, Value> {"parameter", Value(parameter_name)}));
}

TEST_CASE("Sensitive table function parameters refuse SQL export",
          "[sql_export][logical_plan_sql_export][sensitive_parameters]") {
	DuckDB db(nullptr);
	Connection connection(db);
	RegisterSensitiveProbes(db);
	connection.BeginTransaction();

	SECTION("sensitive keyword-only parameter refuses with function and parameter facts") {
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection, "SELECT * FROM sensitive_probe_scan('http://endpoint', api_key := 'credential-value')");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		RequireSensitiveParameterIssue(exported, "sensitive_probe_scan", "api_key");
		REQUIRE(!StringUtil::Contains(exported.GetIssues()[0].message, "credential-value"));
	}
	SECTION("the same scan without the sensitive argument exports") {
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection, "SELECT * FROM sensitive_probe_scan('http://endpoint', region := 'eu')");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		REQUIRE(exported.IsSuccess());
		auto text = exported.GetValue().query->ToString();
		REQUIRE(StringUtil::Contains(text, "sensitive_probe_scan"));
		REQUIRE(StringUtil::Contains(text, "region"));
	}
	SECTION("a sensitive typed kwargs option refuses under its canonical name") {
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection, "SELECT * FROM sensitive_probe_options_scan('http://endpoint', token := 'credential-value')");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		RequireSensitiveParameterIssue(exported, "sensitive_probe_options_scan", "secret_token");
	}
	SECTION("the kwargs scan without the sensitive option exports") {
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection, "SELECT * FROM sensitive_probe_options_scan('http://endpoint', plain := 'visible')");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		REQUIRE(exported.IsSuccess());
		auto text = exported.GetValue().query->ToString();
		REQUIRE(StringUtil::Contains(text, "sensitive_probe_options_scan"));
		REQUIRE(StringUtil::Contains(text, "visible"));
	}
	SECTION("a sensitive positional parameter refuses") {
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection, "SELECT * FROM sensitive_probe_positional_scan('http://endpoint', 'credential-value')");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		RequireSensitiveParameterIssue(exported, "sensitive_probe_positional_scan", "credential");
	}
	connection.Rollback();
}

TEST_CASE("Sensitive parameter refusal surfaces through EXPLAIN (SQL)",
          "[sql_export][logical_plan_sql_export][sensitive_parameters]") {
	DuckDB db(nullptr);
	Connection connection(db);
	RegisterSensitiveProbes(db);

	auto refused = connection.Query(
	    "EXPLAIN (SQL) SELECT * FROM sensitive_probe_scan('http://endpoint', api_key := 'credential-value')");
	REQUIRE(refused->HasError());
	REQUIRE(StringUtil::Contains(refused->GetError(), "sensitive_probe_scan"));
	REQUIRE(StringUtil::Contains(refused->GetError(), "api_key"));
	REQUIRE(!StringUtil::Contains(refused->GetError(), "credential-value"));

	auto allowed =
	    connection.Query("EXPLAIN (SQL) SELECT * FROM sensitive_probe_scan('http://endpoint', region := 'eu')");
	REQUIRE_NO_FAIL(*allowed);
	auto sql = allowed->Collection().GetValue(1, 0).ToString();
	REQUIRE(StringUtil::Contains(sql, "sensitive_probe_scan"));
}

TEST_CASE("Ordinary parameterized scans keep exporting",
          "[sql_export][logical_plan_sql_export][sensitive_parameters]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto csv_path = TestCreatePath("sensitive_export_protection.csv");
	REQUIRE_NO_FAIL(connection.Query("COPY (SELECT 1 AS i, 'x' AS s) TO '" + csv_path + "' (HEADER)"));
	auto exported = connection.Query("EXPLAIN (SQL) SELECT * FROM read_csv('" + csv_path +
	                                 "', header := true, delim := ',', auto_detect := true)");
	REQUIRE_NO_FAIL(*exported);
	auto sql = exported->Collection().GetValue(1, 0).ToString();
	REQUIRE(StringUtil::Contains(sql, "read_csv"));
	auto roundtrip = connection.Query(sql);
	REQUIRE_NO_FAIL(*roundtrip);
	REQUIRE(roundtrip->RowCount() == 1);
}

TEST_CASE("Sensitive copy options refuse COPY TO export",
          "[sql_export][logical_plan_sql_export][sensitive_parameters]") {
	DuckDB db(nullptr);
	Connection connection(db);
	RegisterSensitiveProbes(db);
	auto out_path = TestCreatePath("sensitive_copy_probe_out");

	auto refused = connection.Query("EXPLAIN (SQL) COPY (SELECT 42 AS i) TO '" + out_path +
	                                "' (FORMAT 'sensitive_copy_probe', key_material 'credential-value')");
	REQUIRE(refused->HasError());
	REQUIRE(StringUtil::Contains(refused->GetError(), "sensitive_copy_probe"));
	REQUIRE(StringUtil::Contains(refused->GetError(), "key_material"));
	REQUIRE(!StringUtil::Contains(refused->GetError(), "credential-value"));

	auto allowed = connection.Query("EXPLAIN (SQL) COPY (SELECT 42 AS i) TO '" + out_path +
	                                "' (FORMAT 'sensitive_copy_probe', plain 'visible')");
	REQUIRE_NO_FAIL(*allowed);
	auto sql = allowed->Collection().GetValue(1, 0).ToString();
	REQUIRE(StringUtil::Contains(sql, "sensitive_copy_probe"));
	REQUIRE(StringUtil::Contains(sql, "plain"));
}

} // namespace logical_plan_sql_export_test
