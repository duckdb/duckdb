#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/scalar_function.hpp"
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
	// a sensitive option that is valid without a value
	CopyOption key_flag(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
	key_flag.sensitive = true;
	input.options[Identifier("key_flag")] = key_flag;
	// a sensitive option whose name generic COPY binding consumes into resolved sink state
	CopyOption batch_size(LogicalType::UBIGINT, CopyOptionMode::WRITE_ONLY);
	batch_size.sensitive = true;
	input.options[Identifier("batch_size")] = batch_size;
}

static void SensitiveScalarImpl(DataChunk &, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
}

static OperatorResultType SensitiveProbeInOut(ExecutionContext &, TableFunctionInput &, DataChunk &, DataChunk &) {
	return OperatorResultType::FINISHED;
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
	// a scan whose sensitive parameter accepts any type next to a typed default, where a lossy comparison
	// against the default must not count as the default
	auto any_signature = FunctionSignature()
	                         .AddParameter(Identifier("endpoint"), LogicalType::VARCHAR)
	                         .AddKeywordOnly(Identifier("level"), LogicalType::ANY, Value::DOUBLE(9007199254740992.0))
	                         .Sensitive();
	TableFunction any_scan(Identifier("sensitive_probe_any_scan"), any_signature, SensitiveProbeScan,
	                       SensitiveProbeBind);
	loader.RegisterFunction(std::move(any_scan));
	// an in-out scan whose sensitive argument is computed by the exported input subquery
	auto inout_signature = FunctionSignature()
	                           .AddParameter(Identifier("endpoint"), LogicalType::VARCHAR)
	                           .AddParameter(Identifier("credential"), LogicalType::VARCHAR)
	                           .Sensitive();
	TableFunction inout_scan(Identifier("sensitive_probe_inout_scan"), inout_signature, nullptr, SensitiveProbeBind);
	inout_scan.in_out_function = SensitiveProbeInOut;
	loader.RegisterFunction(std::move(inout_scan));
	// a volatile scalar function with a sensitive parameter, volatility retains the invocation
	auto scalar_signature =
	    FunctionSignature().AddParameter(Identifier("credential"), LogicalType::VARCHAR).Sensitive();
	scalar_signature.SetReturnType(LogicalType::BIGINT);
	ScalarFunction sensitive_scalar(Identifier("sensitive_probe_scalar"), std::move(scalar_signature),
	                                SensitiveScalarImpl);
	sensitive_scalar.SetVolatile();
	loader.RegisterFunction(std::move(sensitive_scalar));
	// an aggregate with a sensitive parameter, implemented as a renamed copy of the built-in "first"
	{
		Connection lookup(db);
		lookup.BeginTransaction();
		auto &first_entry =
		    Catalog::GetEntry<AggregateFunctionCatalogEntry>(*lookup.context, QualifiedName("system", "main", "first"));
		auto first_implementation =
		    first_entry.functions.GetFunctionByArguments(*lookup.context, {LogicalType::VARCHAR});
		lookup.Rollback();
		REQUIRE(first_implementation);
		AggregateFunction sensitive_agg = *first_implementation;
		sensitive_agg.SetName(Identifier("sensitive_probe_agg"));
		auto agg_signature =
		    FunctionSignature().AddParameter(Identifier("credential"), LogicalType::VARCHAR).Sensitive();
		agg_signature.SetReturnType(sensitive_agg.GetReturnType());
		sensitive_agg.GetSignature() = std::move(agg_signature);
		loader.RegisterFunction(std::move(sensitive_agg));
	}
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
		// the materialized default of the sensitive parameter is registration metadata and renders
		REQUIRE(StringUtil::Contains(text, "api_key"));
	}
	SECTION("an explicit value equal to the declared default renders") {
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection, "SELECT * FROM sensitive_probe_scan('http://endpoint', api_key := '')");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		REQUIRE(exported.IsSuccess());
		REQUIRE(StringUtil::Contains(exported.GetValue().query->ToString(), "api_key"));
	}
	SECTION("a value that only compares equal to the default after a lossy cast refuses") {
		// BIGINT 9007199254740993 converts to the declared DOUBLE default under a coercing comparison
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection, "SELECT * FROM sensitive_probe_any_scan('http://endpoint', level := 9007199254740993)");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		RequireSensitiveParameterIssue(exported, "sensitive_probe_any_scan", "level");
	}
	SECTION("a value of the default's own type equal to the default renders") {
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection,
		    "SELECT * FROM sensitive_probe_any_scan('http://endpoint', level := CAST(9007199254740992 AS DOUBLE))");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		REQUIRE(exported.IsSuccess());
	}
	SECTION("a sensitive argument computed by the exported input subquery refuses") {
		auto plan = OptimizeLogicalPlanExportQuery(
		    connection, "SELECT * FROM sensitive_probe_inout_scan('http://endpoint', (SELECT 'swordfish'))");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *get);
		RequireSensitiveParameterIssue(exported, "sensitive_probe_inout_scan", "credential");
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

TEST_CASE("Sensitive scalar, aggregate and window parameters refuse SQL export",
          "[sql_export][logical_plan_sql_export][sensitive_parameters]") {
	DuckDB db(nullptr);
	Connection connection(db);
	RegisterSensitiveProbes(db);

	auto require_refused = [&](const string &query, const string &function_name) {
		auto refused = connection.Query(query);
		REQUIRE(refused->HasError());
		REQUIRE(StringUtil::Contains(refused->GetError(), function_name));
		REQUIRE(StringUtil::Contains(refused->GetError(), "credential"));
		REQUIRE(!StringUtil::Contains(refused->GetError(), "swordfish"));
	};
	require_refused("EXPLAIN (SQL) SELECT sensitive_probe_scalar('swordfish')", "sensitive_probe_scalar");
	require_refused("EXPLAIN (SQL) SELECT sensitive_probe_agg('swordfish') FROM range(2)", "sensitive_probe_agg");
	require_refused("EXPLAIN (SQL) SELECT sensitive_probe_agg('swordfish') OVER () FROM range(2)",
	                "sensitive_probe_agg");

	// a non-constant argument also refuses - the exporter cannot prove the generated text excludes the value
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE sensitive_scalar_inputs AS SELECT 'swordfish' AS s"));
	require_refused("EXPLAIN (SQL) SELECT sensitive_probe_scalar(s) FROM sensitive_scalar_inputs",
	                "sensitive_probe_scalar");

	// the unmarked built-in the aggregate probe copies keeps exporting
	auto allowed = connection.Query("EXPLAIN (SQL) SELECT first('swordfish') FROM range(2)");
	REQUIRE_NO_FAIL(*allowed);
	REQUIRE(StringUtil::Contains(allowed->Collection().GetValue(1, 0).ToString(), "first"));
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
	REQUIRE(StringUtil::Contains(sql, "visible"));

	// a value-less sensitive option renders as a bare name, there is no value to protect
	auto bare_flag = connection.Query("EXPLAIN (SQL) COPY (SELECT 42 AS i) TO '" + out_path +
	                                  "' (FORMAT 'sensitive_copy_probe', key_flag)");
	REQUIRE_NO_FAIL(*bare_flag);
	REQUIRE(StringUtil::Contains(bare_flag->Collection().GetValue(1, 0).ToString(), "key_flag"));

	auto flag_value = connection.Query("EXPLAIN (SQL) COPY (SELECT 42 AS i) TO '" + out_path +
	                                   "' (FORMAT 'sensitive_copy_probe', key_flag true)");
	REQUIRE(flag_value->HasError());
	REQUIRE(StringUtil::Contains(flag_value->GetError(), "key_flag"));

	// generic binding consumes batch_size into resolved sink state, the sensitive registration must still refuse
	auto consumed = connection.Query("EXPLAIN (SQL) COPY (SELECT 42 AS i) TO '" + out_path +
	                                 "' (FORMAT 'sensitive_copy_probe', batch_size 123456)");
	REQUIRE(consumed->HasError());
	REQUIRE(StringUtil::Contains(consumed->GetError(), "batch_size"));
}

} // namespace logical_plan_sql_export_test
