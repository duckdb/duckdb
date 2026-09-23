#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "sql_export_test_helpers.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/planner.hpp"
#include <stdexcept>
#include <type_traits>
#include "logical_plan_sql_export_test_helpers.hpp"

using namespace duckdb;

namespace logical_plan_sql_export_test {

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

TEST_CASE("Logical plan SQL export rejects opaque table sources",
          "[sql_export][logical_plan_sql_export][table_source_sql]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();

	SECTION("opaque source") {
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT * FROM range(1)");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		get->Cast<LogicalGet>().function.to_sql = [](ClientContext &, const LogicalGet &) -> TableFunctionToSQLResult {
			return {nullptr, "test_opaque"};
		};
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *get);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.construct->function->name == "range");
		REQUIRE(issue.facts.size() == 1);
		REQUIRE((issue.facts[0] == pair<string, Value> {"guard", Value("test_opaque")}));
	}
	SECTION("process-local binding input") {
		auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT * FROM range(1)");
		auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
		REQUIRE(get);
		get->Cast<LogicalGet>().bind_info = make_shared_ptr<TableFunctionInfo>();
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *get);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE);
		REQUIRE((result.GetIssues()[0].facts[0] == pair<string, Value> {"guard", Value("process_local_input")}));
	}
	SECTION("retained invocation and absent callback") {
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
		REQUIRE(LogicalPlanSQLExporter::Export(*connection.context, *copy).IsSuccess());
	}
	connection.Rollback();
}

TEST_CASE("Source SQL callbacks preserve centrally applied scan modifiers",
          "[sql_export][logical_plan_sql_export][table_source_sql]") {
	DuckDB db(nullptr);
	Connection connection(db);
	ExtensionLoader loader(*db.instance, "sql_export_wrapper");
	auto register_wrapper = [&](const Identifier &original, const Identifier &name, const LogicalType &argument) {
		auto &entry = loader.GetTableFunction(original);
		auto function = *entry.functions.GetFunctionByArguments(*connection.context, {argument});
		function.name = name;
		function.to_sql = [](ClientContext &, const LogicalGet &get) -> TableFunctionToSQLResult {
			vector<unique_ptr<ParsedExpression>> arguments;
			for (auto &parameter : get.parameters) {
				arguments.push_back(ConstantExpression::FromValue(parameter));
			}
			for (auto &parameter : get.named_parameters) {
				arguments.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
				                                                    make_uniq<ColumnRefExpression>(parameter.first),
				                                                    ConstantExpression::FromValue(parameter.second)));
			}
			auto source = make_uniq<TableFunctionRef>();
			source->function = make_uniq<FunctionExpression>(get.function.GetQualifiedName(), std::move(arguments));
			return {std::move(source), {}};
		};
		loader.RegisterFunction(std::move(function));
	};
	register_wrapper("range", "export_wrapped_range", LogicalType::BIGINT);
	string sql;
	bool ordinal = false;
	bool file_filter = false;
	SECTION("ordinary invocation") {
		sql = "SELECT * FROM export_wrapped_range(3)";
	}
	SECTION("ordinality window fusion") {
		sql = "SELECT * FROM export_wrapped_range(3) WITH ORDINALITY";
		ordinal = true;
	}
	if (db.ExtensionIsLoaded("parquet")) {
		SECTION("file pruning residual") {
			register_wrapper("read_parquet", "export_wrapped_parquet", LogicalType::VARCHAR);
			auto directory = TestJoinPath(TestDirectoryPath(), "sql_export_wrapper");
			TestCreateDirectory(directory);
			REQUIRE_NO_FAIL(connection.Query("COPY (SELECT 1 id) TO '" + directory + "/a.parquet' (FORMAT PARQUET)"));
			REQUIRE_NO_FAIL(connection.Query("COPY (SELECT 2 id) TO '" + directory + "/b.parquet' (FORMAT PARQUET)"));
			sql = "SELECT id FROM export_wrapped_parquet('" + directory +
			      "/*.parquet', filename=true) WHERE filename LIKE '%/a.parquet'";
			file_filter = true;
		}
	}
	auto native = connection.Query(sql);
	REQUIRE_NO_FAIL(*native);
	connection.BeginTransaction();
	auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
	auto get_op = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
	REQUIRE(get_op);
	auto &get = get_op->Cast<LogicalGet>();
	if (ordinal) {
		REQUIRE(FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_WINDOW));
		REQUIRE(get.source_ordinality == OrdinalityType::WITH_ORDINALITY);
		REQUIRE_FALSE(get.ordinality_idx.IsValid());
	}
	if (file_filter) {
		REQUIRE(get.extra_info.file_filter_expressions);
		REQUIRE_FALSE(get.extra_info.file_filter_expressions->empty());
		REQUIRE_FALSE(get.extra_info.file_filters.empty());
	}
	for (bool copy : {false, true}) {
		CAPTURE(copy);
		if (copy) {
			plan = plan->Copy(*connection.context);
			plan->ResolveOperatorTypes();
		}
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		REQUIRE(exported.IsSuccess());
		auto generated_sql = exported.GetValue().query->ToString();
		if (ordinal) {
			REQUIRE(StringUtil::Contains(generated_sql, "WITH ORDINALITY"));
		}
		auto generated = connection.Query(generated_sql);
		REQUIRE_NO_FAIL(*generated);
		REQUIRE(generated->GetTypes() == native->GetTypes());
		REQUIRE(generated->RowCount() == native->RowCount());
		for (idx_t row = 0; row < native->RowCount(); row++) {
			for (idx_t column = 0; column < native->ColumnCount(); column++) {
				REQUIRE(Value::NotDistinctFrom(generated->GetValue(column, row), native->GetValue(column, row)));
			}
		}
	}
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
	auto directory = TestCreatePath("sql_export_file_predicates");
	TestDeleteDirectory(directory);
	REQUIRE_NO_FAIL(
	    connection.Query("COPY (SELECT * FROM (VALUES (10,1), (10,1), (NULL,1), (20,2), (30,NULL)) t(i,p)) TO " +
	                     Value(directory).ToSQLString() + " (FORMAT parquet, PARTITION_BY(p))"));
	connection.BeginTransaction();
	auto source = "read_parquet(" + Value(directory + "/*/*.parquet").ToSQLString() + ", hive_partitioning := true)";
	auto sql = "SELECT i FROM " + source + " WHERE p = 1 ORDER BY i";
	auto plan = OptimizeLogicalPlanExportQuery(connection, sql);
	auto get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET);
	REQUIRE(get);
	auto &info = get->Cast<LogicalGet>().extra_info;
	REQUIRE(info.file_filter_expressions);
	REQUIRE_FALSE(info.file_filter_expressions->empty());
	auto copy = plan->Copy(*connection.context);
	auto copied_get = FindLogicalPlanExportOperator(*copy, LogicalOperatorType::LOGICAL_GET);
	REQUIRE(copied_get);
	auto &copied_info = copied_get->Cast<LogicalGet>().extra_info;
	REQUIRE(copied_info.file_filter_expressions);
	REQUIRE(copied_info.file_filter_expressions->size() == info.file_filter_expressions->size());
	for (idx_t index = 0; index < info.file_filter_expressions->size(); index++) {
		REQUIRE((*copied_info.file_filter_expressions)[index]->Equals(*(*info.file_filter_expressions)[index]));
	}
	REQUIRE(LogicalPlanSQLExporter::Export(*connection.context, *copy).IsSuccess());
	info.file_filter_expressions.reset();
	auto missing = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(missing.HasError());
	REQUIRE(missing.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("file_filter_residual"));
	connection.Rollback();
	TestDeleteDirectory(directory);
}

TEST_CASE("Table row number SQL export retains stream effects",
          "[sql_export][logical_plan_sql_export][table_row_number_sql_export]") {
	for (auto consumption : {0, 1, 2}) {
		for (bool late_error : {false, true}) {
			vector<string> expected_rows;
			Value expected_sequence;
			bool expected_error = false;
			for (auto route : {0, 3, 4, 5}) {
				if (route >= 4 && (consumption != 0 || late_error)) {
					continue;
				}
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
				if (route == 4) {
					plan = plan->Copy(*connection.context);
					plan->ResolveOperatorTypes();
				}
				auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan);
				REQUIRE(exported.IsSuccess());
				QueryParameters parameters;
				parameters.result_eagerness = ResultEagerness::AUTO;
				unique_ptr<QueryResult> result;
				plan.reset();
				if (route == 0) {
					result = SubmitSQLExportResult(*connection.context, sql, parameters);
				} else if (route == 3 || route == 4) {
					result =
					    SubmitSQLExportResult(*connection.context, exported.GetValue().query->ToString(), parameters);
				} else {
					auto statement = make_uniq<SelectStatement>();
					statement->node = std::move(exported.GetValue().query);
					result = SubmitSQLExportResult(*connection.context, std::move(statement), parameters);
				}
				unique_ptr<QueryResultStream> stream;
				if (!result->HasError()) {
					stream = make_uniq<QueryResultStream>(std::move(result));
				}
				vector<string> rows;
				while (stream && !stream->HasError()) {
					auto chunk = stream->Fetch();
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
				auto has_error = (stream ? stream->HasError() : result->HasError());
				if (has_error) {
					REQUIRE(
					    StringUtil::Contains((stream ? stream->GetError() : result->GetError()), "row number stream"));
				}
				stream.reset();
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
	}
	auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT i,row_number() OVER () FROM filtered_numbers");
	auto &get = FindLogicalPlanExportOperator(*plan, LogicalOperatorType::LOGICAL_GET)->Cast<LogicalGet>();
	get.dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
	REQUIRE_FALSE(get.function.to_sql(*connection.context, get).source);
	connection.Rollback();
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

} // namespace logical_plan_sql_export_test
