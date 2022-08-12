#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/planner.hpp"
#include "test_helpers.hpp"

#include <map>
#include <set>

using namespace duckdb;
using namespace std;

static void test_helper(string sql, vector<string> fixtures = vector<string>()) {
	DuckDB db;
	Connection con(db);

	for (const auto &fixture : fixtures) {
		con.SendQuery(fixture);
	}

	Parser p;
	p.ParseQuery(sql);
//	printf("\nParsed query '%s'\n", sql.c_str());

	int i = 0;
	for (auto &statement : p.statements) {
		con.context->transaction.BeginTransaction();
		// Should that be the default "ToString"?
		string statement_sql(statement->query.c_str() + statement->stmt_location, statement->stmt_length);
//		printf("[%d] Processing statement '%s'\n", i, statement_sql.c_str());
		Planner planner(*con.context);
		planner.CreatePlan(move(statement));
//		printf("[%d] Created plan\n", i);
		auto plan = move(planner.plan);

		Optimizer optimizer(*planner.binder, *con.context);

		plan = optimizer.Optimize(move(plan));
//		printf("[%d] Optimized plan\n", i);

		BufferedSerializer serializer;
		plan->Serialize(serializer);
//		printf("[%d] Serialized plan\n", i);

		auto data = serializer.GetData();
		auto deserializer = BufferedDeserializer(data.data.get(), data.size);
		PlanDeserializationState state(*con.context);
		auto new_plan = LogicalOperator::Deserialize(deserializer, state);
//		printf("[%d] Deserialized plan\n", i);

//		printf("[%d] Original plan:\n%s\n", i, plan->ToString().c_str());
//		printf("[%d] New plan:\n%s\n", i, new_plan->ToString().c_str());

		auto optimized_plan = optimizer.Optimize(move(new_plan));
//		printf("[%d] Optimized plan:\n%s\n", i, optimized_plan->ToString().c_str());
		con.context->transaction.Commit();
		++i;
	}
}

TEST_CASE("Test plan serialization", "[serialization]") {
	test_helper(
	    "SELECT last_name,COUNT(*) FROM parquet_scan('data/parquet-testing/userdata1.parquet') GROUP BY last_name");
}

TEST_CASE("Test logical_dummy_scan", "[serialization]") {
	test_helper("SELECT [1, 2, 3]");
}

TEST_CASE("Test logical_unnest", "[serialization]") {
	test_helper("SELECT UNNEST([1, 2, 3])");
}

TEST_CASE("Test bound_constant_expression", "[serialization]") {
	test_helper("SELECT 42 as i");
}

TEST_CASE("Test logical_window", "[serialization]") {
	test_helper("SELECT * FROM (SELECT 42 as i) WINDOW w AS (partition by i)");
}

TEST_CASE("Test logical_set", "[serialization]") {
	test_helper("SET memory_limit='10GB'");
}

TEST_CASE("Test logical_sample", "[serialization]") {
	test_helper("SELECT * FROM (SELECT 42 as i) USING SAMPLE RESERVOIR(20%)");
}

TEST_CASE("Test logical_limit_percent", "[serialization]") {
	test_helper("SELECT 42 LIMIT 35%");
}

TEST_CASE("Test logical_limit", "[serialization]") {
	test_helper("SELECT 42 LIMIT 1");
}

TEST_CASE("Test logical_comparison_join", "[serialization]") {
	test_helper("SELECT * FROM (SELECT 42 as i), (SELECT 42 as j) WHERE i = j");
}

TEST_CASE("Test logical_any_join", "[serialization]") {
	test_helper("SELECT * FROM (SELECT 42 as i), (SELECT 42 as j) WHERE i = j OR i > 1");
}

TEST_CASE("Test logical_show", "[serialization]") {
	test_helper("SHOW SELECT 42");
}

TEST_CASE("Test bound_comparison_expression", "[serialization]") {
	test_helper("SELECT COUNT(*) FILTER (WHERE i = 1) FROM (SELECT 42 as i)");
}

TEST_CASE("Test logical_filter", "[serialization]") {
	test_helper("SELECT COUNT(*) FROM (SELECT 42 as i) HAVING COUNT(*) >= 1");
}

TEST_CASE("Test logical_expression_get", "[serialization]") {
	test_helper("SELECT * FROM (VALUES\n"
	            "  ([1]),\n"
	            "  ([NULL]),\n"
	            "  ([]),\n"
	            "  ([9,10,11]),\n"
	            "  (NULL)\n"
	            "  ) lv(pk);");
}

TEST_CASE("Test logical_explain", "[serialization]") {
	test_helper("EXPLAIN SELECT 42");
}

TEST_CASE("Test logical_empty_result", "[serialization]") {
	test_helper("SELECT * FROM (SELECT 42) WHERE 1>2");
}

TEST_CASE("Test create_table", "[serialization]") {
	test_helper("CREATE TABLE tbl (foo INTEGER)");
}

TEST_CASE("Test insert_into", "[serialization]") {
	test_helper("INSERT INTO tbl VALUES(1)", {"CREATE TABLE tbl (foo INTEGER)"});
}

TEST_CASE("Test logical_distinct", "[serialization]") {
	test_helper("SELECT DISTINCT(i) FROM (SELECT 42 as i)");
}

TEST_CASE("Test logical_delete", "[serialization]") {
	test_helper("DELETE FROM tbl", {"CREATE TABLE tbl (foo INTEGER)"});
}

TEST_CASE("Test logical_cteref", "[serialization]") {
	test_helper("with cte1 as (select 42), cte2 as (select * from cte1) select * FROM cte2");
}

// TODO(stephwang): check why we lost x<3 expression in filter
TEST_CASE("Test logical_recursive_cte", "[serialization]") {
	test_helper("with recursive t as (select 1 as x union all select x+1 from t where x < 3) select * from t");
}

TEST_CASE("Test logical_cross_product", "[serialization]") {
	test_helper("SELECT * FROM (SELECT 42 as i) CROSS JOIN (SELECT 42 as j)");
}

TEST_CASE("Test logical_create_index", "[serialization]") {
	test_helper("CREATE INDEX idx ON tbl (foo)", {"CREATE TABLE tbl (foo INTEGER)"});
}

TEST_CASE("Test logical_create_schema", "[serialization]") {
	test_helper("CREATE SCHEMA test");
}

TEST_CASE("Test logical_create_view", "[serialization]") {
	test_helper("CREATE VIEW test_view AS (SELECT 42)");
}

TEST_CASE("Test logical_top_n", "[serialization]") {
	test_helper("SELECT * FROM tbl ORDER BY foo LIMIT 1", {"CREATE TABLE tbl (foo INTEGER)"});
}

TEST_CASE("Test logical_update", "[serialization]") {
	test_helper("UPDATE tbl SET foo=42", {"CREATE TABLE tbl (foo INTEGER)"});
}

// TODO(stephwang): revisit this later since it doesn't work yet
//TEST_CASE("Test logical_copy_to_file", "[serialization]") {
//	test_helper("COPY tbl TO 'test_table.csv' ( DELIMITER '|', HEADER )", {"CREATE TABLE tbl (foo INTEGER)"});
//}

// TODO(stephwang): revisit this later since it doesn't work yet
//TEST_CASE("Test logical_prepare", "[serialization]") {
//	test_helper("PREPARE v1 AS SELECT 42");
//}
