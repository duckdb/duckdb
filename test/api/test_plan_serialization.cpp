#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/planner.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"

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

		// LogicalOperator's copy utilizes its serialize and deserialize methods
		auto new_plan = plan->Copy(*con.context);

		auto optimized_plan = optimizer.Optimize(move(new_plan));
		con.context->transaction.Commit();
		++i;
	}
}

TEST_CASE("Test logical_set", "[serialization]") {
	test_helper("SET memory_limit='10GB'");
}

TEST_CASE("Test logical_show", "[serialization]") {
	test_helper("SHOW SELECT 42");
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

TEST_CASE("Test logical_delete", "[serialization]") {
	test_helper("DELETE FROM tbl", {"CREATE TABLE tbl (foo INTEGER)"});
}

// TODO: only select for now
// TEST_CASE("Test logical_create_index", "[serialization]") {
//	test_helper("CREATE INDEX idx ON tbl (foo)", {"CREATE TABLE tbl (foo INTEGER)"});
//}
// TODO: only select for now
// TEST_CASE("Test logical_create_schema", "[serialization]") {
//	test_helper("CREATE SCHEMA test");
//}
// TODO: only select for now
// TEST_CASE("Test logical_create_view", "[serialization]") {
//	test_helper("CREATE VIEW test_view AS (SELECT 42)");
//}

TEST_CASE("Test logical_update", "[serialization]") {
	test_helper("UPDATE tbl SET foo=42", {"CREATE TABLE tbl (foo INTEGER)"});
}

// TODO(stephwang): revisit this later since it doesn't work yet
// TEST_CASE("Test logical_copy_to_file", "[serialization]") {
//	test_helper("COPY tbl TO 'test_table.csv' ( DELIMITER '|', HEADER )", {"CREATE TABLE tbl (foo INTEGER)"});
//}

// TODO(stephwang): revisit this later since it doesn't work yet
// TEST_CASE("Test logical_prepare", "[serialization]") {
//	test_helper("PREPARE v1 AS SELECT 42");
//}
