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

static void test_helper(string sql) {
	DuckDB db;
	Connection con(db);

	Parser p;
	p.ParseQuery(sql);
	printf("\nParsed query\n");

	Planner planner(*con.context);
	planner.CreatePlan(move(p.statements[0]));
	printf("Created plan\n");
	auto plan = move(planner.plan);

	Optimizer optimizer(*planner.binder, *con.context);

	plan = optimizer.Optimize(move(plan));
	printf("Optimized plan\n");

	BufferedSerializer serializer;
	plan->Serialize(serializer);
	printf("Serialized plan\n");

	auto data = serializer.GetData();
	auto deserializer = BufferedDeserializer(data.data.get(), data.size);
	auto new_plan = LogicalOperator::Deserialize(deserializer, *con.context);
	printf("Deserialized plan\n");

	printf("Original plan:\n%s\n", plan->ToString().c_str());
	printf("New plan:\n%s\n", new_plan->ToString().c_str());

	auto optimized_plan = optimizer.Optimize(move(new_plan));
	printf("Optimized plan:\n%s\n", optimized_plan->ToString().c_str());
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
