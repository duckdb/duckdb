#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "iostream"

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
	std::cout << "Parsed '" << sql << std::endl;

	Planner planner(*con.context);
	planner.CreatePlan(move(p.statements[0]));
	std::cout << "Created plan " << std::endl;
	auto plan = move(planner.plan);

	Optimizer optimizer(*planner.binder, *con.context);

	plan = optimizer.Optimize(move(plan));
	std::cout << "Optimized plan " << std::endl;

	BufferedSerializer serializer;
	plan->Serialize(serializer);
	std::cout << "Serialized plan " << std::endl;

	auto data = serializer.GetData();
	auto deserializer = BufferedDeserializer(data.data.get(), data.size);
	auto new_plan = LogicalOperator::Deserialize(deserializer, *con.context);
	std::cout << "Deserialized plan " << std::endl;

	printf("%s\n", new_plan->ToString().c_str());

	new_plan = optimizer.Optimize(move(new_plan));
	printf("%s\n", new_plan->ToString().c_str());
}

TEST_CASE("Test plan serialization", "[api]") {
	test_helper("SELECT last_name,COUNT(*) FROM parquet_scan('data/parquet-testing/userdata1.parquet') GROUP BY last_name");
}

TEST_CASE("Test logical_dummy_scan, logical_unnest", "[api]") {
	test_helper("SELECT UNNEST([1, 2, 3]);");
}

TEST_CASE("Test bound_constant_expression", "[api]") {
	test_helper("SELECT 42 as i");
}

TEST_CASE("Test logical_window", "[api]") {
	test_helper("SELECT * FROM (SELECT 42 as i) WINDOW w AS (partition by i)");
}

TEST_CASE("Test logical_set", "[api]") {
	test_helper("SET memory_limit='10GB';");
}

