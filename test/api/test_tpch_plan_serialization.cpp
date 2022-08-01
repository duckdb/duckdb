#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/planner.hpp"
#include "test_helpers.hpp"

#include "tpch-extension.hpp"

#include <map>
#include <set>

using namespace duckdb;
using namespace std;

static void tpch_test_helper(Connection &con, string sql) {

	con.BeginTransaction();
	Parser p;
	p.ParseQuery(sql);

	Planner planner(*con.context);
	planner.CreatePlan(move(p.statements[0]));
	auto plan = move(planner.plan);

	Optimizer optimizer(*planner.binder, *con.context);

	plan = optimizer.Optimize(move(plan));

	BufferedSerializer serializer;
	plan->Serialize(serializer);

	auto data = serializer.GetData();
	auto deserializer = BufferedDeserializer(data.data.get(), data.size);
	auto new_plan = LogicalOperator::Deserialize(deserializer, *con.context);
	printf("Deserialized plan\n");

	printf("Original plan:\n%s\n", plan->ToString().c_str());
	printf("New plan:\n%s\n", new_plan->ToString().c_str());

	auto optimized_plan = optimizer.Optimize(move(new_plan));
	printf("Optimized plan:\n%s\n", optimized_plan->ToString().c_str());
	con.Rollback();
}

TEST_CASE("plan serialize tpch Q1", "[api]") {
	DuckDB db;
	Connection con(db);
	con.EnableQueryVerification();
	con.Query("CALL dbgen(sf=0)");
	tpch_test_helper(con, TPCHExtension::GetQuery(1));
}
