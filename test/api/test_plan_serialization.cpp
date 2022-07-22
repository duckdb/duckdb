#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include "test_helpers.hpp"

#include <map>
#include <set>

using namespace duckdb;
using namespace std;

TEST_CASE("Test plan serialization", "[api]") {
	DuckDB db;
	Connection con(db);

	Parser p;
	p.ParseQuery("SELECT email, last_name FROM parquet_scan('data/parquet-testing/userdata1.parquet')");

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

	printf("%s\n", new_plan->ToString().c_str());

	new_plan = optimizer.Optimize(move(new_plan));
	printf("%s\n", new_plan->ToString().c_str());
}
