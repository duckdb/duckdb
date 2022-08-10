#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/planner.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"

#include "tpch-extension.hpp"

#include <map>
#include <set>

using namespace duckdb;
using namespace std;

static void tpch_test_helper(Connection &con, idx_t q) {
	auto sql = TPCHExtension::GetQuery(q);

	con.BeginTransaction();
	Parser p;
	p.ParseQuery(sql);

	Planner planner(*con.context);
	planner.CreatePlan(move(p.statements[0]));
	auto plan = move(planner.plan);

	// TODO try serializing both non-optimized and optimized plans

	Optimizer optimizer(*planner.binder, *con.context);
	plan = optimizer.Optimize(move(plan));

	// printf("Original plan:\n%s\n", plan->ToString().c_str());

	BufferedSerializer serializer;
	plan->Serialize(serializer);

	auto data = serializer.GetData();
	auto deserializer = BufferedDeserializer(data.data.get(), data.size);
	auto new_plan = LogicalOperator::Deserialize(deserializer, *con.context);
	new_plan->ResolveOperatorTypes();

	auto statement = make_unique<LogicalPlanStatement>(move(new_plan));
	auto result = con.Query(move(statement));
	REQUIRE(result->success);
	// COMPARE_CSV(result, TPCHExtension::GetAnswer(0.01, q), true);

	con.Rollback();
}

TEST_CASE("plan serialize tpch", "[api]") {
	DuckDB db;
	Connection con(db);
	// con.EnableQueryVerification(); // can't because LogicalPlanStatement can't be copied
	con.Query("CALL dbgen(sf=0.01)");
	tpch_test_helper(con, 1);
	// tpch_test_helper(con, 2); // Invalid function serialization key
	tpch_test_helper(con, 3);
	tpch_test_helper(con, 4);
	tpch_test_helper(con, 5);
	tpch_test_helper(con, 6);
	tpch_test_helper(con, 7);
	tpch_test_helper(con, 8);
	// tpch_test_helper(con, 9); // Invalid function serialization key
	tpch_test_helper(con, 10);
	tpch_test_helper(con, 11);
	tpch_test_helper(con, 12);
	// tpch_test_helper(con, 13); // Have bind info but no serialization function
	tpch_test_helper(con, 14);
	tpch_test_helper(con, 15);
	// tpch_test_helper(con, 16); // Have bind info but no serialization function
	tpch_test_helper(con, 17);
	tpch_test_helper(con, 18);
	tpch_test_helper(con, 19);
	tpch_test_helper(con, 20);
	// tpch_test_helper(con, 21); // assertion error related to prepared statement
	// tpch_test_helper(con, 22); // assertion error related to prepared statement
}
