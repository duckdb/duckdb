#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "test_helpers.hpp"
#include "tpcds-extension.hpp"

#include <map>
#include <set>

using namespace duckdb;
using namespace std;

static void tpcds_test_helper(Connection &con, idx_t q) {
	auto sql = TPCDSExtension::GetQuery(q);

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
	PlanDeserializationState state(*con.context);
	auto new_plan = LogicalOperator::Deserialize(deserializer, state);
	new_plan->ResolveOperatorTypes();

	auto statement = make_unique<LogicalPlanStatement>(move(new_plan));
	auto result = con.Query(move(statement));
	REQUIRE(result->success);
	// COMPARE_CSV(result, TPCDSExtension::GetAnswer(0.01, q), true);

	con.Rollback();
}

TEST_CASE("plan serialize tpcds", "[api]") {
	DuckDB db;
	Connection con(db);
	// con.EnableQueryVerification(); // can't because LogicalPlanStatement can't be copied
	con.Query("CALL dsdgen(sf=0.01)");
	tpcds_test_helper(con, 1);
	tpcds_test_helper(con, 2);
	tpcds_test_helper(con, 3);
	tpcds_test_helper(con, 4);
	tpcds_test_helper(con, 5);
	tpcds_test_helper(con, 6);
	tpcds_test_helper(con, 7);
	tpcds_test_helper(con, 8);
	tpcds_test_helper(con, 9);
	// tpcds_test_helper(con, 10); // "right_bindings.find(table_binding) != right_bindings.end()"
	tpcds_test_helper(con, 11);
	// tpcds_test_helper(con, 12); // Failed to bind column reference
	tpcds_test_helper(con, 13);
	tpcds_test_helper(con, 14);
	// tpcds_test_helper(con, 15); // "right_bindings.find(table_binding) != right_bindings.end()"
	// tpcds_test_helper(con, 16); // "op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->type ==
	// LogicalOperatorType::LOGICAL_ANY_JOIN || op->type == LogicalOperatorType::LOGICAL_EXCEPT" tpcds_test_helper(con,
	// 17); // Invalid function serialization key tpcds_test_helper(con, 18); // "right_bindings.find(table_binding) !=
	// right_bindings.end()"
	tpcds_test_helper(con, 19);
	// tpcds_test_helper(con, 20); // Failed to bind column reference

	// TODO(stephwang): below tests have not been run yet
	tpcds_test_helper(con, 21);
	tpcds_test_helper(con, 22);
	tpcds_test_helper(con, 23);
	tpcds_test_helper(con, 24);
	tpcds_test_helper(con, 25);
	tpcds_test_helper(con, 26);
	tpcds_test_helper(con, 27);
	tpcds_test_helper(con, 28);
	tpcds_test_helper(con, 29);
	tpcds_test_helper(con, 30);
	tpcds_test_helper(con, 31);
	tpcds_test_helper(con, 32);
	tpcds_test_helper(con, 33);
	tpcds_test_helper(con, 34);
	tpcds_test_helper(con, 35);
	tpcds_test_helper(con, 36);
	tpcds_test_helper(con, 37);
	tpcds_test_helper(con, 38);
	tpcds_test_helper(con, 39);
	tpcds_test_helper(con, 40);
	tpcds_test_helper(con, 41);
	tpcds_test_helper(con, 42);
	tpcds_test_helper(con, 43);
	tpcds_test_helper(con, 44);
	tpcds_test_helper(con, 45);
	tpcds_test_helper(con, 46);
	tpcds_test_helper(con, 47);
	tpcds_test_helper(con, 48);
	tpcds_test_helper(con, 49);
	tpcds_test_helper(con, 50);
	tpcds_test_helper(con, 51);
	tpcds_test_helper(con, 52);
	tpcds_test_helper(con, 53);
	tpcds_test_helper(con, 54);
	tpcds_test_helper(con, 55);
	tpcds_test_helper(con, 56);
	tpcds_test_helper(con, 57);
	tpcds_test_helper(con, 58);
	tpcds_test_helper(con, 59);
	tpcds_test_helper(con, 60);
	tpcds_test_helper(con, 61);
	tpcds_test_helper(con, 62);
	tpcds_test_helper(con, 63);
	tpcds_test_helper(con, 64);
	tpcds_test_helper(con, 65);
	tpcds_test_helper(con, 66);
	tpcds_test_helper(con, 67);
	tpcds_test_helper(con, 68);
	tpcds_test_helper(con, 69);
	tpcds_test_helper(con, 70);
	tpcds_test_helper(con, 71);
	tpcds_test_helper(con, 72);
	tpcds_test_helper(con, 73);
	tpcds_test_helper(con, 74);
	tpcds_test_helper(con, 75);
	tpcds_test_helper(con, 76);
	tpcds_test_helper(con, 77);
	tpcds_test_helper(con, 78);
	tpcds_test_helper(con, 79);
	tpcds_test_helper(con, 80);
	tpcds_test_helper(con, 81);
	tpcds_test_helper(con, 82);
	tpcds_test_helper(con, 83);
	tpcds_test_helper(con, 84);
	tpcds_test_helper(con, 85);
	tpcds_test_helper(con, 86);
	tpcds_test_helper(con, 87);
	tpcds_test_helper(con, 88);
	tpcds_test_helper(con, 89);
	tpcds_test_helper(con, 90);
	tpcds_test_helper(con, 91);
	tpcds_test_helper(con, 92);
	tpcds_test_helper(con, 93);
	tpcds_test_helper(con, 94);
	tpcds_test_helper(con, 95);
	tpcds_test_helper(con, 96);
	tpcds_test_helper(con, 97);
	tpcds_test_helper(con, 98);
	tpcds_test_helper(con, 99);
}
