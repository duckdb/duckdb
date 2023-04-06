#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "test_helpers.hpp"
#include "tpch-extension.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

string get_full_file_name(const string &file_name) {
	auto my_name = string("test_plan_serialization_bwc.cpp");
	auto path = string(__FILE__);
	return path.replace(path.rfind(my_name), my_name.length(), file_name);
}

void load_db(Connection &con) {
	std::ifstream queries(get_full_file_name("db_load.sql"));
	string query;
	while (std::getline(queries, query)) {
		REQUIRE_NO_FAIL(con.Query(query));
	}
}

TEST_CASE("Generate serialized plans file", "[.]") {
	if (std::getenv("GEN_PLAN_STORAGE") == nullptr) {
		// there is no way in catch2 to only run a test if explicitly requested. Hidden tests will
		// run when "*" is used - which we do to run slow tests. To avoid re-generating the bin file
		// we require an env variable to be explicitly set.
		INFO("set `GEN_PLAN_STORAGE` as an environment variable to generate the serialized file");
		return;
	}
	DuckDB db;
	Connection con(db);
	load_db(con);
	BufferedFileWriter serializer(db.GetFileSystem(), get_full_file_name("serialized_plans.binary"));
	serializer.SetVersion(PLAN_SERIALIZATION_VERSION);
	serializer.Write(serializer.GetVersion());
	std::ifstream queries(get_full_file_name("queries.sql"));
	string query;
	while (std::getline(queries, query)) {
		con.BeginTransaction();
		Parser p;
		p.ParseQuery(query);

		Planner planner(*con.context);

		planner.CreatePlan(std::move(p.statements[0]));
		auto plan = std::move(planner.plan);
		plan->Serialize(serializer);

		con.Rollback();
	}

	serializer.Sync();
}

TEST_CASE("Test deserialized plans from file", "[.][serialization]") {
	DuckDB db;
	Connection con(db);
	load_db(con);
	BufferedFileReader deserializer(db.GetFileSystem(), get_full_file_name("serialized_plans.binary").c_str());
	deserializer.SetVersion(deserializer.Read<uint64_t>());

	std::ifstream queries(get_full_file_name("queries.sql"));
	string query;
	while (std::getline(queries, query)) {
		INFO("evaluating " << query)
		con.BeginTransaction();
		Parser p;
		p.ParseQuery(query);
		Planner planner(*con.context);
		planner.CreatePlan(std::move(p.statements[0]));
		auto expected_plan = std::move(planner.plan);
		expected_plan->ResolveOperatorTypes();
		auto expected_results = con.context->Query(make_unique<LogicalPlanStatement>(std::move(expected_plan)), false);
		REQUIRE_NO_FAIL(*expected_results);

		PlanDeserializationState state(*con.context);
		auto deserialized_plan = LogicalOperator::Deserialize(deserializer, state);
		deserialized_plan->ResolveOperatorTypes();
		auto deserialized_results =
		    con.context->Query(make_unique<LogicalPlanStatement>(std::move(deserialized_plan)), false);
		REQUIRE_NO_FAIL(*deserialized_results);

		REQUIRE(deserialized_results->Equals(*expected_results));

		con.Rollback();
	}
}
