#include "catch.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "test_helpers.hpp"
#include "tpch_extension.hpp"

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

void test_deserialization(const string &file_location);

const char *PERSISTENT_FILE_NAME = "serialized_plans.binary";

TEST_CASE("Generate serialized plans file", "[.][serialization]") {
	string file_location;
	if (std::getenv("GEN_PLAN_STORAGE") != nullptr) {
		// there is no way in catch2 to only run a test if explicitly requested. Hidden tests will
		// run when "*" is used - which we do to run slow tests. To avoid re-generating the bin file
		// we require an env variable to be explicitly set.
		//
		// set `GEN_PLAN_STORAGE` as an environment variable to generate the serialized file
		file_location = get_full_file_name(PERSISTENT_FILE_NAME);
	} else {
		file_location = TestCreatePath("serialized_plans.new.binary");
	}

	DuckDB db;
	Connection con(db);
	load_db(con);
	BufferedFileWriter target(db.GetFileSystem(), file_location);
	std::ifstream queries(get_full_file_name("queries.sql"));
	string query;
	while (std::getline(queries, query)) {
		con.BeginTransaction();
		Parser p;
		p.ParseQuery(query);

		Planner planner(*con.context);

		planner.CreatePlan(std::move(p.statements[0]));
		auto plan = std::move(planner.plan);

		BinarySerializer serializer(target);
		serializer.Begin();
		plan->Serialize(serializer);
		serializer.End();

		con.Rollback();
	}

	target.Sync();

	test_deserialization(file_location);
}

TEST_CASE("Test deserialized plans from file", "[.][serialization]") {
	test_deserialization(get_full_file_name(PERSISTENT_FILE_NAME));
}

void test_deserialization(const string &file_location) {
	DuckDB db;
	Connection con(db);
	load_db(con);
	BufferedFileReader file_source(db.GetFileSystem(), file_location.c_str());

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
		auto expected_results = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(expected_plan)), false);
		REQUIRE_NO_FAIL(*expected_results);

		BinaryDeserializer deserializer(file_source);
		deserializer.Set<ClientContext &>(*con.context);
		deserializer.Begin();
		auto deserialized_plan = LogicalOperator::Deserialize(deserializer);
		deserializer.End();

		deserialized_plan->ResolveOperatorTypes();
		auto deserialized_results =
		    con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized_plan)), false);
		REQUIRE_NO_FAIL(*deserialized_results);

		REQUIRE(deserialized_results->Equals(*expected_results));

		con.Rollback();
	}
}
