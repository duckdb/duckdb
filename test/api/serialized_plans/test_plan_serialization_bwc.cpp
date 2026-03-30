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

TEST_CASE("Test specific serialized plans", "[.][serialization]") {
	DuckDB db;

	auto &fs = db.GetFileSystem();
	auto dir_location = get_full_file_name("cases");

	std::vector<std::pair<string, string>> plans;

	fs.ListFiles(dir_location, [&](const string &entry, bool is_dir) {
		if (is_dir) {
			return;
		}
		if (entry.rfind(".bin") != string::npos) {
			auto query_file = entry.substr(0, entry.size() - 4) + ".sql";
			auto query_path = fs.JoinPath(dir_location, query_file);
			auto plan_path = fs.JoinPath(dir_location, entry);

			if (!fs.FileExists(query_path)) {
				fprintf(stderr, "Query file %s does not exist for plan file %s\n", query_file.c_str(), entry.c_str());
				FAIL("Query file does not exist for plan file");
			}

			plans.emplace_back(plan_path, query_path);
		}
	});

	for (auto &entry : plans) {
		auto &bin_file = entry.first;
		auto &sql_file = entry.second;

		// First, read the query from the sql file
		std::ifstream query_stream(sql_file);
		std::vector<string> statements;
		string line;
		while (std::getline(query_stream, line)) {
			if (line.empty()) {
				continue;
			}
			statements.push_back(line);
		}

		// Open a connection and execute all statements except the last one
		// (which is the target statement we want to test)
		Connection con(db);

		con.BeginTransaction();

		for (idx_t i = 0; i < statements.size() - 1; i++) {
			REQUIRE_NO_FAIL(con.Query(statements[i]));
		}

		auto target_stmt = statements.back();

		// Now read the serialized plan
		BufferedFileReader file_source(db.GetFileSystem(), bin_file.c_str());

		BinaryDeserializer deserializer(file_source);
		deserializer.Set<ClientContext &>(*con.context);
		deserializer.Begin();
		auto deserialized_plan = LogicalOperator::Deserialize(deserializer);
		deserializer.End();

		deserialized_plan->ResolveOperatorTypes();

		auto deserialized_results =
		    con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized_plan)), false);
		REQUIRE_NO_FAIL(*deserialized_results);

		// Now execute the original statement as well and compare results
		Parser p;
		p.ParseQuery(target_stmt);
		Planner planner(*con.context);
		planner.CreatePlan(std::move(p.statements[0]));
		auto expected_plan = std::move(planner.plan);
		expected_plan->ResolveOperatorTypes();
		auto expected_results = con.Query(target_stmt);
		REQUIRE_NO_FAIL(*expected_results);

		if (deserialized_results->names.size() == expected_results->names.size()) {
			// ignore names
			deserialized_results->names = expected_results->names;
		}

		if (!deserialized_results->Equals(*expected_results)) {
			fprintf(stderr, "-----------------------------------\n");
			fprintf(stderr, "Deserialized result does not match!\n");
			fprintf(stderr, "-----------------------------------\n");
			fprintf(stderr, "Query: %s\n", target_stmt.c_str());
			fprintf(stderr, "-------------Deserialized----------\n");
			deserialized_results->Print();
			fprintf(stderr, "---------------Expected------------\n");
			expected_results->Print();
			fprintf(stderr, "-----------------------------------\n");
			FAIL("Deserialized result does not match");
		}

		con.Rollback();
	}
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

		BinaryDeserializer deserializer(file_source);
		deserializer.Set<ClientContext &>(*con.context);
		deserializer.Begin();
		auto deserialized_plan = LogicalOperator::Deserialize(deserializer);
		deserializer.End();

		deserialized_plan->ResolveOperatorTypes();
		auto deserialized_results =
		    con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized_plan)), false);
		REQUIRE_NO_FAIL(*deserialized_results);

		Parser p;
		p.ParseQuery(query);
		Planner planner(*con.context);
		planner.CreatePlan(std::move(p.statements[0]));
		auto expected_plan = std::move(planner.plan);
		expected_plan->ResolveOperatorTypes();
		auto expected_results = con.Query(query);
		REQUIRE_NO_FAIL(*expected_results);

		if (deserialized_results->names.size() == expected_results->names.size()) {
			// ignore names
			deserialized_results->names = expected_results->names;
		}

		if (!deserialized_results->Equals(*expected_results)) {
			fprintf(stderr, "-----------------------------------\n");
			fprintf(stderr, "Deserialized result does not match!\n");
			fprintf(stderr, "-----------------------------------\n");
			fprintf(stderr, "Query: %s\n", query.c_str());
			fprintf(stderr, "-------------Deserialized----------\n");
			deserialized_results->Print();
			fprintf(stderr, "---------------Expected------------\n");
			expected_results->Print();
			fprintf(stderr, "-----------------------------------\n");
			FAIL("Deserialized result does not match");
		}

		con.Rollback();
	}
}
