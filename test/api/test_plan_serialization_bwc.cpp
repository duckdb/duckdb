#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/parser/parser.hpp"
#include "test_helpers.hpp"
#include "tpch-extension.hpp"

using namespace duckdb;
using namespace std;

string get_file_name(FileSystem &fs) {
	auto sep = fs.PathSeparator();
	auto path = string(__FILE__);
	auto last_sep = path.find_last_of(sep);
	return path.substr(0, last_sep) + sep + "serialized_tpch_queries.binary";
}

TEST_CASE("Generate serialized TPCH file", "[.]") {
	DuckDB db;
	Connection con(db);
	con.Query("CALL dbgen(sf=0.01)");
	BufferedFileWriter serializer(db.GetFileSystem(), get_file_name(db.GetFileSystem()));
	serializer.SetVersion(PLAN_SERIALIZATION_VERSION);
	serializer.Write(serializer.GetVersion());
	for (int q = 1; q < 22; q++) {
		auto sql = TPCHExtension::GetQuery(q);

		con.BeginTransaction();
		Parser p;
		p.ParseQuery(sql);

		Planner planner(*con.context);

		planner.CreatePlan(move(p.statements[0]));
		auto plan = move(planner.plan);
		plan->Serialize(serializer);

		con.Rollback();
	}
	serializer.Sync();
}

TEST_CASE("Test deserialize TPCH from file", "[serialization]") {
	DuckDB db;
	Connection con(db);
	con.Query("CALL dbgen(sf=0.01)");
	BufferedFileReader deserializer(db.GetFileSystem(), get_file_name(db.GetFileSystem()).c_str());
	deserializer.SetVersion(deserializer.Read<uint64_t>());

	for (int q = 1; q < 22; q++) {
		auto sql = TPCHExtension::GetQuery(q);

		con.BeginTransaction();
		Parser p;
		p.ParseQuery(sql);

		Planner planner(*con.context);

		planner.CreatePlan(move(p.statements[0]));
		auto plan = move(planner.plan);
		PlanDeserializationState state(*con.context);
		auto read_plan = LogicalOperator::Deserialize(deserializer, state);

		//		printf("[%d] Original plan:\n%s\n", q, plan->ToString().c_str());
		//		printf("[%d] New plan:\n%s\n", q, read_plan->ToString().c_str());

		REQUIRE(read_plan->ToString() == plan->ToString());
		con.Rollback();
	}
}
