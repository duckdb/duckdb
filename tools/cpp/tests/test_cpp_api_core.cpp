#include "catch.hpp"
#include "duckdb_cpp.hpp"

#include "test_cpp_api.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

// ---------------------------------------------------------------------------
// Stable C++ API tests: environment, database options, filesystem, logging,
// exceptions, replacement scans.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: Database GetOption by name and option target scope", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");

	// Database-resolved options carry their declared scope.
	auto option = db.GetOption("allow_community_extensions");
	REQUIRE(option.GetName() == "allow_community_extensions");
	REQUIRE(option.GetTargetScope() == OptionTargetScope::GLOBAL_ONLY);

	// Constructor-built options are unresolved: scope reports Unknown.
	DatabaseOption fresh("memory_limit", "1GB");
	REQUIRE(fresh.GetTargetScope() == OptionTargetScope::UNKNOWN);

	// An alias resolves to its canonical option.
	REQUIRE(db.GetOption("memory_limit").GetName() == "max_memory");

	REQUIRE_THROWS_MATCHES(db.GetOption("no_such_option"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: RenderQuotedIdentifier quotes only when required", "[cpp_api]") {
	using duckdb::cxx::RenderQuotedIdentifier;
	REQUIRE(RenderQuotedIdentifier("col") == "col");
	REQUIRE(RenderQuotedIdentifier("MyCol") == "MyCol");
	REQUIRE(RenderQuotedIdentifier("select") == "\"select\"");
	REQUIRE(RenderQuotedIdentifier("my col") == "\"my col\"");
	REQUIRE(RenderQuotedIdentifier("a\"b") == "\"a\"\"b\"");
}

TEST_CASE("Stable C++API: LibraryVersion reports the engine version", "[cpp_api]") {
	const auto version = duckdb::cxx::LibraryVersion();
	REQUIRE_FALSE(version.empty());

	// The engine agrees; the C entry point reports the same text as a borrowed view.
	duckdb_v2_str raw = {nullptr, 0};
	REQUIRE(duckdb_v2_library_version(&raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(version == std::string(raw.ptr, raw.len));
}

TEST_CASE("Stable C++API: Exception carries the code and message body", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Binder error: GetCode() is the identity, GetRawMessage() the unprefixed body.
	try {
		conn.Execute("SELECT * FROM no_such_table");
		FAIL("expected a Catalog error");
	} catch (const Exception &ex) {
		REQUIRE(ex.GetCode() == DUCKDB_V2_ERROR_DATABASE_CATALOG);
		REQUIRE(std::string(ex.GetRawMessage()).find("no_such_table") != std::string::npos);
		REQUIRE(std::string(ex.GetRawMessage()).rfind("Catalog Error:", 0) != 0);
		// what() is the full prefixed message and contains the body.
		REQUIRE(std::string(ex.what()).rfind("Catalog Error:", 0) == 0);
		REQUIRE(std::string(ex.what()).find(ex.GetRawMessage()) != std::string::npos);
	}

	// Parse error surfaces lazily: ParseSQL only sets up the iterator, the first
	// Next() yields "SELECT 1", and the parse error for "SELEKT 2" surfaces from the
	// Next() that reaches it. Same shape: Parser code, unprefixed body.
	try {
		auto iter = conn.ParseSQL("SELECT 1; SELEKT 2");
		REQUIRE(iter.Next());
		iter.Next();
		FAIL("expected a Parser error");
	} catch (const Exception &ex) {
		REQUIRE(ex.GetCode() == DUCKDB_V2_ERROR_QUERY_PARSER);
		REQUIRE(std::string(ex.GetRawMessage()).rfind("Parser Error:", 0) != 0);
	}
}
TEST_CASE("Stable C++API: Connection::SetOption scope split is visible correctly across connections", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn_a = db.Connect();
	auto conn_b = db.Connect();

	// max_execution_time is LOCAL_DEFAULT: a LOCAL write on conn_a stays
	// invisible to conn_b.
	conn_a.SetOption(DatabaseOption("max_execution_time", "5000"), SettingScope::LOCAL);
	REQUIRE(conn_a.GetOption("max_execution_time").GetValue() == "5000");
	REQUIRE(conn_b.GetOption("max_execution_time").GetValue() != "5000");

	// A GLOBAL write on conn_a is visible identically on conn_b. The options
	// must outlive the borrowed views their getters return.
	conn_a.SetOption(DatabaseOption("memory_limit", "987MB"), SettingScope::GLOBAL);
	auto option_a = conn_a.GetOption("memory_limit");
	auto option_b = conn_b.GetOption("memory_limit");
	auto seen_a = option_a.GetValue();
	auto seen_b = option_b.GetValue();
	REQUIRE_FALSE(seen_a.empty());
	REQUIRE(std::string(seen_a) == std::string(seen_b));

	// A GLOBAL_ONLY option rejects a LOCAL scope.
	REQUIRE_THROWS_MATCHES(conn_a.SetOption(DatabaseOption("allow_community_extensions", "false"), SettingScope::LOCAL),
	                       Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: Connection::GetOption by name and the scopeless SetOption default", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto option = conn.GetOption("allow_community_extensions");
	REQUIRE(option.GetName() == "allow_community_extensions");

	// The scopeless overload uses Automatic scope (SQL `SET` semantics).
	conn.SetOption(DatabaseOption("max_execution_time", "4242"));
	REQUIRE(conn.GetOption("max_execution_time").GetValue() == "4242");

	REQUIRE_THROWS_MATCHES(conn.GetOption("no_such_option_xyz"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: Environment::Open with pre-open options enforces read-only", "[cpp_api]") {
	using namespace duckdb::cxx;

	auto path = duckdb::TestCreatePath("cpp_api_readonly.duckdb");
	duckdb::DeleteDatabase(path);

	Environment env;
	{
		// Seed the database, then close (scope exit) to free the exclusive-open
		// slot for the read-only reopen.
		auto db = env.Open(path);
		auto conn = db.Connect();
		conn.Execute("CREATE TABLE t(i INTEGER)").Drain();
		conn.Execute("INSERT INTO t VALUES (1), (2)").Drain();
	}

	{
		std::vector<DatabaseOption> options;
		options.push_back(DatabaseOption("access_mode", "READ_ONLY"));
		auto ro_db = env.Open(path, options);
		auto ro_conn = ro_db.Connect();

		// Reads see the seeded data. Scoped so the live result is released
		// before the write attempts below.
		{
			auto result = ro_conn.Execute("SELECT count(*) FROM t");
			auto chunk = result.FetchChunk();
			REQUIRE(chunk.GetVector(0).GetValue(0).Get<int64_t>() == 2);
		}

		// Writes are rejected: both DML and DDL.
		REQUIRE_THROWS_AS(ro_conn.Execute("INSERT INTO t VALUES (3)"), Exception);
		REQUIRE_THROWS_AS(ro_conn.Execute("CREATE TABLE u(i INTEGER)"), Exception);

		// The data is unchanged after the rejected write attempts.
		auto after = ro_conn.Execute("SELECT count(*) FROM t");
		REQUIRE(after.FetchChunk().GetVector(0).GetValue(0).Get<int64_t>() == 2);
	}

	duckdb::DeleteDatabase(path);
}
TEST_CASE("Stable C++API: typed exceptions carry their error code", "[cpp_api]") {
	using namespace duckdb::cxx;

	// Each typed exception fixes its code in the implementation; throwing one
	// is how a callback names its error class without any code vocabulary.
	REQUIRE(InvalidInputException("boom").GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE(InterruptException("stop").GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_RUNTIME_INTERRUPT));

	// They are catchable through the Exception base, preserving the code.
	try {
		throw InvalidInputException("bad arg");
	} catch (const Exception &caught) {
		REQUIRE(caught.GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// The base Exception with a raw code still works.
	Exception raw(static_cast<uint32_t>(DUCKDB_V2_ERROR_QUERY_BINDER), "parse boom");
	REQUIRE(raw.GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_QUERY_BINDER));

	// A thrown-and-caught engine error classifies back correctly end to end.
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	try {
		conn.Execute("SELECT * FROM no_such_table_xyz");
		FAIL("expected a Catalog error");
	} catch (const Exception &caught) {
		REQUIRE(caught.GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_DATABASE_CATALOG));
	}
}
