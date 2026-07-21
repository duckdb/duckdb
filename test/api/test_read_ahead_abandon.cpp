#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test abandoning a streaming result mid-scan with read-ahead", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto file = TestCreatePath("readahead_stream_abandon.parquet");
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT range AS i FROM range(500000)) TO '" + file +
	                          "' (FORMAT parquet, ROW_GROUP_SIZE 2048)"));
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=1"));

	for (idx_t i = 0; i < 10; i++) {
		auto stream = con.SendQuery("SELECT i FROM '" + file + "'");
		REQUIRE_NO_FAIL(*stream);
		// fetch a single chunk, then abandon the stream mid-scan
		auto chunk = stream->Fetch();
		REQUIRE(chunk);
		stream.reset();
		// cancelling the scan must not hang, and the connection must remain usable
		auto result = con.Query("SELECT 42");
		REQUIRE_NO_FAIL(*result);
	}
}
