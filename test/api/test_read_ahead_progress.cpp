#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Read-ahead progress only counts the assignments a thread is decoding", "[api]") {
	auto path = TestCreatePath("read_ahead_progress.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);

	// ten row groups
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers AS SELECT range AS i FROM range(1228800)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET storage_block_prefetch='debug_force_always'"));
	REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=4"));
	REQUIRE_NO_FAIL(con.Query("SET enable_progress_bar=true"));
	REQUIRE_NO_FAIL(con.Query("SET enable_progress_bar_print=false"));

	// after one chunk read-ahead has claimed four row groups, only the first of them is being decoded
	auto stream = con.SendQuery("SELECT i FROM integers");
	REQUIRE_NO_FAIL(*stream);
	auto chunk = stream->Fetch();
	REQUIRE(chunk);
	auto percentage = con.context->GetQueryProgress().GetPercentage();
	REQUIRE(percentage >= 0);
	REQUIRE(percentage < 20);
	stream.reset();

	// with single vector assignments the claimed rows are single vectors as well, buffer only a chunk or two
	REQUIRE_NO_FAIL(con.Query("PRAGMA verify_parallelism"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='16KB'"));
	stream = con.SendQuery("SELECT i FROM integers");
	REQUIRE_NO_FAIL(*stream);
	chunk = stream->Fetch();
	REQUIRE(chunk);
	percentage = con.context->GetQueryProgress().GetPercentage();
	REQUIRE(percentage > 0);
	REQUIRE(percentage < 5);
	stream.reset();
}
