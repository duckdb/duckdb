#include "catch.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <string>
#include <thread>

using namespace duckdb;
using namespace std;

namespace {

struct InterruptedQuery {
	string error;
	int64_t elapsed_ms = 0;
};

//! Run sql on a background thread and interrupt it after delay_ms. A negative delay runs it to completion.
InterruptedQuery RunAndInterrupt(Connection &con, const string &sql, int64_t delay_ms) {
	InterruptedQuery outcome;
	const auto start = std::chrono::steady_clock::now();
	std::thread runner([&]() {
		auto result = con.Query(sql);
		if (result->HasError()) {
			outcome.error = result->GetError();
		}
	});
	if (delay_ms >= 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
		con.Interrupt();
	}
	runner.join();
	outcome.elapsed_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	return outcome;
}

} // namespace

// The window Finalize stages materialize their sort in a single task while the other threads of
// the hash group wait on it. Interrupting the query while that task runs must surface the
// interrupt. A waiting thread that consumed the unwritten sort output instead would raise an
// InternalException, and abort the process in builds with CRASH_ON_ASSERT.
TEST_CASE("Test interrupting a parallel window finalize", "[window]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=8"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS "
	                          "SELECT ((i * 2654435761) % 1000003)::VARCHAR AS s, i AS v "
	                          "FROM range(500000) tbl(i)"));

	// count(DISTINCT) sorts through WindowDistinctAggregator, the secondary ORDER BY sorts through
	// WindowMergeSortTree, and the partitions keep several of those sorts in flight at once.
	const string sql = "SELECT count(DISTINCT s) OVER w AS a, "
	                   "       first_value(v ORDER BY s) OVER w AS b, "
	                   "       first_value(v ORDER BY s DESC) OVER w AS c "
	                   "FROM t WINDOW w AS (PARTITION BY v % 64)";

	// The interrupt has to land inside the sort stages, so aim it at fractions of the query's own
	// runtime rather than at a fixed delay.
	const auto baseline_ms = RunAndInterrupt(con, sql, -1).elapsed_ms;
	REQUIRE(baseline_ms > 0);

	for (idx_t attempt = 0; attempt < 18; ++attempt) {
		const auto delay_ms = baseline_ms * (1 + NumericCast<int64_t>(attempt % 9)) / 10;
		auto outcome = RunAndInterrupt(con, sql, delay_ms);

		INFO("interrupt after " << delay_ms << " ms of a " << baseline_ms << " ms query: " << outcome.error);
		REQUIRE(outcome.error.find("INTERNAL") == string::npos);

		// An internal error invalidates the database, so this fails as well if one was raised.
		REQUIRE_NO_FAIL(con.Query("SELECT 42"));
	}
}
