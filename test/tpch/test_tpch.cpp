#include "catch.hpp"
#include "test_helpers.hpp"
#include "tpce.hpp"

#include <condition_variable>
#include <thread>

using namespace duckdb;
using namespace std;

static void long_running_dbgen(Connection *con, bool *interrupted, std::condition_variable *cv) {
	auto result = con->Query("call dbgen(sf=10)");
	if (result->HasError() && result->GetError() == "INTERRUPT Error: Interrupted!") {
		*interrupted = true;
	}
	cv->notify_one();
}

TEST_CASE("Test TPC-H Interrupt", "[tpch]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto con = make_uniq<Connection>(*db);

	bool interrupted = false;
	std::condition_variable cv;
	auto background_thread = thread(long_running_dbgen, con.get(), &interrupted, &cv);

	// wait for query to start executing
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	con->Interrupt();

	std::mutex m;
	std::unique_lock<std::mutex> l(m);
	if (cv.wait_for(l, std::chrono::milliseconds(5000)) == std::cv_status::timeout) {
		FAIL("Query was not interrupted in time");
	}

	// wait for the thread
	background_thread.join();

	REQUIRE(interrupted);
}
