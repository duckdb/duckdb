#include <duckdb/execution/executor.hpp>
#include <future>
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

class ProgressCheck {
public:
	bool finished = false;
	Executor *executor = nullptr;
	thread progress_bar_thread;
	promise<void> exit_signal;
	future<void> future_obj = exit_signal.get_future();
	int cur_percentage = 0;

	explicit ProgressCheck(Executor *executor)
	    : executor(executor) {

	      };

	void GetProgress() {
		while (!StopRequested()) {
			assert(cur_percentage <= executor->GetPipelinesProgress() && executor->GetPipelinesProgress() <= 100);
			cur_percentage = executor->GetPipelinesProgress();
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}

	void Start() {
		exit_signal = promise<void>();
		cur_percentage = 0;
		progress_bar_thread = std::thread(&ProgressCheck::GetProgress, this);
	}

	bool StopRequested() {
		if (future_obj.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout) {
			return false;
		}
		return true;
	}

	void Stop() {
		if (progress_bar_thread.joinable()) {
			exit_signal.set_value();
			progress_bar_thread.join();
		}
	}
};

TEST_CASE("Test Progress Bar", "[api]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	ProgressCheck progress_check(&con.context->executor);
	REQUIRE_NO_FAIL(con.Query("create  table tbl as select range a from range(100000000);"));
	REQUIRE_NO_FAIL(con.Query("create  table tbl_2 as select range a from range(1000);"));
	//! Simple Aggregation
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl"));
	progress_check.Stop();
	//! Simple Join
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl inner join tbl_2 on (tbl.a = tbl_2.a)"));
	progress_check.Stop();

	//! Test Multiple threads
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=4"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_parallelism"));
	//! Simple Aggregation
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl"));
	progress_check.Stop();
	//! Simple Join
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl inner join tbl_2 on (tbl.a = tbl_2.a)"));
	progress_check.Stop();
}