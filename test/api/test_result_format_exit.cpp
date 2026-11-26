#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/main/result_format.hpp"

#include <chrono>
#include <thread>

#if !defined(_WIN32) && !defined(DUCKDB_NO_THREADS)
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace duckdb;

TEST_CASE("Test that the default result formats are shared", "[api]") {
	auto &in_memory = ChunkFormat::InMemory();
	auto &buffer_managed = ChunkFormat::BufferManaged();
	REQUIRE(in_memory);
	REQUIRE(buffer_managed);
	REQUIRE(in_memory.get() != buffer_managed.get());
	REQUIRE(&ChunkFormat::InMemory() == &in_memory);
	REQUIRE(&ChunkFormat::BufferManaged() == &buffer_managed);
}

#if !defined(_WIN32) && !defined(DUCKDB_NO_THREADS)
TEST_CASE("Test exiting the process while another thread runs queries", "[api]") {
	for (idx_t i = 0; i < 20; i++) {
		pid_t pid = fork();
		REQUIRE(pid >= 0);
		if (pid == 0) {
			// child process: keep running queries on another thread while this process exits
			static DuckDB *db = new DuckDB(nullptr);
			std::thread worker([]() {
				Connection con(*db);
				while (true) {
					con.Query("SELECT 42");
				}
			});
			worker.detach();
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
			exit(0);
		}
		int status = 0;
		REQUIRE(waitpid(pid, &status, 0) == pid);
		REQUIRE(WIFEXITED(status));
		REQUIRE(WEXITSTATUS(status) == 0);
	}
}
#endif
