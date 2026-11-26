#include "catch.hpp"
#include "test_environment.hpp"
#include "test_exception_matcher.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <thread>

using namespace duckdb;

static bool RunnerRegressionEnabled() {
	auto value = std::getenv("DUCKDB_TEST_RUNNER_REGRESSION");
	return value && string(value) == "1";
}

TEST_CASE("Exception matcher preserves error types and raw messages", "[test_runner]") {
	auto matcher = ExceptionMatches(ExceptionType::OUT_OF_MEMORY, Catch::Contains("allocation"));
	REQUIRE(matcher.match(Exception(ExceptionType::OUT_OF_MEMORY, "allocation")));
	REQUIRE_FALSE(matcher.match(IOException("allocation")));
	REQUIRE_THAT(matcher.toString(), Catch::Contains("Out of Memory") && Catch::Contains("IO"));
	REQUIRE_FALSE(matcher.match(Exception(ExceptionType::OUT_OF_MEMORY, "different message")));
	REQUIRE_THAT(matcher.toString(), Catch::Contains("different message"));
	REQUIRE_FALSE(matcher.match(std::runtime_error("allocation")));

	string raw = "allocation for \"quoted input\"\nfailed";
	REQUIRE_THROWS_MATCHES(throw Exception(ExceptionType::OUT_OF_MEMORY, raw), std::exception,
	                       ExceptionMatches(ExceptionType::OUT_OF_MEMORY, Catch::Equals(raw)));
}

TEST_CASE("Runner regression passing", "[test_runner]") {
	REQUIRE(1 == 1);
	REQUIRE(2 == 2);
	if (RunnerRegressionEnabled()) {
		REQUIRE(TestLocalFileSystem().DirectoryExists(GetTempDirHome()));
	}
}

TEST_CASE("Failure summaries follow nested test environments", "[test_runner]") {
	TestEnvironmentScope outer;
	TestConfiguration::Get().ParseOption("summarize_failures", Value(true));
	REQUIRE_FALSE(FailureSummary::SkipLoggingSameError("outer.test"));
	REQUIRE(FailureSummary::SkipLoggingSameError("outer.test"));
	FailureSummary::Log("outer diagnostic");
	REQUIRE(FailureSummary::GetSummaryCounter() == 1);
	try {
		TestEnvironmentScope inner;
		TestConfiguration::Get().ParseOption("summarize_failures", Value(true));
		REQUIRE(FailureSummary::GetFailureSummary().empty());
		REQUIRE_FALSE(FailureSummary::SkipLoggingSameError("outer.test"));
		REQUIRE(FailureSummary::GetSummaryCounter() == 1);
		FailureSummary::Log("inner diagnostic");
		throw std::runtime_error("leave inner environment");
	} catch (const std::runtime_error &) {
	}
	REQUIRE(FailureSummary::GetFailureSummary() == "outer diagnostic");
	REQUIRE(FailureSummary::SkipLoggingSameError("outer.test"));
	REQUIRE(FailureSummary::GetSummaryCounter() == 2);
}

TEST_CASE("Runner regression failure followed by skip", "[test_runner]") {
	if (!RunnerRegressionEnabled()) {
		return;
	}
	CHECK(1 == 1);
	CHECK(1 == 2);
	SKIP_TEST("require parquet");
}

TEST_CASE("Runner regression failure context", "[test_runner]") {
	if (!RunnerRegressionEnabled()) {
		return;
	}
	int32_t captured = 271828;
	INFO("input=314159");
	CAPTURE(captured);
	std::cout << "stdout fixture context\n";
	std::cerr << "stderr fixture context\n";
	CHECK(1 == 1);
	CHECK(1 == 2);
}

TEST_CASE("Runner regression skip parquet", "[test_runner]") {
	if (RunnerRegressionEnabled()) {
		SKIP_TEST("require parquet");
	}
}

TEST_CASE("Runner regression skip httpfs", "[test_runner]") {
	if (RunnerRegressionEnabled()) {
		SKIP_TEST("require httpfs");
	}
}

TEST_CASE("Runner regression blocking", "[test_runner]") {
	if (!RunnerRegressionEnabled()) {
		return;
	}
	auto latch = std::getenv("DUCKDB_TEST_RUNNER_LATCH");
	REQUIRE(latch);
	auto path = string(latch);
	std::ofstream ready(path + ".ready");
	ready << "ready";
	ready.close();
	auto &fs = TestLocalFileSystem();
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
	while (!fs.FileExists(path + ".release")) {
		if (std::chrono::steady_clock::now() >= deadline) {
			FAIL("Runner regression latch timed out");
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

TEST_CASE("Runner regression nested invocation", "[test_runner]") {
	if (!RunnerRegressionEnabled()) {
		return;
	}
	auto test_extension = std::getenv("DUCKDB_TEST_RUNNER_EXTENSION");
	REQUIRE(test_extension);
	auto config = GetTestConfig();
	DuckDB database(nullptr, config.get());
	Connection connection(database);
	REQUIRE_NO_FAIL(connection.Query("LOAD " + Value(test_extension).ToSQLString()));
	auto result = connection.Query("SELECT * FROM run_tests([])");
	REQUIRE_FAIL(result);
	REQUIRE_THAT(result->GetError(), Catch::Contains("A test run is already active in this process"));
}

TEST_CASE("Runner regression registry survives cache eviction", "[test_runner]") {
	if (!RunnerRegressionEnabled()) {
		return;
	}
	auto extension = std::getenv("DUCKDB_TEST_RUNNER_EXTENSION");
	REQUIRE(extension);
	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", Value(true));
	DuckDB db(nullptr, &config);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("LOAD '" + StringUtil::Replace(extension, "'", "''") + "'"));
	auto &cache = db.instance->GetObjectCache();
	auto registry = cache.GetObject("duckdb.test.registry");
	REQUIRE(registry);
	REQUIRE_FALSE(registry->GetEstimatedCacheMemory().IsValid());
	cache.EvictToReduceMemory(NumericLimits<idx_t>::Maximum());
	REQUIRE(cache.GetObject("duckdb.test.registry") == registry);
}
