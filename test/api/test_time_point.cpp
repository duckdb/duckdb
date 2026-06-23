#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/time_point.hpp"
#include "duckdb/common/profiler.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;

TEST_CASE("Test TimePoint monotonic behavior", "[api][time_point]") {
	auto start = TimePoint::Tick();
	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	auto elapsed_ms = start.ElapsedMillis();
	REQUIRE(elapsed_ms >= 10);

	auto first_check = start.ElapsedMillis();
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
	auto second_check = start.ElapsedMillis();
	REQUIRE(second_check >= first_check);
}

TEST_CASE("Test TimePoint unit conversions", "[api][time_point]") {
	auto start = TimePoint::Tick();
	std::this_thread::sleep_for(std::chrono::milliseconds(5));

	auto millis = start.ElapsedMillis();
	auto micros = start.ElapsedMicros();
	auto nanos = start.ElapsedNanos();

	REQUIRE(nanos > micros);
	REQUIRE(micros > millis);

	REQUIRE(micros >= millis * 1000);
	REQUIRE(nanos >= micros * 1000);
}

TEST_CASE("Test TimePoint ElapsedNanosSince", "[api][time_point]") {
	auto start = TimePoint::Tick();
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	auto end = TimePoint::Tick();

	auto duration_ns = TimePoint::ElapsedNanosSince(start, end);
	REQUIRE(duration_ns > 0);
	REQUIRE(duration_ns >= 10000000); // at least 10ms in nanoseconds
}

TEST_CASE("Test Profiler with TimePoint integration", "[api][time_point]") {
	Profiler profiler;

	profiler.Start();
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	profiler.End();

	auto elapsed_seconds = profiler.Elapsed();
	auto elapsed_nanos = profiler.ElapsedNanos();

	REQUIRE(elapsed_seconds >= 0.01);
	REQUIRE(elapsed_nanos >= 10000000);

	Profiler profiler2;
	REQUIRE(profiler2.Elapsed() == 0);
	REQUIRE(profiler2.ElapsedNanos() == 0);

	Profiler profiler3;
	profiler3.Start();
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
	auto ongoing_elapsed = profiler3.Elapsed();
	REQUIRE(ongoing_elapsed >= 0.005);
}
