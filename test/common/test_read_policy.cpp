#include "catch.hpp"
#include "duckdb/storage/read_policy.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// Block size used by AlignedReadPolicy
constexpr idx_t BLOCK_SIZE = 2 * 1024 * 1024; // 2MiB

TEST_CASE("Test AlignedReadPolicy basic alignment", "[read_policy]") {
	AlignedReadPolicy policy;
	
	SECTION("Align down start location") {
		// Request at non-aligned location
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/1000, /*location=*/100, 
		                                          /*file_size=*/10 * BLOCK_SIZE, /*next_range=*/optional_idx());
		
		// Should align down to 0
		REQUIRE(result.read_location == 0);
		// Should align up to next block
		REQUIRE(result.read_bytes == BLOCK_SIZE);
	}
	
	SECTION("Already aligned start location") {
		// Request at aligned location
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/1000, /*location=*/BLOCK_SIZE, 
		                                          /*file_size=*/10 * BLOCK_SIZE, /*next_range=*/optional_idx());
		
		// Should stay at block boundary
		REQUIRE(result.read_location == BLOCK_SIZE);
		// Should read exactly one block
		REQUIRE(result.read_bytes == BLOCK_SIZE);
	}
	
	SECTION("Large request spanning multiple blocks") {
		// Request 3.5 blocks worth of data
		idx_t request_size = 3 * BLOCK_SIZE + BLOCK_SIZE / 2;
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/request_size, /*location=*/BLOCK_SIZE, 
		                                          /*file_size=*/10 * BLOCK_SIZE, /*next_range=*/optional_idx());
		
		// Should start at aligned location
		REQUIRE(result.read_location == BLOCK_SIZE);
		// Should align up to 5 blocks total (1 + 3.5 aligned up to 5)
		REQUIRE(result.read_bytes == 4 * BLOCK_SIZE);
	}
	
	SECTION("Small request within one block") {
		// Request small amount at aligned location
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/100, /*location=*/2 * BLOCK_SIZE, 
		                                          /*file_size=*/10 * BLOCK_SIZE, /*next_range=*/optional_idx());
		
		REQUIRE(result.read_location == 2 * BLOCK_SIZE);
		REQUIRE(result.read_bytes == BLOCK_SIZE);
	}
}

TEST_CASE("Test AlignedReadPolicy file size clamping", "[read_policy]") {
	AlignedReadPolicy policy;
	
	SECTION("Request extends past file size") {
		idx_t file_size = 3 * BLOCK_SIZE + BLOCK_SIZE / 2;
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/BLOCK_SIZE, /*location=*/3 * BLOCK_SIZE, 
		                                          /*file_size=*/file_size, /*next_range=*/optional_idx());
		
		// Should align down to 3 * BLOCK_SIZE
		REQUIRE(result.read_location == 3 * BLOCK_SIZE);
		// Should clamp to file size, not align up to 4 * BLOCK_SIZE
		REQUIRE(result.read_bytes == BLOCK_SIZE / 2);
	}
	
	SECTION("Aligned end exactly at file size") {
		idx_t file_size = 4 * BLOCK_SIZE;
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/BLOCK_SIZE, /*location=*/3 * BLOCK_SIZE, 
		                                          /*file_size=*/file_size, /*next_range=*/optional_idx());
		
		REQUIRE(result.read_location == 3 * BLOCK_SIZE);
		REQUIRE(result.read_bytes == BLOCK_SIZE);
	}
	
	SECTION("Request at end of file") {
		idx_t file_size = 5 * BLOCK_SIZE + 100;
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/50, /*location=*/5 * BLOCK_SIZE, 
		                                          /*file_size=*/file_size, /*next_range=*/optional_idx());
		
		REQUIRE(result.read_location == 5 * BLOCK_SIZE);
		// Would align to 6 * BLOCK_SIZE, but clamped to file size
		REQUIRE(result.read_bytes == 100);
	}
}

TEST_CASE("Test AlignedReadPolicy with next range", "[read_policy]") {
	AlignedReadPolicy policy;
	
	SECTION("Next range is far away - no adjustment") {
		idx_t next_range = 10 * BLOCK_SIZE;
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/1000, /*location=*/BLOCK_SIZE, 
		                                          /*file_size=*/20 * BLOCK_SIZE, /*next_range=*/next_range);
		
		// Should not be affected by distant next range
		REQUIRE(result.read_location == BLOCK_SIZE);
		REQUIRE(result.read_bytes == BLOCK_SIZE);
	}
	
	SECTION("Next range causes aligned_end to be clamped") {
		idx_t next_range = 4 * BLOCK_SIZE; // Aligned next range
		// Request would normally align to 4 * BLOCK_SIZE
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/2 * BLOCK_SIZE, /*location=*/2 * BLOCK_SIZE, 
		                                          /*file_size=*/20 * BLOCK_SIZE, /*next_range=*/next_range);
		
		REQUIRE(result.read_location == 2 * BLOCK_SIZE);
		// Should stop at next range boundary
		REQUIRE(result.read_bytes == 2 * BLOCK_SIZE);
	}
	
	SECTION("Next range immediately after request") {
		idx_t location = 2 * BLOCK_SIZE;
		idx_t next_range = 4 * BLOCK_SIZE; // Next range is 2 blocks away
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/BLOCK_SIZE / 2, /*location=*/location, 
		                                          /*file_size=*/20 * BLOCK_SIZE, /*next_range=*/next_range);
		
		REQUIRE(result.read_location == 2 * BLOCK_SIZE);
		// Aligns up to 4 * BLOCK_SIZE which exactly meets next range (no overlap)
		REQUIRE(result.read_bytes == BLOCK_SIZE);
	}
	
	SECTION("Next range beyond aligned end - no effect") {
		idx_t location = 2 * BLOCK_SIZE;
		idx_t next_range = 6 * BLOCK_SIZE; // Far away
		auto result = policy.CalculateBytesToRead(/*nr_bytes=*/BLOCK_SIZE, /*location=*/location, 
		                                          /*file_size=*/20 * BLOCK_SIZE, /*next_range=*/next_range);
		
		REQUIRE(result.read_location == 2 * BLOCK_SIZE);
		// Aligns to 3 * BLOCK_SIZE, which is before next_range
		REQUIRE(result.read_bytes == BLOCK_SIZE);
	}
}
