#include "catch.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer/buffer_pool_reservation.hpp"
#include "duckdb/main/database.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("BufferPoolReservation move assignment releases old reservation", "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	auto baseline = pool.GetUsedMemory();

	BufferPoolReservation r1(MemoryTag::BASE_TABLE, pool);
	r1.Resize(1000);
	REQUIRE(pool.GetUsedMemory() == baseline + 1000);

	BufferPoolReservation r2(MemoryTag::BASE_TABLE, pool);
	r2.Resize(500);
	REQUIRE(pool.GetUsedMemory() == baseline + 1500);

	r1 = std::move(r2);
	REQUIRE(r1.size == 500);
	REQUIRE(r2.size == 0);
	REQUIRE(pool.GetUsedMemory() == baseline + 500);

	r1.Resize(0);
}
