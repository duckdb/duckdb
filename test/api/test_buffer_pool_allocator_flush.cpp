#include "catch.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Buffer pool flushes the allocator after bulk deallocation", "[storage][buffer_pool]") {
	DBConfig config;
	config.options.maximum_memory = 256ULL * 1024 * 1024;
	// the block allocator pool supports flushing on every platform, unlike the fallback allocator
	config.options.block_allocator_size = 64ULL * 1024 * 1024;
	DuckDB db(nullptr, &config);
	auto &db_instance = *db.instance;
	if (!BlockAllocator::Get(db_instance).SupportsFlush()) {
		return;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(db_instance);
	auto &pool = db_instance.GetBufferPool();
	const auto block_size = buffer_manager.GetBlockAllocSize();
	const auto blocks_per_mb = (1024 * 1024) / block_size;

	vector<BufferHandle> live;
	auto hold = [&](idx_t mb) {
		for (idx_t i = 0; i < mb * blocks_per_mb; i++) {
			live.push_back(buffer_manager.Allocate(MemoryTag::EXTENSION, block_size));
		}
	};
	auto allocate_and_free = [&](idx_t mb) {
		vector<BufferHandle> handles;
		for (idx_t i = 0; i < mb * blocks_per_mb; i++) {
			handles.push_back(buffer_manager.Allocate(MemoryTag::EXTENSION, block_size));
		}
	};

	// freed memory is flushed once at least 16MB of it could push resident memory past the 256MB limit
	const auto flushes = pool.GetAllocatorFlushCount();
	hold(200);
	allocate_and_free(20);
	allocate_and_free(20);
	// 200MB live plus 40MB freed still fits in the limit
	auto handle = buffer_manager.Allocate(MemoryTag::EXTENSION, block_size);
	REQUIRE(pool.GetAllocatorFlushCount() == flushes);

	// live memory climbs to 220MB while 40MB is unflushed: the flush happens once they could exceed the limit
	allocate_and_free(20);
	handle = buffer_manager.Allocate(MemoryTag::EXTENSION, block_size);
	REQUIRE(pool.GetAllocatorFlushCount() == flushes + 1);
	// the 20MB freed after that flush fit in the limit again
	handle = buffer_manager.Allocate(MemoryTag::EXTENSION, block_size);
	REQUIRE(pool.GetAllocatorFlushCount() == flushes + 1);

	// operator allocations are counted as well
	auto &allocator = buffer_manager.GetBufferAllocator();
	for (idx_t i = 0; i < 60; i++) {
		auto data = allocator.Allocate(1024 * 1024);
	}
	handle = buffer_manager.Allocate(MemoryTag::EXTENSION, block_size);
	REQUIRE(pool.GetAllocatorFlushCount() == flushes + 2);
}
