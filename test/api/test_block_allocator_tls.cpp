#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/mutex.hpp"

#include <condition_variable>
#include <thread>

using namespace duckdb;

#if INTPTR_MAX == INT64_MAX
namespace {
struct BlockAllocatorFallbackData : public PrivateAllocatorData {
	idx_t allocation_count = 0;

	static data_ptr_t Allocate(PrivateAllocatorData *private_data, idx_t size) {
		private_data->Cast<BlockAllocatorFallbackData>().allocation_count++;
		return Allocator::DefaultAllocate(private_data, size);
	}
};
} // namespace

TEST_CASE("BlockAllocator preserves cached blocks across allocator switches", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 4096;
	constexpr idx_t BLOCK_COUNT = 16;
	constexpr idx_t POOL_SIZE = BLOCK_SIZE * BLOCK_COUNT;

	auto private_data = make_uniq<BlockAllocatorFallbackData>();
	auto &fallback_data = *private_data;
	Allocator fallback(BlockAllocatorFallbackData::Allocate, Allocator::DefaultFree, Allocator::DefaultReallocate,
	                   std::move(private_data));
	BlockAllocator first(fallback, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);

	SECTION("Switch allocations and frees between live allocators") {
		BlockAllocator second(fallback, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);
		for (idx_t iteration = 0; iteration < 3; iteration++) {
			vector<data_ptr_t> first_blocks;
			vector<data_ptr_t> second_blocks;
			for (idx_t i = 0; i < BLOCK_COUNT; i++) {
				first_blocks.push_back(first.AllocateData(BLOCK_SIZE));
				second_blocks.push_back(second.AllocateData(BLOCK_SIZE));
				first_blocks.back()[0] = uint8_t(i + 1);
				second_blocks.back()[0] = uint8_t(i + BLOCK_COUNT + 1);
			}
			for (idx_t i = 0; i < BLOCK_COUNT; i++) {
				CHECK(first_blocks[i][0] == i + 1);
				CHECK(second_blocks[i][0] == i + BLOCK_COUNT + 1);
				first.FreeData(first_blocks[i], BLOCK_SIZE);
				second.FreeData(second_blocks[i], BLOCK_SIZE);
			}
			CHECK(fallback_data.allocation_count == 0);
		}
	}

	SECTION("Destroy another allocator while the first has cached blocks") {
		auto pointer = first.AllocateData(BLOCK_SIZE);
		first.FreeData(pointer, BLOCK_SIZE);
		{ BlockAllocator second(fallback, BLOCK_SIZE, POOL_SIZE, POOL_SIZE); }
		vector<data_ptr_t> blocks;
		for (idx_t i = 0; i < BLOCK_COUNT; i++) {
			blocks.push_back(first.AllocateData(BLOCK_SIZE));
		}
		for (auto block : blocks) {
			first.FreeData(block, BLOCK_SIZE);
		}
		CHECK(fallback_data.allocation_count == 0);
	}
}

TEST_CASE("BlockAllocator switching races with allocator destruction", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 4096;
	constexpr idx_t POOL_SIZE = BLOCK_SIZE * 16;
	Allocator allocator;
	BlockAllocator next(allocator, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);

	for (idx_t iteration = 0; iteration < 100; iteration++) {
		auto previous = make_uniq<BlockAllocator>(allocator, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);
		atomic<bool> ready {false};
		atomic<bool> switch_allocator {false};
		std::thread worker([&]() {
			auto pointer = previous->AllocateData(BLOCK_SIZE);
			previous->FreeData(pointer, BLOCK_SIZE);
			ready = true;
			while (!switch_allocator) {
				std::this_thread::yield();
			}
			pointer = next.AllocateData(BLOCK_SIZE);
			next.FreeData(pointer, BLOCK_SIZE);
		});
		while (!ready) {
			std::this_thread::yield();
		}
		switch_allocator = true;
		previous.reset();
		worker.join();
	}
}
#endif

TEST_CASE("BlockAllocator usage and de-allocation on different threads", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 4096;
	constexpr idx_t VIRTUAL_MEM_SIZE = 256 * 1024 * 1024;
	constexpr idx_t PHYSICAL_MEM_SIZE = 256 * 1024 * 1024;

	std::thread worker;
	mutex mtx;
	std::condition_variable cv;
	bool alloc_done = false;
	bool allocator_destroyed = false;

	// Thread-A, where we construct BlockAllocator.
	{
		Allocator alloc;
		BlockAllocator ba(alloc, BLOCK_SIZE, VIRTUAL_MEM_SIZE, PHYSICAL_MEM_SIZE);

		// Thread-B, where we allocate blocks and creates thread-local state.
		worker = std::thread([&]() {
			constexpr int NUM_BLOCKS = 16;
			data_ptr_t blocks[NUM_BLOCKS];
			for (int i = 0; i < NUM_BLOCKS; i++) {
				blocks[i] = ba.AllocateData(BLOCK_SIZE);
				REQUIRE(blocks[i] != nullptr);
			}
			for (int i = 0; i < NUM_BLOCKS; i++) {
				ba.FreeData(blocks[i], BLOCK_SIZE);
			}

			{
				lock_guard<mutex> lk(mtx);
				alloc_done = true;
			}
			cv.notify_one();

			{
				unique_lock<mutex> lk(mtx);
				cv.wait(lk, [&] { return allocator_destroyed; });
			}
		});

		{
			unique_lock<mutex> lk(mtx);
			cv.wait(lk, [&] { return alloc_done; });
		}
	}
	// BlockAllocator destructs here, which clears thread-local allocation state in thread-B, instead of the one in
	// thread-A, which records free blocks.

	// Destroy thread-B and thread-local state, where allocation happens.
	{
		lock_guard<mutex> lk(mtx);
		allocator_destroyed = true;
		cv.notify_one();
	}
	worker.join();
}
