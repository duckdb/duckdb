#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/storage/storage_info.hpp"

#include <chrono>
#include <condition_variable>
#include <thread>
#include <unordered_set>

#if defined(__linux__)
#include <sys/mman.h>
#endif

using namespace duckdb;

namespace duckdb {
class BlockAllocatorTestHelper {
public:
	explicit BlockAllocatorTestHelper(const BlockAllocator &allocator) : allocator(allocator) {
	}
	void Add(idx_t count, idx_t now_ms) {
		annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
		allocator.AddRetention(count, now_ms);
	}
	void Reuse(idx_t count) {
		annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
		allocator.ReuseRetention(count);
	}
	idx_t Target(idx_t now_ms) {
		annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
		return allocator.RetentionTarget(now_ms);
	}
	static void Expire(const BlockAllocator &allocator) {
		allocator.ThreadFlush(false, 0, 1);
		annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
		std::fill(allocator.retention_buckets.begin(), allocator.retention_buckets.end(), 0);
	}
	static idx_t FreeBlocks(const BlockAllocator &allocator) {
		annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
		return allocator.GetFreeBlockCount();
	}
	static idx_t RetainedBlocks(const BlockAllocator &allocator) {
		annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
		const auto now =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
		        .count();
		return allocator.RetentionTarget(NumericCast<idx_t>(now));
	}
	static bool Drained(const BlockAllocator &allocator) {
		annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
		return allocator.GetFreeBlockCount() == 0 && allocator.flush_state == BlockAllocator::FlushState::IDLE;
	}

private:
	const BlockAllocator &allocator;
};
} // namespace duckdb

#if INTPTR_MAX == INT64_MAX
TEST_CASE("BlockAllocator retention ages free volume", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 4096;
	constexpr idx_t CAPACITY = 4096;
	Allocator fallback;
	BlockAllocator allocator(fallback, BLOCK_SIZE, BLOCK_SIZE * CAPACITY, BLOCK_SIZE * CAPACITY);
	BlockAllocatorTestHelper retention(allocator);
	CHECK(allocator.DecayDelay().GetIndex() == 1);
	retention.Add(1024, 1);

	SECTION("One-second linear decay starts at the bucket ending boundary") {
		CHECK(retention.Target(200) == 1024);
		CHECK(retention.Target(499) == 1024);
		CHECK(retention.Target(500) == 1024);
		CHECK(retention.Target(501) == 1022);
		CHECK(retention.Target(750) == 768);
		CHECK(retention.Target(1000) == 512);
		CHECK(retention.Target(1250) == 256);
		CHECK(retention.Target(1500) == 0);
	}
	SECTION("A return just before a boundary gets the full decay interval") {
		retention.Add(1024, 499);
		CHECK(retention.Target(500) == 2048);
		CHECK(retention.Target(501) == 2045);
		CHECK(retention.Target(1499) == 2);
		CHECK(retention.Target(1500) == 0);
	}
	SECTION("A full-pool burst decays without a percentage cutoff") {
		retention.Add(CAPACITY, 200);
		CHECK(retention.Target(500) == CAPACITY);
		CHECK(retention.Target(501) == 4091);
		CHECK(retention.Target(750) == 3072);
		CHECK(retention.Target(1000) == 2048);
		CHECK(retention.Target(1375) == 512);
		CHECK(retention.Target(1500) == 0);
	}
	SECTION("Large bursts decay without a byte cutoff") {
		constexpr idx_t LARGE_BLOCK_SIZE = idx_t(1) << 20;
		constexpr idx_t LARGE_POOL_SIZE = 8192 * LARGE_BLOCK_SIZE;
		BlockAllocator large_allocator(fallback, LARGE_BLOCK_SIZE, LARGE_POOL_SIZE, LARGE_POOL_SIZE);
		BlockAllocatorTestHelper large_retention(large_allocator);
		large_retention.Add(4096, 1);
		CHECK(large_retention.Target(1000) == 2048);
		CHECK(large_retention.Target(1500) == 0);
	}
	SECTION("A small new burst does not refresh the old allowance") {
		retention.Add(16, 500);
		CHECK(retention.Target(750) == 768 + 16);
		CHECK(retention.Target(1500) == 8);
		CHECK(retention.Target(2000) == 0);
	}
	SECTION("Reuse consumes the youngest allowance first") {
		retention.Add(16, 500);
		retention.Reuse(16);
		CHECK(retention.Target(750) == 768);
		CHECK(retention.Target(1500) == 0);
	}
	SECTION("Repeated small reuse cannot accumulate allowance") {
		retention.Add(16, 500);
		for (idx_t now = 600; now <= 3000; now += 100) {
			retention.Reuse(16);
			retention.Add(16, now);
		}
		CHECK(retention.Target(3000) == 16);
		CHECK(retention.Target(4500) == 0);
	}
	SECTION("A complete refill consumes the allowance including unused TLS blocks") {
		retention.Reuse(CAPACITY);
		CHECK(retention.Target(200) == 0);
	}
	SECTION("Long gaps expire the whole ring without repeated advancement") {
		CHECK(retention.Target(1000000000) == 0);
		retention.Add(16, 1000000001);
		CHECK(retention.Target(1000000001) == 16);
		CHECK(retention.Target(1000001500) == 0);
	}
	SECTION("Historical purged volume cannot reject a new burst") {
		retention.Add(CAPACITY, 500);
		retention.Add(CAPACITY, 501);
		CHECK(retention.Target(501) == 1022 + CAPACITY);
		retention.Reuse(CAPACITY);
		CHECK(retention.Target(750) == 768);
	}
}

namespace {
struct BlockAllocatorFallbackData : public PrivateAllocatorData {
	atomic<idx_t> allocation_count {0};

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

TEST_CASE("BlockAllocator reclaims only free blocks and preserves pool capacity", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 65536;
	constexpr idx_t BLOCK_COUNT = 1103;
	constexpr idx_t POOL_SIZE = BLOCK_SIZE * BLOCK_COUNT;
	auto private_data = make_uniq<BlockAllocatorFallbackData>();
	auto &fallback_data = *private_data;
	Allocator fallback(BlockAllocatorFallbackData::Allocate, Allocator::DefaultFree, Allocator::DefaultReallocate,
	                   std::move(private_data));
	BlockAllocator allocator(fallback, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);
	vector<data_ptr_t> blocks;
	for (idx_t i = 0; i < BLOCK_COUNT; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		block[0] = 42;
		block[BLOCK_SIZE - 1] = 42;
		blocks.push_back(block);
	}
	auto live_first = blocks[3];
	auto live_second = blocks[1030];
	for (auto block : blocks) {
		if (block != live_first && block != live_second) {
			allocator.FreeData(block, BLOCK_SIZE);
		}
	}

	idx_t expected_reclaimed = 0;
	SECTION("Warm reuse") {
	}
	SECTION("Thread flush only returns cached blocks") {
		allocator.ThreadFlush(false, 0, 1);
	}
	SECTION("Zero-byte flush") {
		allocator.FlushAll(0);
	}
	SECTION("Sub-block flush") {
		allocator.FlushAll(BLOCK_SIZE - 1);
	}
	SECTION("Byte-limited flush") {
		expected_reclaimed = 17;
		allocator.FlushAll(expected_reclaimed * BLOCK_SIZE + BLOCK_SIZE / 2);
	}
	SECTION("Complete flush spans reclamation batches") {
		expected_reclaimed = BLOCK_COUNT - 2;
		allocator.FlushAll();
	}
	SECTION("Idle flush spans reclamation batches") {
		expected_reclaimed = BLOCK_COUNT - 2;
		allocator.ThreadIdle();
	}

	std::unordered_set<data_ptr_t> allocated {live_first, live_second};
	blocks.clear();
	idx_t reclaimed = 0;
	for (idx_t i = 0; i < BLOCK_COUNT - 2; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		CHECK(allocated.insert(block).second);
		if (block[0] == 0 && block[BLOCK_SIZE - 1] == 0) {
			reclaimed++;
		}
		if (expected_reclaimed == 0) {
			CHECK(block[0] == 42);
			CHECK(block[BLOCK_SIZE - 1] == 42);
		}
		blocks.push_back(block);
	}
#if defined(__linux__) || defined(_WIN32)
	CHECK(reclaimed == expected_reclaimed);
#else
	CHECK(reclaimed <= expected_reclaimed);
#endif
	CHECK(live_first[0] == 42);
	CHECK(live_first[BLOCK_SIZE - 1] == 42);
	CHECK(live_second[0] == 42);
	CHECK(live_second[BLOCK_SIZE - 1] == 42);
	CHECK(fallback_data.allocation_count == 0);
	for (auto block : blocks) {
		allocator.FreeData(block, BLOCK_SIZE);
	}
	allocator.FreeData(live_first, BLOCK_SIZE);
	allocator.FreeData(live_second, BLOCK_SIZE);
}

TEST_CASE("BlockAllocator reclamation races with allocation and free", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 65536;
	constexpr idx_t BLOCK_COUNT = 256;
	constexpr idx_t POOL_SIZE = BLOCK_SIZE * BLOCK_COUNT;
	auto private_data = make_uniq<BlockAllocatorFallbackData>();
	auto &fallback_data = *private_data;
	Allocator fallback(BlockAllocatorFallbackData::Allocate, Allocator::DefaultFree, Allocator::DefaultReallocate,
	                   std::move(private_data));
	BlockAllocator allocator(fallback, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);
	auto live = allocator.AllocateData(BLOCK_SIZE);
	memset(live, 42, BLOCK_SIZE);
	vector<data_ptr_t> blocks;
	for (idx_t i = 0; i < 128; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		memset(block, 42, BLOCK_SIZE);
		blocks.push_back(block);
	}
	for (auto block : blocks) {
		allocator.FreeData(block, BLOCK_SIZE);
	}
	allocator.ThreadFlush(false, 0, 1);
	atomic<bool> ready {false};
	atomic<bool> start {false};
	atomic<bool> preserved {true};
	std::thread worker([&]() {
		for (idx_t iteration = 0; iteration < 100; iteration++) {
			vector<data_ptr_t> local;
			for (idx_t i = 0; i < 32; i++) {
				auto block = allocator.AllocateData(BLOCK_SIZE);
				memset(block, 84, BLOCK_SIZE);
				local.push_back(block);
			}
			if (iteration == 0) {
				ready = true;
				while (!start) {
					std::this_thread::yield();
				}
			}
			for (auto block : local) {
				for (idx_t i = 0; i < BLOCK_SIZE; i++) {
					if (block[i] != 84) {
						preserved = false;
					}
				}
				allocator.FreeData(block, BLOCK_SIZE);
			}
			allocator.ThreadFlush(false, 0, 1);
		}
	});
	while (!ready) {
		std::this_thread::yield();
	}
	allocator.FlushAll();
	start = true;
	for (idx_t iteration = 0; iteration < 100; iteration++) {
		allocator.FlushAll();
		allocator.ThreadIdle();
	}
	worker.join();
	CHECK(preserved);
	bool live_preserved = true;
	for (idx_t i = 0; i < BLOCK_SIZE; i++) {
		live_preserved &= live[i] == 42;
	}
	CHECK(live_preserved);
	allocator.FreeData(live, BLOCK_SIZE);
	allocator.FlushAll();
	blocks.clear();
	std::unordered_set<data_ptr_t> allocated;
	for (idx_t i = 0; i < BLOCK_COUNT; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		CHECK(allocated.insert(block).second);
		blocks.push_back(block);
	}
	CHECK(fallback_data.allocation_count == 0);
	for (auto block : blocks) {
		allocator.FreeData(block, BLOCK_SIZE);
	}
}

TEST_CASE("BlockAllocator supplies an idle decay delay when enabled", "[api][block_allocator]") {
	Allocator fallback;
	BlockAllocator allocator(fallback, 65536, 65536, 0);
	const auto fallback_delay = Allocator::DecayDelay();
	CHECK(allocator.DecayDelay().IsValid() == fallback_delay.IsValid());
	allocator.Resize(65536);
	REQUIRE(allocator.DecayDelay().IsValid());
	CHECK(allocator.DecayDelay().GetIndex() == (fallback_delay.IsValid() ? fallback_delay.GetIndex() : 1));
}

#ifndef DUCKDB_NO_THREADS
namespace {
class BlockAllocatorAsyncGate {
public:
	explicit BlockAllocatorAsyncGate(TaskScheduler &scheduler) : executor(scheduler, TaskSchedulerType::ASYNC) {
		executor.ScheduleTask(make_uniq<GateTask>(executor, *this));
	}
	~BlockAllocatorAsyncGate() {
		Release();
		executor.CancelAndDrain();
	}

	bool WaitUntilBlocked() {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(10), [&]() { return blocked; });
	}
	void Release() {
		lock_guard<mutex> guard(lock);
		released = true;
		cv.notify_all();
	}

private:
	class GateTask : public BaseExecutorTask {
	public:
		GateTask(TaskExecutor &executor, BlockAllocatorAsyncGate &gate) : BaseExecutorTask(executor), gate(gate) {
		}
		void ExecuteTask() override {
			unique_lock<mutex> guard(gate.lock);
			gate.blocked = true;
			gate.cv.notify_all();
			gate.cv.wait(guard, [&]() { return gate.released; });
		}

	private:
		BlockAllocatorAsyncGate &gate;
	};

	TaskExecutor executor;
	mutex lock;
	std::condition_variable cv;
	bool blocked = false;
	bool released = false;
};
} // namespace

TEST_CASE("BlockAllocator idle reclamation uses the async task queue", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 65536;
	constexpr idx_t BLOCK_COUNT = 32;
	constexpr idx_t POOL_SIZE = BLOCK_SIZE * BLOCK_COUNT;
	Allocator fallback;
	DBConfig config;
	config.options.maximum_threads = 1;
	config.options.async_threads = 1;
	config.block_allocator = make_uniq<BlockAllocator>(fallback, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);
	DuckDB db(nullptr, &config);
	auto &allocator = BlockAllocator::Get(*db.instance);
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.NumberOfAsyncThreads() == 1);
	BlockAllocatorAsyncGate gate(scheduler);
	REQUIRE(gate.WaitUntilBlocked());
	vector<data_ptr_t> live;
	for (idx_t i = 0; i < BLOCK_COUNT; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		block[0] = 42;
		block[BLOCK_SIZE - 1] = 42;
		live.push_back(block);
	}
	auto freed = live.back();
	live.pop_back();
	allocator.FreeData(freed, BLOCK_SIZE);
	BlockAllocatorTestHelper::Expire(allocator);
	for (idx_t i = 0; i < 32; i++) {
		allocator.ThreadIdle(scheduler);
	}
	CHECK(scheduler.GetNumberOfTasks() == 1);
	// Relaunch without changing workers preserves queued work and its coalesced request.
	scheduler.RelaunchThreads();
	allocator.ThreadIdle(scheduler);
	CHECK(scheduler.GetNumberOfTasks() == 1);
	auto reused = allocator.AllocateData(BLOCK_SIZE);
	CHECK(reused == freed);
	CHECK(reused[0] == 42);
	CHECK(reused[BLOCK_SIZE - 1] == 42);
	allocator.FreeData(reused, BLOCK_SIZE);
	BlockAllocatorTestHelper::Expire(allocator);
	allocator.ThreadIdle(scheduler);
	SECTION("Async worker executes the queued pass") {
		gate.Release();
	}
	SECTION("External execution can consume the ASYNC queue") {
		atomic<bool> execute {true};
		CHECK(scheduler.ExecuteTasks(&execute, 32) == 1);
		gate.Release();
	}

	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	while (!BlockAllocatorTestHelper::Drained(allocator) && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	CHECK(BlockAllocatorTestHelper::Drained(allocator));
	reused = allocator.AllocateData(BLOCK_SIZE);
	CHECK(reused == freed);
#if defined(__linux__) || defined(_WIN32)
	CHECK(reused[0] == 0);
	CHECK(reused[BLOCK_SIZE - 1] == 0);
#endif
	allocator.FreeData(reused, BLOCK_SIZE);
	for (auto block : live) {
		CHECK(block[0] == 42);
		CHECK(block[BLOCK_SIZE - 1] == 42);
		allocator.FreeData(block, BLOCK_SIZE);
	}

	scheduler.SetAsyncThreads(0);
	scheduler.RelaunchThreads();
	CHECK(scheduler.NumberOfAsyncThreads() == 0);
	live.clear();
	for (idx_t i = 0; i < BLOCK_COUNT; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		block[0] = 42;
		live.push_back(block);
	}
	for (auto block : live) {
		allocator.FreeData(block, BLOCK_SIZE);
	}
	allocator.ThreadFlush(false, 0, 1);
	CHECK(BlockAllocatorTestHelper::FreeBlocks(allocator) == BLOCK_COUNT);
	BlockAllocatorTestHelper::Expire(allocator);
	allocator.ThreadIdle(scheduler);
	live.clear();
	for (idx_t i = 0; i < BLOCK_COUNT; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
#if defined(__linux__) || defined(_WIN32)
		CHECK(block[0] == 0);
#endif
		live.push_back(block);
	}
	for (auto block : live) {
		allocator.FreeData(block, BLOCK_SIZE);
	}
}

TEST_CASE("BlockAllocator async reclamation survives worker relaunch and shutdown", "[api][block_allocator]") {
	for (idx_t iteration = 0; iteration < 8; iteration++) {
		DBConfig config;
		config.options.maximum_threads = 1;
		config.options.async_threads = 2;
		config.SetOptionByName("scheduler_process_partial", Value::BOOLEAN(iteration % 2 == 0));
		config.options.block_allocator_size = 64 * DEFAULT_BLOCK_ALLOC_SIZE;
		DuckDB db(nullptr, &config);
		auto &allocator = BlockAllocator::Get(*db.instance);
		auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
		vector<data_ptr_t> live;
		for (idx_t round = 0; round < 8; round++) {
			vector<unique_ptr<BlockAllocatorAsyncGate>> gates;
			const auto workers = scheduler.NumberOfThreads() - 1 + scheduler.NumberOfAsyncThreads();
			for (idx_t worker = 0; worker < workers; worker++) {
				auto gate = make_uniq<BlockAllocatorAsyncGate>(scheduler);
				REQUIRE(gate->WaitUntilBlocked());
				gates.push_back(std::move(gate));
			}
			for (idx_t i = 0; i < 64; i++) {
				auto block = allocator.AllocateData(DEFAULT_BLOCK_ALLOC_SIZE);
				block[0] = 42;
				live.push_back(block);
			}
			for (auto block : live) {
				CHECK(block[0] == 42);
				allocator.FreeData(block, DEFAULT_BLOCK_ALLOC_SIZE);
			}
			live.clear();
			BlockAllocatorTestHelper::Expire(allocator);
			allocator.ThreadIdle(scheduler);
			CHECK(scheduler.GetNumberOfTasks() == 1);
			for (auto &gate : gates) {
				gate->Release();
			}
			scheduler.SetThreads(1 + round % 2, 1);
			scheduler.RelaunchThreads();
			if (round % 3 == 0) {
				scheduler.SetAsyncThreads(0);
				scheduler.RelaunchThreads();
				CHECK(scheduler.NumberOfAsyncThreads() == 0);
				scheduler.SetAsyncThreads(2);
				scheduler.RelaunchThreads();
			}
		}
		allocator.ThreadIdle(scheduler);
	}
}

TEST_CASE("BlockAllocator background reclamation yields between bounded tasks", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 65536;
	constexpr idx_t BLOCK_COUNT = 4097;
	constexpr idx_t POOL_SIZE = BLOCK_SIZE * BLOCK_COUNT;
	Allocator fallback;
	DBConfig config;
	config.options.maximum_threads = 1;
	config.options.async_threads = 1;
	config.block_allocator = make_uniq<BlockAllocator>(fallback, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);
	DuckDB db(nullptr, &config);
	auto &allocator = BlockAllocator::Get(*db.instance);
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	BlockAllocatorAsyncGate gate(scheduler);
	REQUIRE(gate.WaitUntilBlocked());

	vector<data_ptr_t> blocks;
	for (idx_t i = 0; i < BLOCK_COUNT; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		block[0] = 42;
		block[BLOCK_SIZE - 1] = 42;
		blocks.push_back(block);
	}
	for (auto block : blocks) {
		allocator.FreeData(block, BLOCK_SIZE);
	}
	blocks.clear();
	auto other_producer = scheduler.CreateProducer();
	BlockAllocatorTestHelper::Expire(allocator);
	allocator.ThreadIdle(scheduler);
	atomic<bool> execute {true};
	REQUIRE(scheduler.ExecuteTasks(&execute, 1) == 1);
	CHECK(scheduler.GetNumberOfTasks() == 1);

	SECTION("One task leaves warm blocks available for reuse") {
		idx_t reclaimed = 0;
		for (idx_t i = 0; i < BLOCK_COUNT; i++) {
			auto block = allocator.AllocateData(BLOCK_SIZE);
			if (block[0] == 0 && block[BLOCK_SIZE - 1] == 0) {
				reclaimed++;
			}
			blocks.push_back(block);
		}
#if defined(__linux__) || defined(_WIN32)
		CHECK(reclaimed > 0);
#endif
		CHECK(reclaimed < BLOCK_COUNT);
		// The continuation must not discard blocks that have since been allocated.
		for (auto block : blocks) {
			block[0] = 84;
		}
		CHECK(scheduler.ExecuteTasks(&execute, BLOCK_COUNT) == 1);
		for (auto block : blocks) {
			CHECK(block[0] == 84);
		}
	}
	SECTION("Continuations finish the backlog without another idle request") {
		CHECK(scheduler.ExecuteTasks(&execute, BLOCK_COUNT) > 0);
		CHECK(scheduler.GetNumberOfTasks() == 0);
		for (idx_t i = 0; i < BLOCK_COUNT; i++) {
			auto block = allocator.AllocateData(BLOCK_SIZE);
#if defined(__linux__) || defined(_WIN32)
			CHECK(block[0] == 0);
			CHECK(block[BLOCK_SIZE - 1] == 0);
#endif
			blocks.push_back(block);
		}
	}
	SECTION("A fresh burst limits the stale continuation budget") {
		for (idx_t i = 0; i < BLOCK_COUNT; i++) {
			auto block = allocator.AllocateData(BLOCK_SIZE);
			block[0] = 84;
			blocks.push_back(block);
		}
		for (auto block : blocks) {
			allocator.FreeData(block, BLOCK_SIZE);
		}
		blocks.clear();
		allocator.ThreadFlush(false, 0, 1);
		CHECK(scheduler.ExecuteTasks(&execute, BLOCK_COUNT) > 0);
		CHECK(scheduler.GetNumberOfTasks() == 0);
		const auto retained = BlockAllocatorTestHelper::RetainedBlocks(allocator);
		REQUIRE(retained > 0);
		CHECK(BlockAllocatorTestHelper::FreeBlocks(allocator) >= retained);
		idx_t warm_blocks = 0;
		for (idx_t i = 0; i < BLOCK_COUNT; i++) {
			auto block = allocator.AllocateData(BLOCK_SIZE);
			warm_blocks += block[0] == 84;
			blocks.push_back(block);
		}
		CHECK(warm_blocks >= retained);
	}
	SECTION("Pending work stops the continuation chain") {
		class OtherTask : public Task {
		public:
			explicit OtherTask(bool &executed) : executed(executed) {
			}
			TaskExecutionResult Execute(TaskExecutionMode) override {
				executed = true;
				return TaskExecutionResult::TASK_FINISHED;
			}

		private:
			bool &executed;
		};
		bool executed = false;
		scheduler.ScheduleTask(*other_producer, make_shared_ptr<OtherTask>(executed), TaskSchedulerType::ASYNC);
		// Either queue order is valid; at most the current drain precedes other work.
		CHECK(scheduler.ExecuteTasks(&execute, 2) == 2);
		CHECK(executed);
		// After contention, an idle request can resume any deferred reclamation.
		allocator.ThreadIdle(scheduler);
		scheduler.ExecuteTasks(&execute, BLOCK_COUNT);
		CHECK(scheduler.GetNumberOfTasks() == 0);
	}
	for (auto block : blocks) {
		allocator.FreeData(block, BLOCK_SIZE);
	}
}

TEST_CASE("BlockAllocator idle workers reclaim eligible blocks", "[api][block_allocator]") {
	DBConfig config;
	config.options.maximum_threads = 2;
	config.options.async_threads = 0;
	config.options.block_allocator_size = 128 * DEFAULT_BLOCK_ALLOC_SIZE;
	SECTION("A regular worker performs synchronous idle maintenance") {
	}
	SECTION("An async worker performs bounded idle maintenance") {
		config.options.maximum_threads = 1;
		config.options.async_threads = 1;
	}
	DuckDB db(nullptr, &config);
	auto &allocator = BlockAllocator::Get(*db.instance);
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	BlockAllocatorAsyncGate gate(scheduler);
	REQUIRE(gate.WaitUntilBlocked());
	vector<data_ptr_t> blocks;
	for (idx_t i = 0; i < 128; i++) {
		auto block = allocator.AllocateData(DEFAULT_BLOCK_ALLOC_SIZE);
		block[0] = 42;
		blocks.push_back(block);
	}
	for (auto block : blocks) {
		allocator.FreeData(block, DEFAULT_BLOCK_ALLOC_SIZE);
	}
	BlockAllocatorTestHelper::Expire(allocator);
	CHECK(scheduler.GetNumberOfTasks() == 0);
	CHECK(BlockAllocatorTestHelper::FreeBlocks(allocator) == 128);
	gate.Release();
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	while (!BlockAllocatorTestHelper::Drained(allocator) && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	CHECK(BlockAllocatorTestHelper::Drained(allocator));
	allocator.ThreadIdle(scheduler);
	CHECK(scheduler.GetNumberOfTasks() == 0);
}

TEST_CASE("BlockAllocator cooling without workers needs another maintenance opportunity", "[api][block_allocator]") {
	DBConfig config;
	config.options.maximum_threads = 1;
	config.options.async_threads = 0;
	config.options.block_allocator_size = 64 * DEFAULT_BLOCK_ALLOC_SIZE;
	DuckDB db(nullptr, &config);
	auto &allocator = BlockAllocator::Get(*db.instance);
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	allocator.ThreadIdle(scheduler);
	auto block = allocator.AllocateData(DEFAULT_BLOCK_ALLOC_SIZE);
	block[0] = 42;
	allocator.FreeData(block, DEFAULT_BLOCK_ALLOC_SIZE);
	allocator.ThreadFlush(false, 0, 1);
	CHECK(BlockAllocatorTestHelper::FreeBlocks(allocator) == 1);
	CHECK(scheduler.GetNumberOfTasks() == 0);
	std::this_thread::sleep_for(std::chrono::milliseconds(1600));
	CHECK(BlockAllocatorTestHelper::FreeBlocks(allocator) == 1);
	allocator.ThreadIdle(scheduler);
	CHECK(BlockAllocatorTestHelper::Drained(allocator));
}
#endif

#if defined(__linux__)
TEST_CASE("BlockAllocator preserves capacity after a failed discard", "[api][block_allocator]") {
	constexpr idx_t BLOCK_SIZE = 65536;
	constexpr idx_t BLOCK_COUNT = 16;
	constexpr idx_t POOL_SIZE = BLOCK_SIZE * BLOCK_COUNT;
	auto private_data = make_uniq<BlockAllocatorFallbackData>();
	auto &fallback_data = *private_data;
	Allocator fallback(BlockAllocatorFallbackData::Allocate, Allocator::DefaultFree, Allocator::DefaultReallocate,
	                   std::move(private_data));
	BlockAllocator allocator(fallback, BLOCK_SIZE, POOL_SIZE, POOL_SIZE);
	vector<data_ptr_t> blocks;
	for (idx_t i = 0; i < BLOCK_COUNT; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		memset(block, 42, BLOCK_SIZE);
		blocks.push_back(block);
	}
	std::sort(blocks.begin(), blocks.end());
	auto live_first = blocks[4];
	auto live_second = blocks[12];
	auto locked = blocks[8];
	// Linux rejects MADV_DONTNEED for locked pages, leaving the mapping accessible.
	if (mlock(locked, 1) != 0) {
		WARN("Cannot lock a page to exercise discard failure");
		for (auto block : blocks) {
			allocator.FreeData(block, BLOCK_SIZE);
		}
		return;
	}
	for (auto block : blocks) {
		if (block != live_first && block != live_second) {
			allocator.FreeData(block, BLOCK_SIZE);
		}
	}
	const optional_idx flush_size(POOL_SIZE);
	STATIC_REQUIRE(noexcept(allocator.FlushAll(flush_size)));
	CHECK_NOTHROW(allocator.FlushAll(POOL_SIZE));
	CHECK_NOTHROW(allocator.FlushAll());
	CHECK_NOTHROW(allocator.ThreadIdle());
	CHECK(munlock(locked, 1) == 0);
	allocator.FlushAll();
	std::unordered_set<data_ptr_t> allocated {live_first, live_second};
	blocks.clear();
	for (idx_t i = 0; i < BLOCK_COUNT - 2; i++) {
		auto block = allocator.AllocateData(BLOCK_SIZE);
		CHECK(allocated.insert(block).second);
		CHECK(block[0] == 0);
		CHECK(block[BLOCK_SIZE - 1] == 0);
		blocks.push_back(block);
	}
	CHECK(live_first[0] == 42);
	CHECK(live_second[0] == 42);
	CHECK(fallback_data.allocation_count == 0);
	for (auto block : blocks) {
		allocator.FreeData(block, BLOCK_SIZE);
	}
	allocator.FreeData(live_first, BLOCK_SIZE);
	allocator.FreeData(live_second, BLOCK_SIZE);
}

TEST_CASE("BlockAllocator discard failure does not interrupt database shutdown", "[api][block_allocator]") {
	DBConfig config;
	config.options.maximum_threads = 1;
	config.options.async_threads = 0;
	config.options.block_allocator_size = 4 * DEFAULT_BLOCK_ALLOC_SIZE;
	auto db = make_uniq<DuckDB>(nullptr, &config);
	auto &allocator = BlockAllocator::Get(*db->instance);
	auto block = allocator.AllocateData(DEFAULT_BLOCK_ALLOC_SIZE);
	block[0] = 42;
	if (mlock(block, 1) != 0) {
		WARN("Cannot lock a page to exercise discard failure during shutdown");
		allocator.FreeData(block, DEFAULT_BLOCK_ALLOC_SIZE);
		return;
	}
	allocator.FreeData(block, DEFAULT_BLOCK_ALLOC_SIZE);
	// Unmapping the pool during destruction also releases the page lock.
	CHECK_NOTHROW(db.reset());
}
#endif
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
