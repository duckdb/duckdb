#include "catch.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/transaction/checkpoint_lock.hpp"

#include <atomic>
#include <thread>

using namespace duckdb;

TEST_CASE("StorageLock allows shared ownership and excludes writers", "[storage]") {
	StorageLock lock;
	auto shared_lock = lock.GetSharedLock();
	auto second_shared_lock = lock.GetSharedLock();
	REQUIRE(!lock.TryGetExclusiveLock());

	shared_lock.reset();
	REQUIRE(!lock.TryGetExclusiveLock());

	second_shared_lock.reset();
	auto exclusive_lock = lock.TryGetExclusiveLock();
	REQUIRE(exclusive_lock);
	REQUIRE(!lock.TryGetExclusiveLock());
}

TEST_CASE("CheckpointLockCoordinator promotes the final shared lease", "[storage]") {
	CheckpointLockCoordinator lock;
	auto first_shared_lock = lock.GetSharedLock();
	auto second_shared_lock = lock.GetSharedLock();

	REQUIRE(!lock.TryUpgrade(*first_shared_lock));
	REQUIRE(first_shared_lock);

	second_shared_lock.reset();
	auto exclusive_lock = lock.TryUpgrade(*first_shared_lock);
	REQUIRE(exclusive_lock);
	REQUIRE(first_shared_lock);
	REQUIRE(exclusive_lock->GetType() == CheckpointLockType::EXCLUSIVE);
	REQUIRE(!lock.TryGetExclusiveLock());

	exclusive_lock.reset();
	REQUIRE(!lock.TryGetExclusiveLock());

	first_shared_lock.reset();
	REQUIRE(lock.TryGetExclusiveLock());
}

TEST_CASE("StorageLock blocks later readers behind a waiting writer", "[storage]") {
	StorageLock lock;
	auto shared_lock = lock.GetSharedLock();

	std::atomic<bool> writer_waiting = false;
	std::atomic<bool> writer_acquired = false;
	std::atomic<bool> reader_acquired = false;
	std::atomic<bool> release_writer = false;

	std::thread writer([&] {
		writer_waiting = true;
		auto exclusive_lock = lock.GetExclusiveLock();
		writer_acquired = true;
		while (!release_writer) {
		}
	});

	while (!writer_waiting) {
	}
	REQUIRE(!lock.TryGetExclusiveLock());

	std::thread reader([&] {
		auto later_shared_lock = lock.GetSharedLock();
		reader_acquired = true;
	});

	for (idx_t i = 0; i < 1000 && !writer_acquired; i++) {
		REQUIRE(!reader_acquired);
		std::this_thread::yield();
	}

	shared_lock.reset();

	for (idx_t i = 0; i < 1000 && !writer_acquired; i++) {
		std::this_thread::yield();
	}
	REQUIRE(writer_acquired);
	REQUIRE(!reader_acquired);

	release_writer = true;
	writer.join();

	for (idx_t i = 0; i < 1000 && !reader_acquired; i++) {
		std::this_thread::yield();
	}
	REQUIRE(reader_acquired);
	reader.join();
}
