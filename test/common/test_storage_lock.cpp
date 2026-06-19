#include "catch.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/transaction/checkpoint_lock.hpp"

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
