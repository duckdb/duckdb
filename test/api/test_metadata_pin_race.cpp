#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <thread>

using namespace duckdb;

namespace {

struct BlockManagerRefs {
	BlockManager *block_manager;
	MetadataManager *metadata_manager;
	BufferManager *buffer_manager;
};

BlockManagerRefs GetBlockManagerRefs(DuckDB &db, Connection &con) {
	BlockManagerRefs refs;
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context, "");
	refs.block_manager = &StorageManager::Get(catalog).GetBlockManager();
	refs.metadata_manager = &refs.block_manager->GetMetadataManager();
	refs.buffer_manager = &refs.block_manager->GetBufferManager();
	con.Commit();
	return refs;
}

} // namespace

// A handle that was orphaned (its id unregistered while the handle is alive, then re-registered
// with a new handle) must not unregister the newer handle when it is destroyed.
TEST_CASE("A dying stale block handle does not unregister a newer handle", "[storage]") {
	auto path = TestCreatePath("stale_handle_unregister.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		auto refs = GetBlockManagerRefs(db, con);
		auto &block_manager = *refs.block_manager;

		// take a fresh block id so that retiring it goes through the newly-used path
		auto test_id = block_manager.GetFreeBlockId();
		auto stale = block_manager.RegisterBlock(test_id);
		// orphan the handle
		block_manager.UnregisterBlock(test_id);
		// register a new handle for the same block id
		auto current = block_manager.RegisterBlock(test_id);
		REQUIRE(current.get() != stale.get());
		// retire the block id - the live handle moves it to free_blocks_in_use instead of the
		// free list, so the block cannot be reused while the handle exists
		block_manager.MarkBlockAsModified(test_id);
		auto free_blocks_before = block_manager.FreeBlocks();

		// destroying the stale handle must leave the newer handle registered and must not
		// release the block id to the free list
		stale.reset();
		REQUIRE(block_manager.FreeBlocks() == free_blocks_before);
		auto resolved = block_manager.RegisterBlock(test_id);
		REQUIRE(resolved.get() == current.get());

		// once the last handle dies, the block id must become available again
		resolved.reset();
		current.reset();
		REQUIRE(block_manager.FreeBlocks() == free_blocks_before + 1);
	}
	DeleteDatabase(path);
}

// A dying handle's registration check and free-list transition must be atomic: destroying expired
// handles for a block id while another thread re-registers the same id must never drop the live
// handle's registration (or release the id to the free list).
TEST_CASE("Concurrent handle destruction does not drop a newer registration", "[storage]") {
	auto path = TestCreatePath("stale_handle_unregister_race.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		auto refs = GetBlockManagerRefs(db, con);
		auto &block_manager = *refs.block_manager;

		const block_id_t test_id = 2000000;
		std::atomic<bool> done {false};
		std::atomic<bool> registration_lost {false};

		// churn thread: repeatedly creates and destroys handles for the id - each destruction
		// runs the expired-registration cleanup that must not touch a newer live handle
		std::thread churn([&]() {
			while (!done) {
				auto handle = block_manager.RegisterBlock(test_id);
				handle.reset();
			}
		});

		for (idx_t i = 0; i < 100000; i++) {
			auto live = block_manager.RegisterBlock(test_id);
			// while 'live' exists, resolving the id must always return the same handle
			auto resolved = block_manager.RegisterBlock(test_id);
			if (resolved.get() != live.get()) {
				registration_lost = true;
				break;
			}
		}
		done = true;
		churn.join();
		REQUIRE(!registration_lost.load());
	}
	DeleteDatabase(path);
}
