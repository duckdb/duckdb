#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "test_helpers.hpp"

#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

using namespace duckdb;

// Simulates Linux TRIM by zeroing the range. Intercepts the first Trim()
// call from TrimFreeBlocks(), pausing the checkpoint thread before the
// zeroing occurs. At that point WriteHeader() has fully committed (header
// written and both syncs done), so the racing writer can allocate the block
// from free_list and commit (its WAL is fsynced immediately on commit).
// Trim() then resumes and zeros the now-live block.
//
// Without the fix: Phase 2 reads a zeroed block → checksum mismatch.
// With the fix: TrimFreeBlocks() re-checks free_list under the lock and skips
//   blocks already allocated by the racing writer → Phase 2 succeeds.
class TrimRaceFileSystem : public duckdb::LocalFileSystem {
public:
	std::function<void()> racing_writer;

	// Called by the main thread: waits for the first Trim() call to pause the
	// checkpoint thread before trimming, runs racing_writer, then unblocks it
	// so the trim can happen.
	void RunRace() {
		std::unique_lock<std::mutex> lock(m_);
		cv_.wait(lock, [&] { return trim_fired_; });
		lock.unlock();
		if (racing_writer) {
			racing_writer();
		}
		lock.lock();
		writer_done_ = true;
		cv_.notify_all();
	}

	bool Trim(duckdb::FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override {
		// We are going to trim a block that was free. Stop here and let the racing writer run.
		// The racing writer will re-allocate this block and use it. Then we will get back here
		// and resume to trim this block that is now being used.
		std::unique_lock<std::mutex> lock(m_);
		trim_fired_ = true;
		cv_.notify_all();
		cv_.wait(lock, [&] { return writer_done_; });
		// Simulate Linux TRIM by explicitly zeroing the range.
		std::string nulls(length_bytes, '\0');
		duckdb::LocalFileSystem::Write(handle, (void *)nulls.data(), length_bytes, offset_bytes);
		return true;
	}

private:
	std::mutex m_;
	std::condition_variable cv_;
	bool trim_fired_ = false;
	bool writer_done_ = false;
};

// Regression test for a race condition in TrimFreeBlocks.
//
// WriteHeader() calls AddFreeBlock() which adds a block B to both free_list
// and fully_freed_blocks while holding the lock. The lock is then released
// before metadata_manager.Flush(). A concurrent thread can allocate B from
// free_list, write data to it, and commit a WAL entry referencing B. When
// TrimFreeBlocks runs afterwards (without the lock) it zeroes B, destroying
// that data. On restart WAL replay reads B, gets zeros, and fails with:
//   "Corrupt database file: computed checksum X does not match stored checksum 0"
//
// The fix: TrimFreeBlocks acquires the lock before trimming and skips any block
// no longer in free_list (i.e. concurrently allocated).
//
// How the race window is made deterministic:
//   TrimRaceFileSystem intercepts the first Trim() call from TrimFreeBlocks().
//   At that point WriteHeader() has fully committed (header written and synced)
//   and the single_file_block_lock is already released, so the racing writer
//   can allocate from free_list. The writer commits (WAL fsynced), then
//   Trim() resumes and zeros the now-live block.
//
// Blocks reach fully_freed_blocks (the trigger for Trim()) via DROP TABLE:
//   1. DROP TABLE with no active readers → CommitDropTable() adds old blocks to
//      modified_blocks AND CleanupTransactions() immediately destroys the DataTable,
//      releasing all BlockHandle refs. UnregisterBlock() is called, but since the
//      blocks are only in modified_blocks (not free_blocks_in_use), they are not
//      moved to free_list — they merely disappear from BlockManager::blocks.
//   2. At the next checkpoint WriteHeader() iterates modified_blocks and calls
//      AddFreeBlock() for each. TryGetBlock() returns nullptr (no live handle),
//      so the block is added to fully_freed_blocks and free_list.
//   3. TrimFreeBlocks() is then called with those blocks → Trim() fires.
//
// CONCURRENT_CHECKPOINT is triggered by opening a reader transaction AFTER DROP
// TABLE commits. The reader must run a real query before the next write commits,
// because Connection::BeginTransaction() only creates a MetaTransaction shell —
// the DuckTransaction is created lazily on the first database access. Without
// a real query first, the reader does not appear in active_transactions and the
// checkpoint falls back to FULL (holding the exclusive vacuum lock), which
// deadlocks with the racing writer's INSERT (which needs the shared vacuum lock).
// After the reader runs a query, GetLastCommit() > LowestActiveStart() holds and
// CONCURRENT checkpoint is used. CONCURRENT does not hold the exclusive vacuum
// lock, allowing the racing writer's INSERT to run concurrently.
TEST_CASE("trim_free_blocks does not trim concurrently allocated blocks", "[persistence][.]") {
	std::string db_path = TestCreatePath("trim_race.db");
	DeleteDatabase(db_path);

	// Phase 1: racing write during Trim() inside TrimFreeBlocks().
	{
		TrimRaceFileSystem *race_fs = new TrimRaceFileSystem();
		duckdb::DBConfig config;
		config.options.trim_free_blocks = true;
		config.options.checkpoint_on_shutdown = false;
		config.file_system = duckdb::make_uniq<duckdb::VirtualFileSystem>(
		    duckdb::unique_ptr<TrimRaceFileSystem>(race_fs));

		duckdb::DuckDB db(db_path, &config);
		duckdb::Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT * FROM range(50000)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT")); // Checkpoint 1: write blocks to database file

		// DROP TABLE with no active readers: CleanupTransactions() runs synchronously,
		// destroying the DataTable and releasing all BlockHandle refs. The old data
		// blocks are added to modified_blocks and will reach fully_freed_blocks at
		// the next checkpoint (TryGetBlock() returns nullptr → Trim() fires).
		REQUIRE_NO_FAIL(con.Query("DROP TABLE t"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (i INTEGER)"));

		// Open a read transaction AFTER DROP TABLE committed (DataTable already
		// destroyed). Eagerly create the DuckTransaction by running a real query
		// so that DuckTransactionManager::StartTransaction() runs immediately and
		// sets LowestActiveStart() below the next commit timestamp.
		//
		// BeginTransaction() only creates a MetaTransaction shell; the underlying
		// DuckTransaction (the entry in active_transactions) is created lazily on
		// the first database access. Without this eager query, CHECKPOINT would not
		// see the reader in active_transactions, GetLastCommit() would equal
		// LowestActiveStart(), and the checkpoint would become FULL instead of
		// CONCURRENT. A FULL checkpoint holds the exclusive vacuum lock, which
		// deadlocks with the racing writer's INSERT (which needs shared vacuum lock).
		duckdb::Connection reader_con(db);
		reader_con.BeginTransaction();
		reader_con.Query("SELECT count(*) FROM t"); // eagerly creates DuckTransaction in active_transactions

		// Commit something after reader_con started so GetLastCommit() >
		// LowestActiveStart(), which is the condition for CONCURRENT_CHECKPOINT.
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (0)"));

		// The racing writer runs from inside the first Trim() call in TrimFreeBlocks().
		// Without the fix: the writer allocates the block from free_list and commits
		// a WAL entry referencing it, then Trim() zeros the now-live block.
		// With the fix: TrimFreeBlocks re-checks free_list under lock and skips
		// blocks allocated by the writer.
		//
		// The INSERT must produce >= row_group_size (122880) rows to take the
		// optimistic-write path in LocalTableStorage::FlushBlocks, which allocates
		// block IDs from free_list and commits WAL entries referencing them.
		// A small INSERT uses local-append instead and writes raw row data to WAL,
		// so no block is allocated from free_list and the bug is never hit.
		duckdb::Connection writer_con(db);
		race_fs->racing_writer = [&]() {
			writer_con.Query("INSERT INTO t SELECT * FROM range(200000)");
		};

		// Trigger the checkpoint on a background thread.
		// WriteHeader() completes → TrimFreeBlocks() → Trim() pauses → racing_writer()
		// → Trim() resumes and zeros the allocated block.
		duckdb::Connection ckpt_con(db);
		std::thread ckpt_thread([&] { ckpt_con.Query("CHECKPOINT"); });

		// Wait for Trim() to pause, run the racing writer, then let it proceed.
		race_fs->RunRace();
		ckpt_thread.join();

		reader_con.Rollback();

		// DB closes here; checkpoint_on_shutdown=false leaves the WAL on disk.
	}

	// Phase 2: reopen and replay the WAL.
	// Without the fix: the racing writer's block was zeroed by Trim()
	//   → checksum mismatch, WAL replay fails.
	// With the fix: the racing writer got a safe block → replay succeeds.
	{
		duckdb::DBConfig config;
		config.options.checkpoint_on_shutdown = false;
		duckdb::DuckDB db(db_path, &config);
		duckdb::Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SELECT SUM(i::BIGINT) FROM t"));
	}

	DeleteDatabase(db_path);
}
