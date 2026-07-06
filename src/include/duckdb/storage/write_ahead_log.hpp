//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/write_ahead_log.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/block.hpp"

#include <condition_variable>
#include <mutex>

namespace duckdb {

struct AlterInfo;

class AttachedDatabase;
class Catalog;
class DatabaseInstance;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class ScalarMacroCatalogEntry;
class ViewCatalogEntry;
class TriggerCatalogEntry;
class TypeCatalogEntry;
class TableCatalogEntry;
class Transaction;
class TransactionManager;
class WriteAheadLogDeserializer;
struct PersistentCollectionData;

enum class WALInitState { NO_WAL, UNINITIALIZED, UNINITIALIZED_REQUIRES_TRUNCATE, INITIALIZED };

//! The WriteAheadLog (WAL) is a log that is used to provide durability. Prior
//! to committing a transaction it writes the changes the transaction made to
//! the database to the log, which can then be replayed upon startup in case the
//! server crashes or is shut down.
class WriteAheadLog {
public:
	//! Initialize the WAL in the specified directory
	explicit WriteAheadLog(StorageManager &storage_manager, const string &wal_path, idx_t wal_size = 0ULL,
	                       WALInitState state = WALInitState::NO_WAL,
	                       optional_idx checkpoint_iteration = optional_idx());
	virtual ~WriteAheadLog();

public:
	//! Replay and initialize the WAL, QueryContext is passed for metric collection purposes only!!
	static unique_ptr<WriteAheadLog> Replay(QueryContext context, StorageManager &storage_manager,
	                                        const string &wal_path);

	AttachedDatabase &GetDatabase();
	StorageManager &GetStorageManager();

	const string &GetPath() const {
		return wal_path;
	}
	//! Gets the total bytes written to the WAL since startup
	idx_t GetTotalWritten() const;

	//! A WAL is initialized, if a writer to a file exists.
	bool Initialized() const;
	//! Initializes the file of the WAL by creating the file writer.
	BufferedFileWriter &Initialize();

	//! Write the WAL header.
	void WriteHeader();

	virtual void WriteCreateTable(const TableCatalogEntry &entry);
	void WriteDropTable(const TableCatalogEntry &entry);

	void WriteCreateSchema(const SchemaCatalogEntry &entry);
	void WriteDropSchema(const SchemaCatalogEntry &entry);

	void WriteCreateView(const ViewCatalogEntry &entry);
	void WriteDropView(const ViewCatalogEntry &entry);

	void WriteCreateSequence(const SequenceCatalogEntry &entry);
	void WriteDropSequence(const SequenceCatalogEntry &entry);
	void WriteSequenceValue(SequenceValue val);

	void WriteCreateMacro(const ScalarMacroCatalogEntry &entry);
	void WriteDropMacro(const ScalarMacroCatalogEntry &entry);

	void WriteCreateTableMacro(const TableMacroCatalogEntry &entry);
	void WriteDropTableMacro(const TableMacroCatalogEntry &entry);

	void WriteCreateIndex(const IndexCatalogEntry &entry);
	void WriteDropIndex(const IndexCatalogEntry &entry);

	void WriteCreateType(const TypeCatalogEntry &entry);
	void WriteDropType(const TypeCatalogEntry &entry);

	void WriteCreateTrigger(const TriggerCatalogEntry &entry);
	void WriteDropTrigger(const TriggerCatalogEntry &entry);
	//! Sets the table used for subsequent insert/delete/update commands
	void WriteSetTable(const Identifier &schema, const Identifier &table);

	void WriteAlter(CatalogEntry &entry, const AlterInfo &info);

	void WriteInsert(DataChunk &chunk);
	void WriteRowGroupData(const PersistentCollectionData &data);
	void WriteDelete(DataChunk &chunk);
	//! Write a single (sub-) column update to the WAL. Chunk must be a pair of (COL, ROW_ID).
	//! The column_path vector is a *path* towards a column within the table
	//! i.e. if we have a table with a single column S STRUCT(A INT, B INT)
	//! and we update the validity mask of "S.B"
	//! the column path is:
	//! 0 (first column of table)
	//! -> 1 (second subcolumn of struct)
	//! -> 0 (first subcolumn of INT)
	void WriteUpdate(DataChunk &chunk, const vector<column_t> &column_path);

	//! Truncate the WAL to a previous size, and clear anything currently set in the writer.
	//! Used during RevertCommit.
	void Truncate(idx_t size);
	void Flush();
	//! Append the WAL_FLUSH marker for this commit and push the buffered bytes into the page cache, WITHOUT issuing an
	//! fsync. Must be called while the WAL lock (StorageManager::wal_lock) is held so the append stays totally ordered.
	//! Returns the WAL byte offset that covers this commit's entries; the caller must subsequently call GroupSync with
	//! this offset to make the bytes durable before acknowledging the commit.
	idx_t FlushAppendNoSync();
	//! Group commit: make every WAL byte up to (at least) my_offset durable. Called with the WAL lock NOT held, so
	//! concurrent committers overlap their fsyncs or coalesce onto in-flight ones. A committer returns from this call
	//! only once durable_offset >= my_offset, i.e. its WAL_FLUSH marker is on stable storage.
	void GroupSync(idx_t my_offset);
	//! Increment the WAL entry count, which is used for the auto-checkpoint threshold.
	void IncrementWALEntriesCount();
	void WriteCheckpoint(MetaBlockPointer meta_block);

protected:
	StorageManager &storage_manager;
	mutex wal_lock;
	unique_ptr<BufferedFileWriter> writer;
	string wal_path;
	atomic<WALInitState> init_state;
	optional_idx checkpoint_iteration;

private:
	//! Park until sync_epoch differs from current (slow path only; fast paths never touch the mutex).
	void WaitSyncEpochChange(uint64_t current);
	//! Bump sync_epoch and wake every parked committer (called after durable_offset advances or the WAL is poisoned).
	void BumpSyncEpochNotify();
	//! Maximum number of concurrent fsyncs worth issuing on this WAL's storage, set at initialization from the file
	//! system's declared sync semantics (FileSystem::SyncParallelism): unbounded where a sync is a per-call round
	//! trip that overlaps (network file systems), 1 where a sync commits a shared journal and concurrent syncs only
	//! add cost (local file systems) -- there the single stream's late-snapped targets batch naturally.
	idx_t sync_lane_cap = 1;

	//! Number of fsyncs currently in flight (bounded by sync_lane_cap).
	atomic<idx_t> active_syncs = 0;
	//! Raise-only maximum target of any in-flight (or completed) fsync: a committer whose bytes are already covered
	//! by an in-flight fsync parks instead of claiming a lane for a redundant fsync.
	atomic<idx_t> syncing_target = 0;
	//! Bumped by Truncate (under the WAL lock, after draining lanes): a sync lane whose fsync raced a truncate
	//! discards its durable_offset raise, since its snapshotted target may lie beyond the truncation point.
	atomic<uint64_t> truncate_gen = 0;
	//! Parking word for committers waiting on durability: bumped on every durable_offset advance and on failure.
	atomic<uint64_t> sync_epoch = 0;
	//! Terminal poison flag: after a failed fsync the OS may have dropped the failed dirty pages as clean, so a
	//! retried fsync could falsely report success for bytes that never reached disk -- durability can no longer be
	//! promised for any pending offset.
	atomic<bool> sync_failed = false;
	//! Guards only the parking of sync waiters; committers that are covered by a completed fsync, and committers
	//! issuing their own fsync, never take it.
	std::mutex sync_wait_mutex;
	std::condition_variable sync_wait_cv;
	//! Highest WAL byte offset known durable. Raise-only; stored by an fsyncer strictly AFTER its Sync() returns,
	//! acquire-loaded as the sole durable-before-ack predicate.
	atomic<idx_t> durable_offset = 0;
	//! Highest WAL byte offset pushed to the page cache (writer->Flush()). Release-stored under the WAL lock by
	//! FlushAppendNoSync (so it is monotonic and needs no sync coordination), acquire-loaded by fsyncers as their
	//! target, and clamped down by Truncate.
	atomic<idx_t> flushed_offset = 0;
};

} // namespace duckdb
