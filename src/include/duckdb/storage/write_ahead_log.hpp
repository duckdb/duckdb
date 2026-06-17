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
	//! Write a WAL_FLUSH marker, push all buffered data to the operating system and fsync (fully durable).
	void Flush();
	//! Write a WAL_FLUSH marker. Returns the WAL offset that a subsequent SyncUpTo() call must reach to make the
	//! data durable. The caller must hold the WAL lock of the storage manager.
	//! If push_to_os is true the buffered data is pushed to the operating system inline (the synchronous commit
	//! path). If false, only the in-memory buffer is advanced and the push to the OS is deferred to the group
	//! commit sync leader (SyncUpTo with leader_pushes_batch), so that a single write() covers the whole batch -
	//! this matters on network storage (e.g. EFS) where the per-commit write is itself a multi-ms round-trip.
	//! If requires_block_sync is set, the commit completed by this marker references optimistically written
	//! row group data: the database file is fsynced (once, batched) by the sync leader BEFORE any WAL fsync
	//! that makes this marker durable, so that the referenced blocks are always durable first.
	idx_t WriteFlushMarker(bool requires_block_sync = false, bool push_to_os = true);
	//! Ensure the WAL is fsynced at least up to the given target offset (as returned by WriteFlushMarker).
	//! Safe to call without holding the WAL lock: concurrent callers elect a leader whose single fsync
	//! covers all data pushed to the operating system so far (group commit).
	//! wait_for_batch enables the adaptive micro-batching wait - pass false when the caller holds the WAL lock
	//! (no concurrent appends can arrive, so there is nothing to wait for).
	//! leader_pushes_batch makes the sync leader push the buffered WAL data to the OS (one write() for the whole
	//! batch) before its fsync, under the WAL flush_lock - NOT the storage manager WAL lock. Pass true only when the
	//! caller does NOT hold the WAL lock and the markers were written with push_to_os=false (the deferred path).
	void SyncUpTo(idx_t target_offset, bool wait_for_batch, bool leader_pushes_batch = false);
	//! The WAL offset (logical bytes written) that has been pushed to the operating system so far
	idx_t GetWrittenOffset() const {
		return written_offset;
	}
	//! Acquire the WAL flush lock. Serializes pushes of the in-memory WAL buffer to the OS (the group commit sync
	//! leader's batched write, and the synchronous push-to-OS) against all writes into that buffer (entry appends,
	//! flush markers), truncation, and header writes. Distinct from the storage manager WAL lock so the sync
	//! leader's push cannot deadlock a checkpoint / catalog commit / synchronous commit that holds the WAL lock.
	unique_lock<mutex> LockFlush() {
		return unique_lock<mutex>(flush_lock);
	}
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

	//! Serializes pushes of the in-memory WAL buffer to the OS against writes into it (entry appends, flush
	//! markers), truncation, header writes, and updates of written_offset - see LockFlush(). Distinct from the
	//! storage manager WAL lock so the sync leader's batched push does not deadlock pending-commit drains.
	mutex flush_lock;

	//! Group commit state.
	//! Offsets are logical byte counters (BufferedFileWriter::GetTotalWritten), they increase monotonically
	//! across committed flush markers and are not affected by truncation of reverted (uncommitted) entries.
	//! Logical bytes pushed to the operating system so far - updated under flush_lock together with the buffer push.
	atomic<idx_t> written_offset {0};
	//! Protects synced_offset and sync_in_progress.
	mutex sync_mutex;
	//! Signalled when a leader finishes an fsync.
	std::condition_variable sync_cv;
	//! Logical bytes that are durable on disk.
	idx_t synced_offset = 0;
	//! Whether a sync leader is currently performing an fsync.
	bool sync_in_progress = false;
	//! The number of threads currently waiting for an fsync to complete.
	idx_t sync_waiters = 0;
	//! Whether concurrent commit activity was detected during the last fsync - if set, the next sync leader
	//! briefly waits for concurrent committers to append their flush markers before fsync-ing (micro-batching).
	//! The maximum wait is controlled by the group_commit_delay setting.
	bool batch_commits = false;
	//! Exponentially weighted moving average of the observed fsync duration in microseconds - used to scale the
	//! micro-batching window when group_commit_delay is -1 (automatic).
	atomic<idx_t> sync_duration_micros {0};
	//! Highest marker offset whose commit references optimistically written row group data (group commit only).
	//! Updated in WriteFlushMarker BEFORE written_offset advances, so that any sync leader whose fsync covers
	//! the marker observes the block sync requirement.
	atomic<idx_t> block_sync_pending_offset {0};
	//! Marker offset up to which the referenced row group blocks are known durable in the database file.
	//! Only updated by sync leaders (serialized via sync_in_progress).
	atomic<idx_t> block_synced_offset {0};
};

} // namespace duckdb
