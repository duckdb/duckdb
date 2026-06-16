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
	void WriteSetTable(const string &schema, const string &table);

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
	//! Flat-combining group commit: fsync the WAL so that every byte up to (at least) my_offset is durable. Called with
	//! the WAL lock NOT held, so multiple committers can coalesce onto a single physical fsync. A committer returns
	//! from this call only once durable_offset >= my_offset, i.e. its WAL_FLUSH marker is on stable storage.
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
	//! Group-commit (flat combining) fsync coordination, lock-free on a single futex-backed word.
	enum class SyncState : uint64_t { IDLE = 0, SYNCING = 1, SYNC_PENDING = 2 };
	static constexpr uint64_t kStateBits = 2;
	static constexpr uint64_t kStateMask = (uint64_t(1) << kStateBits) - 1;
	static uint64_t MakeSyncWord(SyncState s, uint64_t round) {
		return (round << kStateBits) | static_cast<uint64_t>(s);
	}
	static SyncState SyncWordState(uint64_t w) {
		return static_cast<SyncState>(w & kStateMask);
	}
	static uint64_t SyncWordRound(uint64_t w) {
		return w >> kStateBits;
	}
	//! Lead one grouped fsync (plus any SYNC_PENDING re-sync) after winning the IDLE->SYNCING election.
	void RunSyncLeader(uint64_t round);

	//! Coordination word: [round : 63..2][state : 1..0]. round increments on every word-changing transition so a
	//! parked follower cannot alias a prior state -- this defeats ABA and lost wakeups (load-bearing, not diagnostic).
	atomic<uint64_t> sync_state {MakeSyncWord(SyncState::IDLE, 0)};
	//! Highest WAL byte offset known durable (fsynced). Raise-only; release-stored by the leader strictly AFTER
	//! writer->handle->Sync() returns, acquire-loaded as the sole durable-before-ack predicate.
	atomic<idx_t> durable_offset {0};
	//! Highest WAL byte offset pushed to the page cache (writer->Flush()). Release-stored under the WAL lock by
	//! FlushAppendNoSync (so it is monotonic and needs no sync coordination), acquire-loaded by the leader to size
	//! its fsync, and clamped down by Truncate.
	atomic<idx_t> flushed_offset {0};
};

} // namespace duckdb
