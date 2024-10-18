//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/temporary_file_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BlockIndexManager
//===--------------------------------------------------------------------===//

class TemporaryFileManager;

struct BlockIndexManager {
public:
	explicit BlockIndexManager(TemporaryFileManager &manager);
	BlockIndexManager();

public:
	//! Obtains a new block index from the index manager
	idx_t GetNewBlockIndex();
	//! Removes an index from the block manager
	//! Returns true if the max_index has been altered
	bool RemoveIndex(idx_t index);
	idx_t GetMaxIndex();
	bool HasFreeBlocks();

private:
	void SetMaxIndex(idx_t blocks);
	idx_t GetNewBlockIndexInternal();

private:
	idx_t max_index;
	set<idx_t> free_indexes;
	set<idx_t> indexes_in_use;
	optional_ptr<TemporaryFileManager> manager;
};

//===--------------------------------------------------------------------===//
// TemporaryFileIndex
//===--------------------------------------------------------------------===//

// FIXME: should be optional_idx
struct TemporaryFileIndex {
	explicit TemporaryFileIndex(idx_t file_index = DConstants::INVALID_INDEX,
	                            idx_t block_index = DConstants::INVALID_INDEX);

	idx_t file_index;
	idx_t block_index;

public:
	bool IsValid() const;
};

//===--------------------------------------------------------------------===//
// TemporaryFileHandle
//===--------------------------------------------------------------------===//

class TemporaryFileHandle {
	constexpr static idx_t MAX_ALLOWED_INDEX_BASE = 4000;

public:
	TemporaryFileHandle(idx_t temp_file_count, DatabaseInstance &db, const string &temp_directory, idx_t index,
	                    TemporaryFileManager &manager);

public:
	struct TemporaryFileLock {
	public:
		explicit TemporaryFileLock(mutex &mutex);

	public:
		lock_guard<mutex> lock;
	};

public:
	TemporaryFileIndex TryGetBlockIndex();
	void WriteTemporaryFile(FileBuffer &buffer, TemporaryFileIndex index);
	unique_ptr<FileBuffer> ReadTemporaryBuffer(idx_t block_index, unique_ptr<FileBuffer> reusable_buffer);
	void EraseBlockIndex(block_id_t block_index);
	bool DeleteIfEmpty();
	TemporaryFileInformation GetTemporaryFile();

private:
	void CreateFileIfNotExists(TemporaryFileLock &);
	void RemoveTempBlockIndex(TemporaryFileLock &, idx_t index);
	idx_t GetPositionInFile(idx_t index);

private:
	const idx_t max_allowed_index;
	DatabaseInstance &db;
	unique_ptr<FileHandle> handle;
	idx_t file_index;
	string path;
	mutex file_lock;
	BlockIndexManager index_manager;
};

//===--------------------------------------------------------------------===//
// TemporaryDirectoryHandle
//===--------------------------------------------------------------------===//

class TemporaryDirectoryHandle {
public:
	TemporaryDirectoryHandle(DatabaseInstance &db, string path_p, optional_idx max_swap_space);
	~TemporaryDirectoryHandle();

	TemporaryFileManager &GetTempFile();

private:
	DatabaseInstance &db;
	string temp_directory;
	bool created_directory = false;
	unique_ptr<TemporaryFileManager> temp_file;
};

//===--------------------------------------------------------------------===//
// TemporaryFileManager
//===--------------------------------------------------------------------===//

class TemporaryFileManager {
public:
	TemporaryFileManager(DatabaseInstance &db, const string &temp_directory_p);
	~TemporaryFileManager();

public:
	struct TemporaryManagerLock {
	public:
		explicit TemporaryManagerLock(mutex &mutex);

	public:
		lock_guard<mutex> lock;
	};

	void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer);
	bool HasTemporaryBuffer(block_id_t block_id);
	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> reusable_buffer);
	void DeleteTemporaryBuffer(block_id_t id);
	vector<TemporaryFileInformation> GetTemporaryFiles();
	idx_t GetTotalUsedSpaceInBytes();
	optional_idx GetMaxSwapSpace() const;
	void SetMaxSwapSpace(optional_idx limit);
	//! Register temporary file size growth
	void IncreaseSizeOnDisk(idx_t amount);
	//! Register temporary file size decrease
	void DecreaseSizeOnDisk(idx_t amount);

private:
	void EraseUsedBlock(TemporaryManagerLock &lock, block_id_t id, TemporaryFileHandle *handle,
	                    TemporaryFileIndex index);
	TemporaryFileHandle *GetFileHandle(TemporaryManagerLock &, idx_t index);
	TemporaryFileIndex GetTempBlockIndex(TemporaryManagerLock &, block_id_t id);
	void EraseFileHandle(TemporaryManagerLock &, idx_t file_index);

private:
	DatabaseInstance &db;
	mutex manager_lock;
	//! The temporary directory
	string temp_directory;
	//! The set of active temporary file handles
	unordered_map<idx_t, unique_ptr<TemporaryFileHandle>> files;
	//! map of block_id -> temporary file position
	unordered_map<block_id_t, TemporaryFileIndex> used_blocks;
	//! Manager of in-use temporary file indexes
	BlockIndexManager index_manager;
	//! The size in bytes of the temporary files that are currently alive
	atomic<idx_t> size_on_disk;
	//! The max amount of disk space that can be used
	idx_t max_swap_space;
};

} // namespace duckdb
