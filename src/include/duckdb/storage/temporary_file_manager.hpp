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
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class TemporaryFileManager;

//===--------------------------------------------------------------------===//
// TemporaryBufferSize
//===--------------------------------------------------------------------===//
static constexpr idx_t TEMPORARY_BUFFER_SIZE_GRANULARITY = 32ULL * 1024ULL;

enum class TemporaryBufferSize : idx_t {
	INVALID = 0ULL * TEMPORARY_BUFFER_SIZE_GRANULARITY,
	THIRTY_TWO_KB = 1ULL * TEMPORARY_BUFFER_SIZE_GRANULARITY,
	SIXTY_FOUR_KB = 2ULL * TEMPORARY_BUFFER_SIZE_GRANULARITY,
	NINETY_SIX_KB = 3ULL * TEMPORARY_BUFFER_SIZE_GRANULARITY,
	HUNDRED_TWENTY_EIGHT_KB = 4ULL * TEMPORARY_BUFFER_SIZE_GRANULARITY,
	HUNDRED_SIXTY_KB = 5ULL * TEMPORARY_BUFFER_SIZE_GRANULARITY,
	HUNDRED_NINETY_TWO_KB = 6ULL * TEMPORARY_BUFFER_SIZE_GRANULARITY,
	TWO_HUNDRED_TWENTY_FOUR_KB = 7ULL * TEMPORARY_BUFFER_SIZE_GRANULARITY,
	DEFAULT = DEFAULT_BLOCK_ALLOC_SIZE,
};

//===--------------------------------------------------------------------===//
// TemporaryFileIdentifier/TemporaryFileIndex
//===--------------------------------------------------------------------===//
struct TemporaryFileIdentifier {
public:
	TemporaryFileIdentifier();
	TemporaryFileIdentifier(TemporaryBufferSize size, idx_t file_index);

public:
	//! Whether this temporary file identifier is valid (fields have been set)
	bool IsValid() const;

public:
	//! The size of the buffers within the temp file
	TemporaryBufferSize size;
	//! The index of the temp file
	optional_idx file_index;
};

struct TemporaryFileIndex {
public:
	TemporaryFileIndex();
	TemporaryFileIndex(TemporaryFileIdentifier identifier, idx_t block_index);

public:
	//! Whether this temporary file index is valid (fields have been set)
	bool IsValid() const;

public:
	//! The identifier for the temporary file
	TemporaryFileIdentifier identifier;
	//! The block index within the temporary file
	optional_idx block_index;
};

//===--------------------------------------------------------------------===//
// BlockIndexManager
//===--------------------------------------------------------------------===//
struct BlockIndexManager {
public:
	BlockIndexManager();
	explicit BlockIndexManager(TemporaryFileManager &manager);

public:
	//! Obtains a new block index from the index manager
	idx_t GetNewBlockIndex(TemporaryBufferSize size);
	//! Removes an index from the block manager (returns true if the max_index has been altered)
	bool RemoveIndex(idx_t index, TemporaryBufferSize size);
	//! Get the maximum block index
	idx_t GetMaxIndex() const;
	//! Whether there are free blocks available within the file
	bool HasFreeBlocks() const;

private:
	//! Get/set max block index
	idx_t GetNewBlockIndexInternal(TemporaryBufferSize size);
	void SetMaxIndex(idx_t new_index, TemporaryBufferSize size);

private:
	//! The maximum block index
	idx_t max_index;
	//! Free indexes within the file
	set<idx_t> free_indexes;
	//! Used indexes within the file
	set<idx_t> indexes_in_use;
	//! The TemporaryFileManager that "owns" this BlockIndexManager
	optional_ptr<TemporaryFileManager> manager;
};

//===--------------------------------------------------------------------===//
// TemporaryFileHandle
//===--------------------------------------------------------------------===//
class TemporaryFileHandle {
	constexpr static idx_t MAX_ALLOWED_INDEX_BASE = 4000;

public:
	TemporaryFileHandle(TemporaryFileManager &manager, TemporaryFileIdentifier identifier, idx_t temp_file_count);

public:
	struct TemporaryFileLock {
	public:
		explicit TemporaryFileLock(mutex &mutex);

	public:
		lock_guard<mutex> lock;
	};

public:
	//! Try to get an index of where to write in this file. Returns an invalid index if full
	TemporaryFileIndex TryGetBlockIndex();
	//! Remove block index from this TemporaryFileHandle
	void EraseBlockIndex(block_id_t block_index);

	//! Read/Write temporary buffers at given positions in this file (potentially compressed)
	unique_ptr<FileBuffer> ReadTemporaryBuffer(idx_t block_index, unique_ptr<FileBuffer> reusable_buffer) const;
	void WriteTemporaryBuffer(FileBuffer &buffer, idx_t block_index, AllocatedData &compressed_buffer) const;

	//! Deletes the file if there are no more blocks
	bool DeleteIfEmpty();
	//! Get information about this temporary file
	TemporaryFileInformation GetTemporaryFile();

private:
	//! Create temporary file if it did not exist yet
	void CreateFileIfNotExists(TemporaryFileLock &);
	//! Remove block index from this file
	void RemoveTempBlockIndex(TemporaryFileLock &, idx_t index);
	//! Get the position of a block in the file
	idx_t GetPositionInFile(idx_t index) const;

private:
	//! Reference to the DB instance
	DatabaseInstance &db;
	//! The identifier (size/file index) of this TemporaryFileHandle
	const TemporaryFileIdentifier identifier;
	//! The maximum allowed index
	const idx_t max_allowed_index;
	//! File path/handle
	const string path;
	unique_ptr<FileHandle> handle;
	//! Lock for concurrent access and block index manager
	mutex file_lock;
	BlockIndexManager index_manager;
};

//===--------------------------------------------------------------------===//
// TemporaryFileMap
//===--------------------------------------------------------------------===//
class TemporaryFileMap {
	using temporary_file_map_t = map<idx_t, unique_ptr<TemporaryFileHandle>>;

public:
	explicit TemporaryFileMap(TemporaryFileManager &manager);
	void Clear();

public:
	//! Gets the map for the given size
	temporary_file_map_t &GetMapForSize(TemporaryBufferSize size);

	//! Get/create/erase a TemporaryFileHandle for a size/index
	optional_ptr<TemporaryFileHandle> GetFile(const TemporaryFileIdentifier &identifier);
	TemporaryFileHandle &CreateFile(const TemporaryFileIdentifier &identifier);
	void EraseFile(const TemporaryFileIdentifier &identifier);

private:
	TemporaryFileManager &manager;
	unordered_map<TemporaryBufferSize, temporary_file_map_t> files;
};

//===--------------------------------------------------------------------===//
// TemporaryFileManager
//===--------------------------------------------------------------------===//
class TemporaryFileManager {
	friend struct BlockIndexManager;
	friend class TemporaryFileHandle;

public:
	TemporaryFileManager(DatabaseInstance &db, const string &temp_directory_p);
	~TemporaryFileManager();

public:
	struct TemporaryFileManagerLock {
	public:
		explicit TemporaryFileManagerLock(mutex &mutex);

	public:
		lock_guard<mutex> lock;
	};

	//! Create/Read/Update/Delete operations for temporary buffers
	void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer);
	bool HasTemporaryBuffer(block_id_t block_id);
	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> reusable_buffer);
	void DeleteTemporaryBuffer(block_id_t id);

	//! Get the list of temporary files and their sizes
	vector<TemporaryFileInformation> GetTemporaryFiles();

	//! Get/set maximum swap space
	optional_idx GetMaxSwapSpace() const;
	void SetMaxSwapSpace(optional_idx limit);

	//! Get temporary file size
	idx_t GetTotalUsedSpaceInBytes() const;
	//! Register temporary file size growth
	void IncreaseSizeOnDisk(idx_t amount);
	//! Register temporary file size decrease
	void DecreaseSizeOnDisk(idx_t amount);

private:
	//! Compress buffer, write it in compressed_buffer and return the size
	TemporaryBufferSize CompressTemporaryBuffer(FileBuffer &buffer, AllocatedData &compressed_buffer) const;

	//! Create file name for given size/index
	string CreateTemporaryFileName(const TemporaryFileIdentifier &identifier) const;

	//! Get/erase a temporary block
	TemporaryFileIndex GetTempBlockIndex(TemporaryFileManagerLock &, block_id_t id);
	void EraseUsedBlock(TemporaryFileManagerLock &lock, block_id_t id, TemporaryFileHandle &handle,
	                    TemporaryFileIndex index);

	//! Get/erase a temporary file handle
	optional_ptr<TemporaryFileHandle> GetFileHandle(TemporaryFileManagerLock &,
	                                                const TemporaryFileIdentifier &identifier);
	void EraseFileHandle(TemporaryFileManagerLock &, const TemporaryFileIdentifier &identifier);

private:
	//! Reference to the DB instance
	DatabaseInstance &db;
	//! The temporary directory
	string temp_directory;
	//! Lock for parallel access
	mutex manager_lock;
	//! The set of active temporary file handles
	TemporaryFileMap files;
	//! Map of block_id -> temporary file position
	unordered_map<block_id_t, TemporaryFileIndex> used_blocks;
	//! Map of TemporaryBufferSize -> manager of in-use temporary file indexes
	unordered_map<TemporaryBufferSize, BlockIndexManager> index_managers;
	//! The size in bytes of the temporary files that are currently alive
	atomic<idx_t> size_on_disk;
	//! The max amount of disk space that can be used
	idx_t max_swap_space;
};

//===--------------------------------------------------------------------===//
// TemporaryDirectoryHandle
//===--------------------------------------------------------------------===//
class TemporaryDirectoryHandle {
public:
	TemporaryDirectoryHandle(DatabaseInstance &db, string path_p, optional_idx max_swap_space);
	~TemporaryDirectoryHandle();

public:
	TemporaryFileManager &GetTempFile() const;

private:
	DatabaseInstance &db;
	string temp_directory;
	bool created_directory = false;
	unique_ptr<TemporaryFileManager> temp_file;
};

} // namespace duckdb
