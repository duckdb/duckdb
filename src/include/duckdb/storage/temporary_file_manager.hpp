//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/temporary_file_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enum_class_hash.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class TemporaryFileManager;

//===--------------------------------------------------------------------===//
// TemporaryBufferSize
//===--------------------------------------------------------------------===//
static constexpr uint64_t TEMPORARY_BUFFER_SIZE_GRANULARITY = 32ULL * 1024ULL;

enum class TemporaryBufferSize : uint64_t {
	INVALID = 0,
	S32K = 32768,
	S64K = 65536,
	S96K = 98304,
	S128K = 131072,
	S160K = 163840,
	S192K = 196608,
	S224K = 229376,
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
private:
	template <class T>
	using temporary_buffer_size_map_t = unordered_map<TemporaryBufferSize, T, EnumClassHash>;
	using temporary_file_map_t = unordered_map<idx_t, unique_ptr<TemporaryFileHandle>>;

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
	temporary_buffer_size_map_t<temporary_file_map_t> files;
};

//===--------------------------------------------------------------------===//
// TemporaryFileCompressionLevel/TemporaryFileCompressionAdaptivity
//===--------------------------------------------------------------------===//
enum class TemporaryCompressionLevel : int {
	ZSTD_MINUS_FIVE = -5,
	ZSTD_MINUS_THREE = -3,
	ZSTD_MINUS_ONE = -1,
	UNCOMPRESSED = 0,
	ZSTD_ONE = 1,
	ZSTD_THREE = 3,
	ZSTD_FIVE = 5,
};

class TemporaryFileCompressionAdaptivity {
public:
	TemporaryFileCompressionAdaptivity();

public:
	//! Get current time in nanoseconds to measure write times
	static int64_t GetCurrentTimeNanos();
	//! Get the compression level to use based on current write times
	TemporaryCompressionLevel GetCompressionLevel();
	//! Update write time for given compression level
	void Update(TemporaryCompressionLevel level, int64_t time_before_ns);

private:
	//! Convert from level to index into write time array and back
	static TemporaryCompressionLevel IndexToLevel(idx_t index);
	static idx_t LevelToIndex(TemporaryCompressionLevel level);
	//! Min/max compression levels
	static TemporaryCompressionLevel MinimumCompressionLevel();
	static TemporaryCompressionLevel MaximumCompressionLevel();

private:
	//! The value to initialize the atomic write counters to
	static constexpr int64_t INITIAL_NS = 50000;
	//! How many compression levels we adapt between
	static constexpr idx_t LEVELS = 6;
	//! Bias towards compressed writes: we only choose uncompressed if it is more than 2x faster than compressed
	static constexpr double DURATION_RATIO_THRESHOLD = 2.0;
	//! Probability to deviate from the current best write behavior (1 in 20)
	static constexpr double COMPRESSION_DEVIATION = 0.5;
	//! Weight to use for moving weighted average
	static constexpr int64_t WEIGHT = 16;

	//! Random engine to (sometimes) randomize compression
	RandomEngine random_engine;
	//! Duration of the last uncompressed write
	int64_t last_uncompressed_write_ns;
	//! Duration of the last compressed writes
	int64_t last_compressed_writes_ns[LEVELS];
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

private:
	struct CompressionResult {
		TemporaryBufferSize size;
		TemporaryCompressionLevel level;
	};

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
	//! Compress buffer, write it in compressed_buffer and return the size/level
	CompressionResult CompressBuffer(TemporaryFileCompressionAdaptivity &compression_adaptivity, FileBuffer &buffer,
	                                 AllocatedData &compressed_buffer);

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
	unordered_map<TemporaryBufferSize, BlockIndexManager, EnumClassHash> index_managers;
	//! The size in bytes of the temporary files that are currently alive
	atomic<idx_t> size_on_disk;
	//! The max amount of disk space that can be used
	idx_t max_swap_space;
	//! How many compression adaptivities we have so that threads don't all share the same one
	static constexpr idx_t COMPRESSION_ADAPTIVITIES = 64;
	//! Class that oversees when/how much to compress
	array<TemporaryFileCompressionAdaptivity, COMPRESSION_ADAPTIVITIES> compression_adaptivities;
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
