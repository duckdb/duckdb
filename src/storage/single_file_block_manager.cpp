#include "duckdb/storage/single_file_block_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

const char MainHeader::MAGIC_BYTES[] = "DUCK";

void SerializeVersionNumber(WriteStream &ser, const string &version_str) {
	data_t version[MainHeader::MAX_VERSION_SIZE];
	memset(version, 0, MainHeader::MAX_VERSION_SIZE);
	memcpy(version, version_str.c_str(), MinValue<idx_t>(version_str.size(), MainHeader::MAX_VERSION_SIZE));
	ser.WriteData(version, MainHeader::MAX_VERSION_SIZE);
}

void DeserializeVersionNumber(ReadStream &stream, data_t *dest) {
	memset(dest, 0, MainHeader::MAX_VERSION_SIZE);
	stream.ReadData(dest, MainHeader::MAX_VERSION_SIZE);
}

void MainHeader::Write(WriteStream &ser) {
	ser.WriteData(const_data_ptr_cast(MAGIC_BYTES), MAGIC_BYTE_SIZE);
	ser.Write<uint64_t>(version_number);
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		ser.Write<uint64_t>(flags[i]);
	}
	SerializeVersionNumber(ser, DuckDB::LibraryVersion());
	SerializeVersionNumber(ser, DuckDB::SourceID());
}

void MainHeader::CheckMagicBytes(FileHandle &handle) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	if (handle.GetFileSize() < MainHeader::MAGIC_BYTE_SIZE + MainHeader::MAGIC_BYTE_OFFSET) {
		throw IOException("The file \"%s\" exists, but it is not a valid DuckDB database file!", handle.path);
	}
	handle.Read(magic_bytes, MainHeader::MAGIC_BYTE_SIZE, MainHeader::MAGIC_BYTE_OFFSET);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file \"%s\" exists, but it is not a valid DuckDB database file!", handle.path);
	}
}

MainHeader MainHeader::Read(ReadStream &source) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	MainHeader header;
	source.ReadData(magic_bytes, MainHeader::MAGIC_BYTE_SIZE);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file is not a valid DuckDB database file!");
	}
	header.version_number = source.Read<uint64_t>();
	// check the version number
	if (header.version_number != VERSION_NUMBER) {
		auto version = GetDuckDBVersion(header.version_number);
		string version_text;
		if (!version.empty()) {
			// known version
			version_text = "DuckDB version " + string(version);
		} else {
			version_text = string("an ") + (VERSION_NUMBER > header.version_number ? "older development" : "newer") +
			               string(" version of DuckDB");
		}
		throw IOException(
		    "Trying to read a database file with version number %lld, but we can only read version %lld.\n"
		    "The database file was created with %s.\n\n"
		    "The storage of DuckDB is not yet stable; newer versions of DuckDB cannot read old database files and "
		    "vice versa.\n"
		    "The storage will be stabilized when version 1.0 releases.\n\n"
		    "For now, we recommend that you load the database file in a supported version of DuckDB, and use the "
		    "EXPORT DATABASE command "
		    "followed by IMPORT DATABASE on the current version of DuckDB.\n\n"
		    "See the storage page for more information: https://duckdb.org/internals/storage",
		    header.version_number, VERSION_NUMBER, version_text);
	}
	// read the flags
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		header.flags[i] = source.Read<uint64_t>();
	}
	DeserializeVersionNumber(source, header.library_git_desc);
	DeserializeVersionNumber(source, header.library_git_hash);
	return header;
}

void DatabaseHeader::Write(WriteStream &ser) {
	ser.Write<uint64_t>(iteration);
	ser.Write<idx_t>(meta_block);
	ser.Write<idx_t>(free_list);
	ser.Write<uint64_t>(block_count);
	ser.Write<idx_t>(block_alloc_size);
	ser.Write<idx_t>(vector_size);
}

DatabaseHeader DatabaseHeader::Read(ReadStream &source) {
	DatabaseHeader header;
	header.iteration = source.Read<uint64_t>();
	header.meta_block = source.Read<idx_t>();
	header.free_list = source.Read<idx_t>();
	header.block_count = source.Read<uint64_t>();

	header.block_alloc_size = source.Read<idx_t>();

	// backwards compatibility
	if (!header.block_alloc_size) {
		header.block_alloc_size = DEFAULT_BLOCK_ALLOC_SIZE;
	}

	header.vector_size = source.Read<idx_t>();
	if (!header.vector_size) {
		// backwards compatibility
		header.vector_size = DEFAULT_STANDARD_VECTOR_SIZE;
	}
	if (header.vector_size != STANDARD_VECTOR_SIZE) {
		throw IOException("Cannot read database file: DuckDB's compiled vector size is %llu bytes, but the file has a "
		                  "vector size of %llu bytes.",
		                  STANDARD_VECTOR_SIZE, header.vector_size);
	}

	return header;
}

template <class T>
void SerializeHeaderStructure(T header, data_ptr_t ptr) {
	MemoryStream ser(ptr, Storage::FILE_HEADER_SIZE);
	header.Write(ser);
}

template <class T>
T DeserializeHeaderStructure(data_ptr_t ptr) {
	MemoryStream source(ptr, Storage::FILE_HEADER_SIZE);
	return T::Read(source);
}

SingleFileBlockManager::SingleFileBlockManager(AttachedDatabase &db, const string &path_p,
                                               const StorageManagerOptions &options)
    : BlockManager(BufferManager::GetBufferManager(db), options.block_alloc_size), db(db), path(path_p),
      header_buffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER,
                    Storage::FILE_HEADER_SIZE - Storage::DEFAULT_BLOCK_HEADER_SIZE),
      iteration_count(0), options(options) {
}

FileOpenFlags SingleFileBlockManager::GetFileFlags(bool create_new) const {
	FileOpenFlags result;
	if (options.read_only) {
		D_ASSERT(!create_new);
		result = FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS | FileLockType::READ_LOCK;
	} else {
		result = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ | FileLockType::WRITE_LOCK;
		if (create_new) {
			result |= FileFlags::FILE_FLAGS_FILE_CREATE;
		}
	}
	if (options.use_direct_io) {
		result |= FileFlags::FILE_FLAGS_DIRECT_IO;
	}
	// database files can be read from in parallel
	result |= FileFlags::FILE_FLAGS_PARALLEL_ACCESS;
	return result;
}

void SingleFileBlockManager::CreateNewDatabase() {
	auto flags = GetFileFlags(true);

	// open the RDBMS handle
	auto &fs = FileSystem::Get(db);
	handle = fs.OpenFile(path, flags);

	// if we create a new file, we fill the metadata of the file
	// first fill in the new header
	header_buffer.Clear();

	MainHeader main_header;
	main_header.version_number = VERSION_NUMBER;
	memset(main_header.flags, 0, sizeof(uint64_t) * 4);

	SerializeHeaderStructure<MainHeader>(main_header, header_buffer.buffer);
	// now write the header to the file
	ChecksumAndWrite(header_buffer, 0);
	header_buffer.Clear();

	// write the database headers
	// initialize meta_block and free_list to INVALID_BLOCK because the database file does not contain any actual
	// content yet
	DatabaseHeader h1;
	// header 1
	h1.iteration = 0;
	h1.meta_block = idx_t(INVALID_BLOCK);
	h1.free_list = idx_t(INVALID_BLOCK);
	h1.block_count = 0;
	// We create the SingleFileBlockManager with the desired block allocation size before calling CreateNewDatabase.
	h1.block_alloc_size = GetBlockAllocSize();
	h1.vector_size = STANDARD_VECTOR_SIZE;
	SerializeHeaderStructure<DatabaseHeader>(h1, header_buffer.buffer);
	ChecksumAndWrite(header_buffer, Storage::FILE_HEADER_SIZE);

	// header 2
	DatabaseHeader h2;
	h2.iteration = 0;
	h2.meta_block = idx_t(INVALID_BLOCK);
	h2.free_list = idx_t(INVALID_BLOCK);
	h2.block_count = 0;
	// We create the SingleFileBlockManager with the desired block allocation size before calling CreateNewDatabase.
	h2.block_alloc_size = GetBlockAllocSize();
	h2.vector_size = STANDARD_VECTOR_SIZE;
	SerializeHeaderStructure<DatabaseHeader>(h2, header_buffer.buffer);
	ChecksumAndWrite(header_buffer, Storage::FILE_HEADER_SIZE * 2ULL);

	// ensure that writing to disk is completed before returning
	handle->Sync();
	// we start with h2 as active_header, this way our initial write will be in h1
	iteration_count = 0;
	active_header = 1;
	max_block = 0;
}

void SingleFileBlockManager::LoadExistingDatabase() {
	auto flags = GetFileFlags(false);

	// open the RDBMS handle
	auto &fs = FileSystem::Get(db);
	handle = fs.OpenFile(path, flags);
	if (!handle) {
		// this can only happen in read-only mode - as that is when we set FILE_FLAGS_NULL_IF_NOT_EXISTS
		throw IOException("Cannot open database \"%s\" in read-only mode: database does not exist", path);
	}

	MainHeader::CheckMagicBytes(*handle);
	// otherwise, we check the metadata of the file
	ReadAndChecksum(header_buffer, 0);
	DeserializeHeaderStructure<MainHeader>(header_buffer.buffer);

	// read the database headers from disk
	DatabaseHeader h1;
	ReadAndChecksum(header_buffer, Storage::FILE_HEADER_SIZE);
	h1 = DeserializeHeaderStructure<DatabaseHeader>(header_buffer.buffer);

	DatabaseHeader h2;
	ReadAndChecksum(header_buffer, Storage::FILE_HEADER_SIZE * 2ULL);
	h2 = DeserializeHeaderStructure<DatabaseHeader>(header_buffer.buffer);

	// check the header with the highest iteration count
	if (h1.iteration > h2.iteration) {
		// h1 is active header
		active_header = 0;
		Initialize(h1, GetOptionalBlockAllocSize());
	} else {
		// h2 is active header
		active_header = 1;
		Initialize(h2, GetOptionalBlockAllocSize());
	}
	LoadFreeList();
}

void SingleFileBlockManager::ReadAndChecksum(FileBuffer &block, uint64_t location) const {
	// read the buffer from disk
	block.Read(*handle, location);

	// compute the checksum
	auto stored_checksum = Load<uint64_t>(block.InternalBuffer());
	auto computed_checksum = Checksum(block.buffer, block.size);

	// verify the checksum
	if (stored_checksum != computed_checksum) {
		throw IOException("Corrupt database file: computed checksum %llu does not match stored checksum %llu in block "
		                  "at location %llu",
		                  computed_checksum, stored_checksum, location);
	}
}

void SingleFileBlockManager::ChecksumAndWrite(FileBuffer &block, uint64_t location) const {
	// compute the checksum and write it to the start of the buffer (if not temp buffer)
	uint64_t checksum = Checksum(block.buffer, block.size);
	Store<uint64_t>(checksum, block.InternalBuffer());
	// now write the buffer
	block.Write(*handle, location);
}

void SingleFileBlockManager::Initialize(const DatabaseHeader &header, const optional_idx block_alloc_size) {
	free_list_id = header.free_list;
	meta_block = header.meta_block;
	iteration_count = header.iteration;
	max_block = NumericCast<block_id_t>(header.block_count);

	if (block_alloc_size.IsValid() && block_alloc_size.GetIndex() != header.block_alloc_size) {
		throw InvalidInputException("cannot initialize the same database with a different block size: provided block "
		                            "size: %llu, file block size: %llu",
		                            GetBlockAllocSize(), header.block_alloc_size);
	}
	SetBlockAllocSize(header.block_alloc_size);
}

void SingleFileBlockManager::LoadFreeList() {
	MetaBlockPointer free_pointer(free_list_id, 0);
	if (!free_pointer.IsValid()) {
		// no free list
		return;
	}
	MetadataReader reader(GetMetadataManager(), free_pointer, nullptr, BlockReaderType::REGISTER_BLOCKS);
	auto free_list_count = reader.Read<uint64_t>();
	free_list.clear();
	for (idx_t i = 0; i < free_list_count; i++) {
		auto block = reader.Read<block_id_t>();
		free_list.insert(block);
		newly_freed_list.insert(block);
	}
	auto multi_use_blocks_count = reader.Read<uint64_t>();
	multi_use_blocks.clear();
	for (idx_t i = 0; i < multi_use_blocks_count; i++) {
		auto block_id = reader.Read<block_id_t>();
		auto usage_count = reader.Read<uint32_t>();
		multi_use_blocks[block_id] = usage_count;
	}
	GetMetadataManager().Read(reader);
	GetMetadataManager().MarkBlocksAsModified();
}

bool SingleFileBlockManager::IsRootBlock(MetaBlockPointer root) {
	return root.block_pointer == meta_block;
}

block_id_t SingleFileBlockManager::GetFreeBlockId() {
	lock_guard<mutex> lock(block_lock);
	block_id_t block;
	if (!free_list.empty()) {
		// The free list is not empty, so we take its first element.
		block = *free_list.begin();
		// erase the entry from the free list again
		free_list.erase(free_list.begin());
		newly_freed_list.erase(block);
	} else {
		block = max_block++;
	}
	return block;
}

block_id_t SingleFileBlockManager::PeekFreeBlockId() {
	lock_guard<mutex> lock(block_lock);
	if (!free_list.empty()) {
		return *free_list.begin();
	} else {
		return max_block;
	}
}

void SingleFileBlockManager::MarkBlockAsFree(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);
	if (free_list.find(block_id) != free_list.end()) {
		throw InternalException("MarkBlockAsFree called but block %llu was already freed!", block_id);
	}
	multi_use_blocks.erase(block_id);
	free_list.insert(block_id);
	newly_freed_list.insert(block_id);
}

void SingleFileBlockManager::MarkBlockAsUsed(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	if (max_block <= block_id) {
		// the block is past the current max_block
		// in this case we need to increment  "max_block" to "block_id"
		// any blocks in the middle are added to the free list
		// i.e. if max_block = 0, and block_id = 3, we need to add blocks 1 and 2 to the free list
		while (max_block < block_id) {
			free_list.insert(max_block);
			max_block++;
		}
		max_block++;
	} else if (free_list.find(block_id) != free_list.end()) {
		// block is currently int he free list - erase
		free_list.erase(block_id);
	} else {
		// block is already in use - increase reference count
		IncreaseBlockReferenceCountInternal(block_id);
	}
}

void SingleFileBlockManager::MarkBlockAsModified(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);

	// check if the block is a multi-use block
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		// it is! reduce the reference count of the block
		entry->second--;
		// check the reference count: is the block still a multi-use block?
		if (entry->second <= 1) {
			// no longer a multi-use block!
			multi_use_blocks.erase(entry);
		}
		return;
	}
	// Check for multi-free
	// TODO: Fix the bug that causes this assert to fire, then uncomment it.
	// D_ASSERT(modified_blocks.find(block_id) == modified_blocks.end());
	D_ASSERT(free_list.find(block_id) == free_list.end());
	modified_blocks.insert(block_id);
}

void SingleFileBlockManager::IncreaseBlockReferenceCountInternal(block_id_t block_id) {
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);
	D_ASSERT(free_list.find(block_id) == free_list.end());
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		entry->second++;
	} else {
		multi_use_blocks[block_id] = 2;
	}
}

void SingleFileBlockManager::VerifyBlocks(const unordered_map<block_id_t, idx_t> &block_usage_count) {
	// probably don't need this?
	lock_guard<mutex> lock(block_lock);
	// all blocks should be accounted for - either in the block_usage_count, or in the free list
	set<block_id_t> referenced_blocks;
	for (auto &block : block_usage_count) {
		if (block.first == INVALID_BLOCK) {
			continue;
		}
		if (block.first >= max_block) {
			throw InternalException("Block %lld is used, but it is bigger than the max block %d", block.first,
			                        max_block);
		}
		referenced_blocks.insert(block.first);
		if (block.second > 1) {
			// multi-use block
			auto entry = multi_use_blocks.find(block.first);
			if (entry == multi_use_blocks.end()) {
				throw InternalException("Block %lld was used %llu times, but not present in multi_use_blocks",
				                        block.first, block.second);
			}
			if (entry->second != block.second) {
				throw InternalException(
				    "Block %lld was used %llu times, but multi_use_blocks says it is used %llu times", block.first,
				    block.second, entry->second);
			}
		} else {
			D_ASSERT(block.second > 0);
			auto entry = free_list.find(block.first);
			if (entry != free_list.end()) {
				throw InternalException("Block %lld was used, but it is present in the free list", block.first);
			}
		}
	}
	for (auto &free_block : free_list) {
		referenced_blocks.insert(free_block);
	}
	if (referenced_blocks.size() != NumericCast<idx_t>(max_block)) {
		// not all blocks are accounted for
		string missing_blocks;
		for (block_id_t i = 0; i < max_block; i++) {
			if (referenced_blocks.find(i) == referenced_blocks.end()) {
				if (!missing_blocks.empty()) {
					missing_blocks += ", ";
				}
				missing_blocks += to_string(i);
			}
		}
		throw InternalException(
		    "Blocks %s were neither present in the free list or in the block_usage_count (max block %lld)",
		    missing_blocks, max_block);
	}
}

void SingleFileBlockManager::IncreaseBlockReferenceCount(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	IncreaseBlockReferenceCountInternal(block_id);
}

idx_t SingleFileBlockManager::GetMetaBlock() {
	return meta_block;
}

idx_t SingleFileBlockManager::TotalBlocks() {
	lock_guard<mutex> lock(block_lock);
	return NumericCast<idx_t>(max_block);
}

idx_t SingleFileBlockManager::FreeBlocks() {
	lock_guard<mutex> lock(block_lock);
	return free_list.size();
}

bool SingleFileBlockManager::IsRemote() {
	return !handle->OnDiskFile();
}

unique_ptr<Block> SingleFileBlockManager::ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) {
	D_ASSERT(source_buffer.AllocSize() == GetBlockAllocSize());
	return make_uniq<Block>(source_buffer, block_id);
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock(block_id_t block_id, FileBuffer *source_buffer) {
	unique_ptr<Block> result;
	if (source_buffer) {
		result = ConvertBlock(block_id, *source_buffer);
	} else {
		result = make_uniq<Block>(Allocator::Get(db), block_id, GetBlockSize());
	}
	result->Initialize(options.debug_initialize);
	return result;
}

idx_t SingleFileBlockManager::GetBlockLocation(block_id_t block_id) {
	return BLOCK_START + NumericCast<idx_t>(block_id) * GetBlockAllocSize();
}

void SingleFileBlockManager::Read(Block &block) {
	D_ASSERT(block.id >= 0);
	D_ASSERT(std::find(free_list.begin(), free_list.end(), block.id) == free_list.end());
	ReadAndChecksum(block, GetBlockLocation(block.id));
}

void SingleFileBlockManager::ReadBlocks(FileBuffer &buffer, block_id_t start_block, idx_t block_count) {
	D_ASSERT(start_block >= 0);
	D_ASSERT(block_count >= 1);

	// read the buffer from disk
	auto location = GetBlockLocation(start_block);
	buffer.Read(*handle, location);

	// for each of the blocks - verify the checksum
	auto ptr = buffer.InternalBuffer();
	for (idx_t i = 0; i < block_count; i++) {
		// compute the checksum
		auto start_ptr = ptr + i * GetBlockAllocSize();
		auto stored_checksum = Load<uint64_t>(start_ptr);
		uint64_t computed_checksum = Checksum(start_ptr + Storage::DEFAULT_BLOCK_HEADER_SIZE, GetBlockSize());
		// verify the checksum
		if (stored_checksum != computed_checksum) {
			throw IOException(
			    "Corrupt database file: computed checksum %llu does not match stored checksum %llu in block "
			    "at location %llu",
			    computed_checksum, stored_checksum, location + i * GetBlockAllocSize());
		}
	}
}

void SingleFileBlockManager::Write(FileBuffer &buffer, block_id_t block_id) {
	D_ASSERT(block_id >= 0);
	ChecksumAndWrite(buffer, BLOCK_START + NumericCast<idx_t>(block_id) * GetBlockAllocSize());
}

void SingleFileBlockManager::Truncate() {
	BlockManager::Truncate();
	idx_t blocks_to_truncate = 0;
	// reverse iterate over the free-list
	for (auto entry = free_list.rbegin(); entry != free_list.rend(); entry++) {
		auto block_id = *entry;
		if (block_id + 1 != max_block) {
			break;
		}
		blocks_to_truncate++;
		max_block--;
	}
	if (blocks_to_truncate == 0) {
		// nothing to truncate
		return;
	}
	// truncate the file
	free_list.erase(free_list.lower_bound(max_block), free_list.end());
	newly_freed_list.erase(newly_freed_list.lower_bound(max_block), newly_freed_list.end());
	handle->Truncate(NumericCast<int64_t>(BLOCK_START + NumericCast<idx_t>(max_block) * GetBlockAllocSize()));
}

vector<MetadataHandle> SingleFileBlockManager::GetFreeListBlocks() {
	vector<MetadataHandle> free_list_blocks;
	auto &metadata_manager = GetMetadataManager();

	// reserve all blocks that we are going to write the free list to
	// since these blocks are no longer free we cannot just include them in the free list!
	auto block_size = metadata_manager.GetMetadataBlockSize() - sizeof(idx_t);
	idx_t allocated_size = 0;
	while (true) {
		auto free_list_size = sizeof(uint64_t) + sizeof(block_id_t) * (free_list.size() + modified_blocks.size());
		auto multi_use_blocks_size =
		    sizeof(uint64_t) + (sizeof(block_id_t) + sizeof(uint32_t)) * multi_use_blocks.size();
		auto metadata_blocks =
		    sizeof(uint64_t) + (sizeof(block_id_t) + sizeof(idx_t)) * GetMetadataManager().BlockCount();
		auto total_size = free_list_size + multi_use_blocks_size + metadata_blocks;
		if (total_size < allocated_size) {
			break;
		}
		auto free_list_handle = GetMetadataManager().AllocateHandle();
		free_list_blocks.push_back(std::move(free_list_handle));
		allocated_size += block_size;
	}

	return free_list_blocks;
}

class FreeListBlockWriter : public MetadataWriter {
public:
	FreeListBlockWriter(MetadataManager &manager, vector<MetadataHandle> free_list_blocks_p)
	    : MetadataWriter(manager), free_list_blocks(std::move(free_list_blocks_p)), index(0) {
	}

	vector<MetadataHandle> free_list_blocks;
	idx_t index;

protected:
	MetadataHandle NextHandle() override {
		if (index >= free_list_blocks.size()) {
			throw InternalException(
			    "Free List Block Writer ran out of blocks, this means not enough blocks were allocated up front");
		}
		return std::move(free_list_blocks[index++]);
	}
};

void SingleFileBlockManager::WriteHeader(DatabaseHeader header) {
	auto free_list_blocks = GetFreeListBlocks();

	// now handle the free list
	auto &metadata_manager = GetMetadataManager();
	// add all modified blocks to the free list: they can now be written to again
	metadata_manager.MarkBlocksAsModified();

	lock_guard<mutex> lock(block_lock);
	// set the iteration count
	header.iteration = ++iteration_count;

	for (auto &block : modified_blocks) {
		free_list.insert(block);
		newly_freed_list.insert(block);
	}
	modified_blocks.clear();

	if (!free_list_blocks.empty()) {
		// there are blocks to write, either in the free_list or in the modified_blocks
		// we write these blocks specifically to the free_list_blocks
		// a normal MetadataWriter will fetch blocks to use from the free_list
		// but since we are WRITING the free_list, this behavior is sub-optimal
		FreeListBlockWriter writer(metadata_manager, std::move(free_list_blocks));

		auto ptr = writer.GetMetaBlockPointer();
		header.free_list = ptr.block_pointer;

		writer.Write<uint64_t>(free_list.size());
		for (auto &block_id : free_list) {
			writer.Write<block_id_t>(block_id);
		}
		writer.Write<uint64_t>(multi_use_blocks.size());
		for (auto &entry : multi_use_blocks) {
			writer.Write<block_id_t>(entry.first);
			writer.Write<uint32_t>(entry.second);
		}
		GetMetadataManager().Write(writer);
		writer.Flush();
	} else {
		// no blocks in the free list
		header.free_list = DConstants::INVALID_INDEX;
	}
	metadata_manager.Flush();
	header.block_count = NumericCast<idx_t>(max_block);

	auto &config = DBConfig::Get(db);
	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE) {
		throw FatalException("Checkpoint aborted after free list write because of PRAGMA checkpoint_abort flag");
	}

	if (!options.use_direct_io) {
		// if we are not using Direct IO we need to fsync BEFORE we write the header to ensure that all the previous
		// blocks are written as well
		handle->Sync();
	}
	// set the header inside the buffer
	header_buffer.Clear();
	MemoryStream serializer;
	header.Write(serializer);
	memcpy(header_buffer.buffer, serializer.GetData(), serializer.GetPosition());
	// now write the header to the file, active_header determines whether we write to h1 or h2
	// note that if active_header is h1 we write to h2, and vice versa
	ChecksumAndWrite(header_buffer, active_header == 1 ? Storage::FILE_HEADER_SIZE : Storage::FILE_HEADER_SIZE * 2);
	// switch active header to the other header
	active_header = 1 - active_header;
	//! Ensure the header write ends up on disk
	handle->Sync();
	// Release the free blocks to the filesystem.
	TrimFreeBlocks();
}

void SingleFileBlockManager::FileSync() {
	handle->Sync();
}

void SingleFileBlockManager::TrimFreeBlocks() {
	if (DBConfig::Get(db).options.trim_free_blocks) {
		for (auto itr = newly_freed_list.begin(); itr != newly_freed_list.end(); ++itr) {
			block_id_t first = *itr;
			block_id_t last = first;
			// Find end of contiguous range.
			for (++itr; itr != newly_freed_list.end() && (*itr == last + 1); ++itr) {
				last = *itr;
			}
			// We are now one too far.
			--itr;
			// Trim the range.
			handle->Trim(BLOCK_START + (NumericCast<idx_t>(first) * GetBlockAllocSize()),
			             NumericCast<idx_t>(last + 1 - first) * GetBlockAllocSize());
		}
	}
	newly_freed_list.clear();
}

} // namespace duckdb
