#include "duckdb/storage/single_file_block_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/main/config.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

const char MainHeader::MAGIC_BYTES[] = "DUCK";

void MainHeader::Serialize(Serializer &ser) {
	ser.WriteData((data_ptr_t)MAGIC_BYTES, MAGIC_BYTE_SIZE);
	ser.Write<uint64_t>(version_number);
	FieldWriter writer(ser);
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		writer.WriteField<uint64_t>(flags[i]);
	}
	writer.Finalize();
}

void MainHeader::CheckMagicBytes(FileHandle &handle) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	if (handle.GetFileSize() < MainHeader::MAGIC_BYTE_SIZE + MainHeader::MAGIC_BYTE_OFFSET) {
		throw IOException("The file is not a valid DuckDB database file!");
	}
	handle.Read(magic_bytes, MainHeader::MAGIC_BYTE_SIZE, MainHeader::MAGIC_BYTE_OFFSET);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file is not a valid DuckDB database file!");
	}
}

MainHeader MainHeader::Deserialize(Deserializer &source) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	MainHeader header;
	source.ReadData(magic_bytes, MainHeader::MAGIC_BYTE_SIZE);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file is not a valid DuckDB database file!");
	}
	header.version_number = source.Read<uint64_t>();
	// read the flags
	FieldReader reader(source);
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		header.flags[i] = reader.ReadRequired<uint64_t>();
	}
	reader.Finalize();
	return header;
}

void DatabaseHeader::Serialize(Serializer &ser) {
	ser.Write<uint64_t>(iteration);
	ser.Write<block_id_t>(meta_block);
	ser.Write<block_id_t>(free_list);
	ser.Write<uint64_t>(block_count);
}

DatabaseHeader DatabaseHeader::Deserialize(Deserializer &source) {
	DatabaseHeader header;
	header.iteration = source.Read<uint64_t>();
	header.meta_block = source.Read<block_id_t>();
	header.free_list = source.Read<block_id_t>();
	header.block_count = source.Read<uint64_t>();
	return header;
}

template <class T>
void SerializeHeaderStructure(T header, data_ptr_t ptr) {
	BufferedSerializer ser(ptr, Storage::FILE_HEADER_SIZE);
	header.Serialize(ser);
}

template <class T>
T DeserializeHeaderStructure(data_ptr_t ptr) {
	BufferedDeserializer source(ptr, Storage::FILE_HEADER_SIZE);
	return T::Deserialize(source);
}

SingleFileBlockManager::SingleFileBlockManager(DatabaseInstance &db, string path_p, bool read_only, bool create_new,
                                               bool use_direct_io)
    : db(db), path(move(path_p)),
      header_buffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER, Storage::FILE_HEADER_SIZE), iteration_count(0),
      read_only(read_only), use_direct_io(use_direct_io) {
	uint8_t flags;
	FileLockType lock;
	if (read_only) {
		D_ASSERT(!create_new);
		flags = FileFlags::FILE_FLAGS_READ;
		lock = FileLockType::READ_LOCK;
	} else {
		flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ;
		lock = FileLockType::WRITE_LOCK;
		if (create_new) {
			flags |= FileFlags::FILE_FLAGS_FILE_CREATE;
		}
	}
	if (use_direct_io) {
		flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
	}
	// open the RDBMS handle
	auto &fs = FileSystem::GetFileSystem(db);
	handle = fs.OpenFile(path, flags, lock);
	if (create_new) {
		// if we create a new file, we fill the metadata of the file
		// first fill in the new header
		header_buffer.Clear();

		MainHeader main_header;
		main_header.version_number = VERSION_NUMBER;
		memset(main_header.flags, 0, sizeof(uint64_t) * 4);

		SerializeHeaderStructure<MainHeader>(main_header, header_buffer.buffer);
		// now write the header to the file
		header_buffer.ChecksumAndWrite(*handle, 0);
		header_buffer.Clear();

		// write the database headers
		// initialize meta_block and free_list to INVALID_BLOCK because the database file does not contain any actual
		// content yet
		DatabaseHeader h1, h2;
		// header 1
		h1.iteration = 0;
		h1.meta_block = INVALID_BLOCK;
		h1.free_list = INVALID_BLOCK;
		h1.block_count = 0;
		SerializeHeaderStructure<DatabaseHeader>(h1, header_buffer.buffer);
		header_buffer.ChecksumAndWrite(*handle, Storage::FILE_HEADER_SIZE);
		// header 2
		h2.iteration = 0;
		h2.meta_block = INVALID_BLOCK;
		h2.free_list = INVALID_BLOCK;
		h2.block_count = 0;
		SerializeHeaderStructure<DatabaseHeader>(h2, header_buffer.buffer);
		header_buffer.ChecksumAndWrite(*handle, Storage::FILE_HEADER_SIZE * 2);
		// ensure that writing to disk is completed before returning
		handle->Sync();
		// we start with h2 as active_header, this way our initial write will be in h1
		iteration_count = 0;
		active_header = 1;
		max_block = 0;
	} else {
		MainHeader::CheckMagicBytes(*handle);
		// otherwise, we check the metadata of the file
		header_buffer.ReadAndChecksum(*handle, 0);
		MainHeader header = DeserializeHeaderStructure<MainHeader>(header_buffer.buffer);
		// check the version number
		if (header.version_number != VERSION_NUMBER) {
			throw IOException(
			    "Trying to read a database file with version number %lld, but we can only read version %lld.\n"
			    "The database file was created with an %s version of DuckDB.\n\n"
			    "The storage of DuckDB is not yet stable; newer versions of DuckDB cannot read old database files and "
			    "vice versa.\n"
			    "The storage will be stabilized when version 1.0 releases.\n\n"
			    "For now, we recommend that you load the database file in a supported version of DuckDB, and use the "
			    "EXPORT DATABASE command "
			    "followed by IMPORT DATABASE on the current version of DuckDB.",
			    header.version_number, VERSION_NUMBER, VERSION_NUMBER > header.version_number ? "older" : "newer");
		}

		// read the database headers from disk
		DatabaseHeader h1, h2;
		header_buffer.ReadAndChecksum(*handle, Storage::FILE_HEADER_SIZE);
		h1 = DeserializeHeaderStructure<DatabaseHeader>(header_buffer.buffer);
		header_buffer.ReadAndChecksum(*handle, Storage::FILE_HEADER_SIZE * 2);
		h2 = DeserializeHeaderStructure<DatabaseHeader>(header_buffer.buffer);
		// check the header with the highest iteration count
		if (h1.iteration > h2.iteration) {
			// h1 is active header
			active_header = 0;
			Initialize(h1);
		} else {
			// h2 is active header
			active_header = 1;
			Initialize(h2);
		}
	}
}

void SingleFileBlockManager::Initialize(DatabaseHeader &header) {
	free_list_id = header.free_list;
	meta_block = header.meta_block;
	iteration_count = header.iteration;
	max_block = header.block_count;
}

void SingleFileBlockManager::LoadFreeList() {
	if (read_only) {
		// no need to load free list for read only db
		return;
	}
	if (free_list_id == INVALID_BLOCK) {
		// no free list
		return;
	}
	MetaBlockReader reader(db, free_list_id);
	auto free_list_count = reader.Read<uint64_t>();
	free_list.clear();
	for (idx_t i = 0; i < free_list_count; i++) {
		free_list.insert(reader.Read<block_id_t>());
	}
	auto multi_use_blocks_count = reader.Read<uint64_t>();
	multi_use_blocks.clear();
	for (idx_t i = 0; i < multi_use_blocks_count; i++) {
		auto block_id = reader.Read<block_id_t>();
		auto usage_count = reader.Read<uint32_t>();
		multi_use_blocks[block_id] = usage_count;
	}
}

void SingleFileBlockManager::StartCheckpoint() {
}

bool SingleFileBlockManager::IsRootBlock(block_id_t root) {
	return root == meta_block;
}

block_id_t SingleFileBlockManager::GetFreeBlockId() {
	block_id_t block;
	if (!free_list.empty()) {
		// free list is non empty
		// take an entry from the free list
		block = *free_list.begin();
		// erase the entry from the free list again
		free_list.erase(free_list.begin());
	} else {
		block = max_block++;
	}
	return block;
}

void SingleFileBlockManager::MarkBlockAsModified(block_id_t block_id) {
	D_ASSERT(block_id >= 0);

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
	modified_blocks.insert(block_id);
}

void SingleFileBlockManager::IncreaseBlockReferenceCount(block_id_t block_id) {
	D_ASSERT(free_list.find(block_id) == free_list.end());
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		entry->second++;
	} else {
		multi_use_blocks[block_id] = 2;
	}
}

block_id_t SingleFileBlockManager::GetMetaBlock() {
	return meta_block;
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock(block_id_t block_id) {
	return make_unique<Block>(Allocator::Get(db), block_id);
}

void SingleFileBlockManager::Read(Block &block) {
	D_ASSERT(block.id >= 0);
	D_ASSERT(std::find(free_list.begin(), free_list.end(), block.id) == free_list.end());
	block.ReadAndChecksum(*handle, BLOCK_START + block.id * Storage::BLOCK_ALLOC_SIZE);
}

void SingleFileBlockManager::Write(FileBuffer &buffer, block_id_t block_id) {
	D_ASSERT(block_id >= 0);
	buffer.ChecksumAndWrite(*handle, BLOCK_START + block_id * Storage::BLOCK_ALLOC_SIZE);
}

vector<block_id_t> SingleFileBlockManager::GetFreeListBlocks() {
	vector<block_id_t> free_list_blocks;

	if (!free_list.empty() || !multi_use_blocks.empty() || !modified_blocks.empty()) {
		// there are blocks in the free list or multi_use_blocks
		// figure out how many blocks we need to write these to the file
		auto free_list_size = sizeof(uint64_t) + sizeof(block_id_t) * (free_list.size() + modified_blocks.size());
		auto multi_use_blocks_size =
		    sizeof(uint64_t) + (sizeof(block_id_t) + sizeof(uint32_t)) * multi_use_blocks.size();
		auto total_size = free_list_size + multi_use_blocks_size;
		// because of potential alignment issues and needing to store a next pointer in a block we subtract
		// a bit from the max block size
		auto space_in_block = Storage::BLOCK_SIZE - 4 * sizeof(block_id_t);
		auto total_blocks = (total_size + space_in_block - 1) / space_in_block;
		auto &config = DBConfig::GetConfig(db);
		if (config.options.debug_many_free_list_blocks) {
			total_blocks++;
		}
		D_ASSERT(total_size > 0);
		D_ASSERT(total_blocks > 0);

		// reserve the blocks that we are going to write
		// since these blocks are no longer free we cannot just include them in the free list!
		for (idx_t i = 0; i < total_blocks; i++) {
			auto block_id = GetFreeBlockId();
			free_list_blocks.push_back(block_id);
		}
	}

	return free_list_blocks;
}

class FreeListBlockWriter : public MetaBlockWriter {
public:
	FreeListBlockWriter(DatabaseInstance &db_p, vector<block_id_t> &free_list_blocks_p)
	    : MetaBlockWriter(db_p, free_list_blocks_p[0]), free_list_blocks(free_list_blocks_p), index(1) {
	}

	vector<block_id_t> &free_list_blocks;
	idx_t index;

protected:
	block_id_t GetNextBlockId() override {
		if (index >= free_list_blocks.size()) {
			throw InternalException(
			    "Free List Block Writer ran out of blocks, this means not enough blocks were allocated up front");
		}
		return free_list_blocks[index++];
	}
};

void SingleFileBlockManager::WriteHeader(DatabaseHeader header) {
	// set the iteration count
	header.iteration = ++iteration_count;

	vector<block_id_t> free_list_blocks = GetFreeListBlocks();

	// now handle the free list
	// add all modified blocks to the free list: they can now be written to again
	for (auto &block : modified_blocks) {
		free_list.insert(block);
	}
	modified_blocks.clear();

	if (!free_list_blocks.empty()) {
		// there are blocks to write, either in the free_list or in the modified_blocks
		// we write these blocks specifically to the free_list_blocks
		// a normal MetaBlockWriter will fetch blocks to use from the free_list
		// but since we are WRITING the free_list, this behavior is sub-optimal

		FreeListBlockWriter writer(db, free_list_blocks);

		D_ASSERT(writer.block->id == free_list_blocks[0]);
		header.free_list = writer.block->id;
		for (auto &block_id : free_list_blocks) {
			modified_blocks.insert(block_id);
		}

		writer.Write<uint64_t>(free_list.size());
		for (auto &block_id : free_list) {
			writer.Write<block_id_t>(block_id);
		}
		writer.Write<uint64_t>(multi_use_blocks.size());
		for (auto &entry : multi_use_blocks) {
			writer.Write<block_id_t>(entry.first);
			writer.Write<uint32_t>(entry.second);
		}
		writer.Flush();
	} else {
		// no blocks in the free list
		header.free_list = INVALID_BLOCK;
	}
	header.block_count = max_block;

	auto &config = DBConfig::GetConfig(db);
	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE) {
		throw IOException("Checkpoint aborted after free list write because of PRAGMA checkpoint_abort flag");
	}

	if (!use_direct_io) {
		// if we are not using Direct IO we need to fsync BEFORE we write the header to ensure that all the previous
		// blocks are written as well
		handle->Sync();
	}
	// set the header inside the buffer
	header_buffer.Clear();
	Store<DatabaseHeader>(header, header_buffer.buffer);
	// now write the header to the file, active_header determines whether we write to h1 or h2
	// note that if active_header is h1 we write to h2, and vice versa
	header_buffer.ChecksumAndWrite(*handle,
	                               active_header == 1 ? Storage::FILE_HEADER_SIZE : Storage::FILE_HEADER_SIZE * 2);
	// switch active header to the other header
	active_header = 1 - active_header;
	//! Ensure the header write ends up on disk
	handle->Sync();
}

} // namespace duckdb
