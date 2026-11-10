#include "duckdb/main/database.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"

#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

MetadataReader::MetadataReader(MetadataManager &manager, MetaBlockPointer pointer,
                               optional_ptr<vector<MetaBlockPointer>> read_pointers_p, BlockReaderType type)
    : manager(manager), type(type), next_pointer(pointer), has_next_block(true), read_pointers(read_pointers_p),
      index(0), offset(0), next_offset(pointer.offset), capacity(0), cycle_check_pointer(DConstants::INVALID_INDEX),
      cycle_check_at(1), blocks_read(0) {
}

MetadataReader::MetadataReader(MetadataManager &manager, BlockPointer pointer)
    : MetadataReader(manager, MetadataManager::FromBlockPointer(pointer, manager.GetMetadataBlockSize())) {
}

MetadataPointer MetadataReader::FromDiskPointer(MetaBlockPointer pointer) {
	if (type == BlockReaderType::EXISTING_BLOCKS) {
		return manager.FromDiskPointer(pointer);
	} else {
		return manager.RegisterDiskPointer(pointer);
	}
}

MetadataReader::~MetadataReader() {
}

void MetadataReader::ReadData(data_ptr_t buffer, idx_t read_size) {
	ReadData(manager.GetDatabase().GetContext(), buffer, read_size);
}

void MetadataReader::ReadData(Context &context, data_ptr_t buffer, idx_t read_size) {
	while (offset + read_size > capacity) {
		// cannot read entire entry from block
		// first read what we can from this block
		idx_t to_read = capacity - offset;
		if (to_read > 0) {
			memcpy(buffer, Ptr(), to_read);
			read_size -= to_read;
			buffer += to_read;
			offset += to_read;
		}
		// then move to the next block
		ReadNextBlock(context);
	}
	// we have enough left in this block to read from the buffer
	memcpy(buffer, Ptr(), read_size);
	offset += read_size;
}

MetaBlockPointer MetadataReader::GetMetaBlockPointer() {
	if (capacity == 0) {
		throw InternalException("GetMetaBlockPointer called but there is no active pointer");
	}
	return manager.GetDiskPointer(block.pointer, UnsafeNumericCast<uint32_t>(offset));
}

void MetadataReader::ReadNextBlock() {
	ReadNextBlock(manager.GetDatabase().GetContext());
}

void MetadataReader::ReadNextBlock(Context &context) {
	if (!has_next_block) {
		throw IOException("No more data remaining in MetadataReader");
	}
	CheckForCycle();
	if (read_pointers) {
		read_pointers->push_back(next_pointer);
	}
	auto next_disk_pointer = FromDiskPointer(next_pointer);
	block = manager.Pin(context, next_disk_pointer);
	index = next_disk_pointer.index;

	idx_t next_block = Load<idx_t>(BasePtr());
	if (next_block == idx_t(-1)) {
		has_next_block = false;
	} else {
		next_pointer = MetaBlockPointer(next_block, 0);
	}
	if (next_offset < sizeof(block_id_t)) {
		next_offset = sizeof(block_id_t);
	}
	if (next_offset > GetMetadataManager().GetMetadataBlockSize()) {
		throw InternalException("next_offset cannot be bigger than block size");
	}
	offset = next_offset;
	next_offset = sizeof(block_id_t);
	capacity = GetMetadataManager().GetMetadataBlockSize();
}

void MetadataReader::CheckForCycle() {
	// the pointers to the next block are read from the file - a chain that points back into itself never ends
	// we use Brent's algorithm to detect this: remember the block read at every power of two and compare against it
	if (next_pointer.block_pointer == cycle_check_pointer) {
		throw DataCorruptionException(
		    "Corrupt database file: metadata block chain contains a cycle (block id %lld, index %llu)",
		    next_pointer.GetBlockId(), next_pointer.GetBlockIndex());
	}
	blocks_read++;
	if (blocks_read == cycle_check_at) {
		cycle_check_pointer = next_pointer.block_pointer;
		cycle_check_at *= 2;
	}
}

data_ptr_t MetadataReader::BasePtr() {
	return block.handle.GetDataMutable() + index * GetMetadataManager().GetMetadataBlockSize();
}

data_ptr_t MetadataReader::Ptr() {
	return BasePtr() + offset;
}

} // namespace duckdb
