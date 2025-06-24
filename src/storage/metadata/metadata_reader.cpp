#include "duckdb/storage/metadata/metadata_reader.hpp"

namespace duckdb {

MetadataReader::MetadataReader(MetadataManager &manager, MetaBlockPointer pointer,
                               optional_ptr<vector<MetaBlockPointer>> read_pointers_p, BlockReaderType type)
    : manager(manager), type(type), next_pointer(FromDiskPointer(pointer)), has_next_block(true),
      read_pointers(read_pointers_p), index(0), offset(0), next_offset(pointer.offset), capacity(0) {
	if (read_pointers) {
		D_ASSERT(read_pointers->empty());
		read_pointers->push_back(pointer);
	}
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
	while (offset + read_size > capacity) {
		// cannot read entire entry from block
		// first read what we can from this block
		idx_t to_read = capacity - offset;
		if (to_read > 0) {
			memcpy(buffer, Ptr(), to_read);
			read_size -= to_read;
			buffer += to_read;
			offset += read_size;
		}
		// then move to the next block
		ReadNextBlock();
	}
	// we have enough left in this block to read from the buffer
	memcpy(buffer, Ptr(), read_size);
	offset += read_size;
}

MetaBlockPointer MetadataReader::GetMetaBlockPointer() {
	return manager.GetDiskPointer(block.pointer, UnsafeNumericCast<uint32_t>(offset));
}

void MetadataReader::ReadNextBlock() {
	if (!has_next_block) {
		throw IOException("No more data remaining in MetadataReader");
	}
	block = manager.Pin(next_pointer);
	index = next_pointer.index;

	idx_t next_block = Load<idx_t>(BasePtr());
	if (next_block == idx_t(-1)) {
		has_next_block = false;
	} else {
		next_pointer = FromDiskPointer(MetaBlockPointer(next_block, 0));
		MetaBlockPointer next_block_pointer(next_block, 0);
		if (read_pointers) {
			read_pointers->push_back(next_block_pointer);
		}
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

data_ptr_t MetadataReader::BasePtr() {
	return block.handle.Ptr() + index * GetMetadataManager().GetMetadataBlockSize();
}

data_ptr_t MetadataReader::Ptr() {
	return BasePtr() + offset;
}

} // namespace duckdb
