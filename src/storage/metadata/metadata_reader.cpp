#include "duckdb/storage/metadata/metadata_reader.hpp"

namespace duckdb {

MetadataReader::MetadataReader(MetadataManager &manager, MetadataPointer pointer) :
	manager(manager), next_pointer(pointer), has_next_block(true), offset(0), capacity(0) {
}

MetadataReader::MetadataReader(MetadataManager &manager, MetaBlockPointer pointer) :
	manager(manager), next_pointer(manager.FromDiskPointer(pointer)), has_next_block(true), offset(0), capacity(0) {
}

void MetadataReader::ReadData(data_ptr_t buffer, idx_t read_size) {
	while (offset + read_size > capacity) {
		// cannot read entire entry from block
		// first read what we can from this block
		idx_t to_read = capacity - offset;
		if (to_read > 0) {
			memcpy(buffer, Ptr() + offset, to_read);
			read_size -= to_read;
			buffer += to_read;
		}
		// then move to the next block
		ReadNextBlock();
	}
	// we have enough left in this block to read from the buffer
	memcpy(buffer, Ptr() + offset, read_size);
	offset += read_size;
}

MetaBlockPointer MetadataReader::GetBlockPointer() {
	throw InternalException("FIXME: GetBlockPointer");
}

void MetadataReader::ReadNextBlock() {
	if (!has_next_block) {
		throw IOException("No more data remaining in MetadataReader");
	}
	block = manager.Pin(next_pointer);

	idx_t next_block = Load<idx_t>(Ptr());
	if (next_block == idx_t(-1)) {
		has_next_block = false;
	} else {
		next_pointer = manager.FromDiskPointer(MetaBlockPointer(next_block, 0));
	}
	offset = sizeof(block_id_t);
	capacity = MetadataManager::METADATA_BLOCK_SIZE;
}

data_ptr_t MetadataReader::Ptr() {
	return block.handle.Ptr();
}

}
