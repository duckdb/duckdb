#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace duckdb {

MetadataWriter::MetadataWriter(MetadataManager &manager) :
	manager(manager), capacity(0), offset(0) {

}

void MetadataWriter::NextBlock() {
	// FIXME: next block
	if (capacity > 0) {
		written_blocks.push_back(current_pointer);
	}
//	written_blocks.insert(block->id);
//	if (offset > sizeof(block_id_t)) {
//		block_manager.Write(*block);
//		offset = sizeof(block_id_t);
//	}
}

void MetadataWriter::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	while (offset + write_size > capacity) {
		// we need to make a new block
		// first copy what we can
		D_ASSERT(offset <= capacity);
		idx_t copy_amount = capacity - offset;
		if (copy_amount > 0) {
			memcpy(block.handle.Ptr() + offset, buffer, copy_amount);
			buffer += copy_amount;
			offset += copy_amount;
			write_size -= copy_amount;
		}
		// now we need to get a new block id
		auto new_handle = manager.AllocateHandle();

		// write the block id of the new block to the start of the current block
		if (capacity > 0) {
			Store<idx_t>(manager.GetDiskPointer(new_handle.pointer), Ptr());
		}
		// first flush the old block
		NextBlock();
		// now update the block id of the block
		block = std::move(new_handle);
		current_pointer = new_handle.pointer;
		capacity = MetadataManager::METADATA_BLOCK_SIZE;
		Store<block_id_t>(-1, Ptr());
	}
	memcpy(Ptr() + offset, buffer, write_size);
	offset += write_size;
}

data_ptr_t MetadataWriter::Ptr() {
	return block.handle.Ptr();
}

}
