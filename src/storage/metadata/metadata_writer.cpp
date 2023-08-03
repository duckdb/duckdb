#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace duckdb {

MetadataWriter::MetadataWriter(MetadataManager &manager) :
	manager(manager), capacity(0), offset(0) {

}

MetaBlockPointer MetadataWriter::GetBlockPointer() {
	throw InternalException("MetaBlockPointer - GetBlockPointer");
}

MetadataHandle MetadataWriter::NextHandle() {
	return manager.AllocateHandle();
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
		auto new_handle = NextHandle();

		// write the block id of the new block to the start of the current block
		if (capacity > 0) {
			Store<idx_t>(manager.GetDiskPointer(new_handle.pointer), Ptr());
		}
		// now update the block id of the block
		block = std::move(new_handle);
		current_pointer = block.pointer;
		offset = sizeof(idx_t);
		capacity = MetadataManager::METADATA_BLOCK_SIZE;
		Store<idx_t>(-1, Ptr());
	}
	memcpy(Ptr() + offset, buffer, write_size);
	offset += write_size;
}

data_ptr_t MetadataWriter::Ptr() {
	return block.handle.Ptr();
}

}
