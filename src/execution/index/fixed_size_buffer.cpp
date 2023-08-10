#include "duckdb/execution/index/fixed_size_buffer.hpp"

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"

namespace duckdb {

data_ptr_t FixedSizeBuffer::GetPtr(FixedSizeAllocator &fixed_size_allocator) {

	if (in_memory) {
		return memory_ptr;
	}
	D_ASSERT(on_disk);

	dirty = false;
	in_memory = true;

	memory_ptr = fixed_size_allocator.allocator.AllocateData(fixed_size_allocator.BUFFER_SIZE);
	MetadataReader reader(fixed_size_allocator.metadata_manager, block_ptr);
	reader.ReadData(memory_ptr, fixed_size_allocator.BUFFER_SIZE);

	return memory_ptr;
}

void FixedSizeBuffer::Serialize(FixedSizeAllocator &fixed_size_allocator, MetadataWriter &writer) {

	if (!in_memory) {
		D_ASSERT(on_disk && !dirty);
		return;
	}
	if (!dirty && on_disk) {
		return;
	}
	D_ASSERT(in_memory);

	block_ptr = writer.GetBlockPointer();
	writer.WriteData(memory_ptr, fixed_size_allocator.BUFFER_SIZE);
	dirty = false;
	on_disk = true;
}

} // namespace duckdb
