#include "duckdb/execution/index/fixed_size_buffer.hpp"

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"

namespace duckdb {

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

data_ptr_t FixedSizeBuffer::Deserialize(FixedSizeAllocator &fixed_size_allocator) {

	D_ASSERT(on_disk);
	D_ASSERT(block_ptr.IsValid());

	dirty = false;
	in_memory = true;

	memory_ptr = fixed_size_allocator.allocator.AllocateData(fixed_size_allocator.BUFFER_SIZE);
	MetadataReader reader(fixed_size_allocator.metadata_manager, block_ptr);
	reader.ReadData(memory_ptr, fixed_size_allocator.BUFFER_SIZE);

	return memory_ptr;
}

} // namespace duckdb
