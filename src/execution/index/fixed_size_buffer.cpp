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

	// the buffer is in memory,
	D_ASSERT(in_memory);
	D_ASSERT(!on_disk || dirty);

	// FIXME: we should not use the metadata writer here, as it only writes blocks of METADATA_BLOCK_SIZE
	block_ptr = writer.GetBlockPointer();
	writer.WriteData(memory_ptr, fixed_size_allocator.BUFFER_SIZE);
	on_disk = true;

	// TODO: do we potentially want to set dirty to false here? But breaks foreign_key_persistent.test,
	// TODO: so I guess not...
}

void FixedSizeBuffer::Deserialize(FixedSizeAllocator &fixed_size_allocator) {

	D_ASSERT(on_disk);
	D_ASSERT(block_ptr.IsValid());
	in_memory = true;

	// FIXME: we should not use the metadata reader here, as it only reads blocks of METADATA_BLOCK_SIZE
	memory_ptr = fixed_size_allocator.allocator.AllocateData(fixed_size_allocator.BUFFER_SIZE);
	MetadataReader reader(fixed_size_allocator.metadata_manager, block_ptr);
	reader.ReadData(memory_ptr, fixed_size_allocator.BUFFER_SIZE);
}

} // namespace duckdb
