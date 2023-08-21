#include "duckdb/execution/index/fixed_size_buffer.hpp"

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"

namespace duckdb {

void FixedSizeBuffer::Serialize(FixedSizeAllocator &fixed_size_allocator, MetadataWriter &writer) {

	if (!in_memory) {
		if (!on_disk || dirty) {
			throw InternalException("invalid/missing buffer in FixedSizeAllocator");
		}
		return;
	}
	if (!dirty && on_disk) {
		return;
	}

	// the buffer is in memory
	D_ASSERT(in_memory);
	// the buffer never was on disk, or there were changes to it after loading it from disk
	D_ASSERT(!on_disk || dirty);

	// FIXME: we should not use the metadata writer here, as it only writes blocks of METADATA_BLOCK_SIZE
	block_ptr = writer.GetBlockPointer();
	writer.WriteData(memory_ptr, fixed_size_allocator.BUFFER_SIZE);
	on_disk = true;

	// FIXME: we want to reset the dirty-flag here. However, this is a temporary fix, which we should remove when
	// FIXME: fixing the issue explained in test_art_storage_multi_checkpoint.test
	// dirty = false;
}

void FixedSizeBuffer::Deserialize(FixedSizeAllocator &fixed_size_allocator) {

	D_ASSERT(on_disk && !dirty && !in_memory);
	D_ASSERT(block_ptr.IsValid());
	in_memory = true;

	// FIXME: we should not use the metadata reader here, as it only reads blocks of METADATA_BLOCK_SIZE
	memory_ptr = fixed_size_allocator.allocator.AllocateData(fixed_size_allocator.BUFFER_SIZE);
	MetadataReader reader(fixed_size_allocator.metadata_manager, block_ptr);
	reader.ReadData(memory_ptr, fixed_size_allocator.BUFFER_SIZE);
}

} // namespace duckdb
