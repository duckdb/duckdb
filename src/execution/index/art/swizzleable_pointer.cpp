#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

namespace duckdb {

SwizzleablePointer::SwizzleablePointer(MetaBlockReader &reader) {

	idx_t block_id = reader.Read<block_id_t>();
	uint32_t offset = reader.Read<uint32_t>();

	if (block_id == DConstants::INVALID_INDEX || offset == (uint32_t)DConstants::INVALID_INDEX) {
		pointer = 0;
		return;
	}

	// set the block id
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = block_id;

	// set the offset, which assumes that the high 32 bits of pointer are zero
	pointer = pointer << (pointer_size / 2);
	D_ASSERT((pointer >> (pointer_size / 2)) == block_id);
	pointer += offset;

	// set the leftmost bit to indicate this is a swizzled pointer
	idx_t mask = 1;
	mask = mask << (pointer_size - 1);
	pointer |= mask;
}

BlockPointer SwizzleablePointer::GetBlockInfo() {

	D_ASSERT(IsSwizzled());
	idx_t pointer_size = sizeof(pointer) * 8;

	auto temp_ptr = pointer & ~(1ULL << (pointer_size - 1));
	uint32_t block_id = temp_ptr >> (pointer_size / 2);
	uint32_t offset = temp_ptr & 0xffffffff;
	return {block_id, offset};
}

} // namespace duckdb
