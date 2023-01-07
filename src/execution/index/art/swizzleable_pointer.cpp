#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

namespace duckdb {
SwizzleablePointer::~SwizzleablePointer() {
	if (pointer) {
		if (!IsSwizzled()) {
			Node::Delete((Node *)pointer);
		}
	}
}

SwizzleablePointer::SwizzleablePointer(duckdb::MetaBlockReader &reader) {
	idx_t block_id = reader.Read<block_id_t>();
	idx_t offset = reader.Read<uint32_t>();
	if (block_id == DConstants::INVALID_INDEX || offset == DConstants::INVALID_INDEX) {
		pointer = 0;
		return;
	}
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = block_id;
	// This assumes high 32 bits of pointer are zero.
	pointer = pointer << (pointer_size / 2);
	D_ASSERT((pointer >> (pointer_size / 2)) == block_id);
	pointer += offset;
	// Set the left most bit to indicate this is a swizzled pointer and send it back to the mother-ship
	uint64_t mask = 1;
	mask = mask << (pointer_size - 1);
	// This assumes the 33rd most significant bit of the block_id is zero.
	pointer |= mask;
}

SwizzleablePointer &SwizzleablePointer::operator=(const Node *ptr) {
	// If the object already has a non-swizzled pointer, this will leak memory.
	//
	// TODO: If enabled, this assert will fire, indicating a possible leak. If an exception
	// is thrown here, it will cause a double-free. There is some work to do to make all this safer.
	// D_ASSERT(empty() || IsSwizzled());
	if (sizeof(ptr) == 4) {
		pointer = (uint32_t)(size_t)ptr;
	} else {
		pointer = (uint64_t)ptr;
	}
	return *this;
}

bool operator!=(const SwizzleablePointer &s_ptr, const uint64_t &ptr) {
	return (s_ptr.pointer != ptr);
}

BlockPointer SwizzleablePointer::GetSwizzledBlockInfo() {
	D_ASSERT(IsSwizzled());
	idx_t pointer_size = sizeof(pointer) * 8;
	// This is destructive. Pointer will be invalid after this operation.
	// That's okay because this is only ever called from Unswizzle.
	pointer = pointer & ~(1ULL << (pointer_size - 1));
	uint32_t block_id = pointer >> (pointer_size / 2);
	uint32_t offset = pointer & 0xffffffff;
	return {block_id, offset};
}

bool SwizzleablePointer::IsSwizzled() {
	idx_t pointer_size = sizeof(pointer) * 8;
	return (pointer >> (pointer_size - 1)) & 1;
}

void SwizzleablePointer::Reset() {
	if (pointer) {
		if (!IsSwizzled()) {
			Node::Delete((Node *)pointer);
		}
	}
	*this = nullptr;
}

Node *SwizzleablePointer::Unswizzle(ART &art) {
	if (IsSwizzled()) {
		// This means our pointer is not yet in memory, gotta deserialize this
		// first we unset the bae
		auto block_info = GetSwizzledBlockInfo();
		*this = Node::Deserialize(art, block_info.block_id, block_info.offset);
	}
	return (Node *)pointer;
}

BlockPointer SwizzleablePointer::Serialize(ART &art, duckdb::MetaBlockWriter &writer) {
	if (pointer) {
		Unswizzle(art);
		return ((Node *)pointer)->Serialize(art, writer);
	} else {
		return {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)DConstants::INVALID_INDEX};
	}
}

} // namespace duckdb
