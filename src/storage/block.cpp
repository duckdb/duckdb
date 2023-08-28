#include "duckdb/storage/block.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

Block::Block(Allocator &allocator, block_id_t id)
    : FileBuffer(allocator, FileBufferType::BLOCK, Storage::BLOCK_SIZE), id(id) {
}

Block::Block(Allocator &allocator, block_id_t id, uint32_t internal_size)
    : FileBuffer(allocator, FileBufferType::BLOCK, internal_size), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

Block::Block(FileBuffer &source, block_id_t id) : FileBuffer(source, FileBufferType::BLOCK), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

void MetaBlockPointer::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty(100, "block_pointer", block_pointer);
	serializer.WriteProperty(101, "offset", offset);
}

MetaBlockPointer MetaBlockPointer::FormatDeserialize(FormatDeserializer &source) {
	auto block_pointer = source.ReadProperty<idx_t>(100, "block_pointer");
	auto offset = source.ReadProperty<uint32_t>(101, "offset");
	return MetaBlockPointer(block_pointer, offset);
}

} // namespace duckdb
