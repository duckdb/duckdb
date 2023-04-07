#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

SwizzleablePointer::SwizzleablePointer(MetaBlockReader &reader) {

	idx_t block_id = reader.Read<block_id_t>();
	offset = reader.Read<uint32_t>();
	type = 0;

	if (block_id == DConstants::INVALID_INDEX) {
		swizzle_flag = 0;
		return;
	}

	buffer_id = (uint32_t)block_id;
	swizzle_flag = 1;
}

} // namespace duckdb
