#include "duckdb/storage/external_file_cache/external_file_cache_block.hpp"

#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

void CacheBlock::Reinit() {
	shared_ptr<BlockHandle> old_block_handle;
	{
		const annotated_lock_guard<annotated_mutex> guard(mtx);
		state = CacheBlockState::EMPTY;
		old_block_handle = std::move(block_handle);
		nr_bytes = 0;
#ifdef DEBUG
		checksum = 0;
#endif
	}
}

} // namespace duckdb
