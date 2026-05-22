#include "duckdb/storage/external_file_cache/external_file_cache_block.hpp"

#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

void CacheBlock::Reinit() {
	shared_ptr<BlockHandle> old_block_handle;
	std::function<void()> old_cleanup;
	{
		const annotated_lock_guard<annotated_mutex> guard(mtx);
		state = CacheBlockState::EMPTY;
		old_block_handle = std::move(block_handle);
		old_cleanup = std::move(cleanup);
		nr_bytes = 0;
#ifdef DEBUG
		checksum = 0;
#endif
	}
	if (old_cleanup) {
		old_cleanup();
	}
	old_block_handle.reset();
}

} // namespace duckdb
