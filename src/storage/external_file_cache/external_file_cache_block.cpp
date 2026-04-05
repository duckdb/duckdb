#include "duckdb/storage/external_file_cache/external_file_cache_block.hpp"

namespace duckdb {

void CacheBlock::Reinit() {
	const annotated_lock_guard<annotated_mutex> guard(mtx);
	state = CacheBlockState::EMPTY;
	block_handle = nullptr;
	nr_bytes = 0;
#ifdef DEBUG
	checksum = 0;
#endif
}

} // namespace duckdb
