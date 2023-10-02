#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

BufferPoolReservation::BufferPoolReservation(BufferPool &pool) : pool(pool) {
}

BufferPoolReservation::BufferPoolReservation(BufferPoolReservation &&src) noexcept : pool(src.pool) {
	size = src.size;
	src.size = 0;
}

BufferPoolReservation &BufferPoolReservation::operator=(BufferPoolReservation &&src) noexcept {
	size = src.size;
	src.size = 0;
	return *this;
}

BufferPoolReservation::~BufferPoolReservation() {
	D_ASSERT(size == 0);
}

void BufferPoolReservation::Resize(idx_t new_size) {
	int64_t delta = (int64_t)new_size - size;
	pool.IncreaseUsedMemory(delta);
	size = new_size;
}

void BufferPoolReservation::Merge(BufferPoolReservation &&src) {
	size += src.size;
	src.size = 0;
}

} // namespace duckdb
