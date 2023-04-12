#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

BufferPoolReservation::BufferPoolReservation(BufferPoolReservation &&src) noexcept {
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

void BufferPoolReservation::Resize(atomic<idx_t> &counter, idx_t new_size) {
	int64_t delta = (int64_t)new_size - size;
	D_ASSERT(delta > 0 || (int64_t)counter >= -delta);
	counter += delta;
	size = new_size;
}

void BufferPoolReservation::Merge(BufferPoolReservation &&src) {
	size += src.size;
	src.size = 0;
}

} // namespace duckdb
