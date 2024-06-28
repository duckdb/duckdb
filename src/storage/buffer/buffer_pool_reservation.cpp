#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

BufferPoolReservation::BufferPoolReservation(MemoryTag tag, BufferPool &pool) : tag(tag), pool(pool) {
}

BufferPoolReservation::BufferPoolReservation(BufferPoolReservation &&src) noexcept : tag(src.tag), pool(src.pool) {
	size = src.size;
	src.size = 0;
}

BufferPoolReservation &BufferPoolReservation::operator=(BufferPoolReservation &&src) noexcept {
	tag = src.tag;
	size = src.size;
	src.size = 0;
	return *this;
}

BufferPoolReservation::~BufferPoolReservation() {
	D_ASSERT(size == 0);
}

void BufferPoolReservation::Resize(idx_t new_size) {
	auto delta = UnsafeNumericCast<int64_t>(new_size) - UnsafeNumericCast<int64_t>(size);
	pool.UpdateUsedMemory(tag, delta);
	size = new_size;
}

void BufferPoolReservation::Merge(BufferPoolReservation src) {
	size += src.size;
	src.size = 0;
}

} // namespace duckdb
