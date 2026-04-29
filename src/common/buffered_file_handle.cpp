#include "duckdb/common/file_system/buffered_file_handle.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"

#include <cstring>

namespace duckdb {

BufferedFileHandle::BufferedFileHandle(unique_ptr<FileHandle> inner_p) : inner(std::move(inner_p)) {
	D_ASSERT(inner);
}

BufferedFileHandle::~BufferedFileHandle() = default;

void BufferedFileHandle::CopyBufferFrom(const BufferedFileHandle &other) {
	if (other.state != BufferedFileHandleState::REALIZED) {
		state = BufferedFileHandleState::EMPTY;
		range_offset = 0;
		range_size = 0;
		buffer.reset();
		return;
	}
	range_offset = other.range_offset;
	range_size = other.range_size;
	buffer = make_unsafe_uniq_array<char>(range_size);
	std::memcpy(buffer.get(), other.buffer.get(), range_size);
	state = BufferedFileHandleState::REALIZED;
}

void BufferedFileHandle::RegisterPrefetch(idx_t offset, idx_t size) {
	D_ASSERT(inner);
	// Idempotent: if we already realized this exact range, do nothing.
	// This makes repeated CheckMagicBytes-style calls a no-op on the wire.
	if (state == BufferedFileHandleState::REALIZED && range_offset == offset && range_size == size) {
		return;
	}
	buffer.reset();
	if (size == 0) {
		state = BufferedFileHandleState::EMPTY;
		range_offset = 0;
		range_size = 0;
		return;
	}
	state = BufferedFileHandleState::HINT;
	range_offset = offset;
	range_size = size;
}

idx_t BufferedFileHandle::ReadIntoBuffer(QueryContext context, void *out, idx_t nr_bytes, idx_t location) {
	D_ASSERT(inner);

	if (state == BufferedFileHandleState::REALIZED) {
		if (location >= range_offset && location + nr_bytes <= range_offset + range_size) {
			std::memcpy(out, buffer.get() + (location - range_offset), nr_bytes);
			return nr_bytes;
		}
		// miss → fall through to forward
	} else if (state == BufferedFileHandleState::HINT) {
		// Realize the hint if the request overlaps the hint range. The inner
		// ReadIntoBuffer clamps to file size so we never read past EOF.
		const idx_t hint_end = range_offset + range_size;
		const bool overlaps = location < hint_end && location + nr_bytes > range_offset;
		if (overlaps) {
			auto buf = make_unsafe_uniq_array<char>(range_size);
			const idx_t actual_size = inner->ReadIntoBuffer(context, buf.get(), range_size, range_offset);
			if (actual_size == 0) {
				// Hint is entirely past EOF; nothing to buffer. Forward verbatim.
				state = BufferedFileHandleState::EMPTY;
				range_offset = 0;
				range_size = 0;
			} else {
				buffer = std::move(buf);
				range_size = actual_size;
				state = BufferedFileHandleState::REALIZED;
				if (location >= range_offset && location + nr_bytes <= range_offset + range_size) {
					std::memcpy(out, buffer.get() + (location - range_offset), nr_bytes);
					return nr_bytes;
				}
				// Caller's request straddles past the realized buffer; fall through.
			}
		}
	}

	return inner->ReadIntoBuffer(context, out, nr_bytes, location);
}

idx_t BufferedFileHandle::Read(void *out, idx_t nr_bytes, idx_t location) {
	return ReadIntoBuffer(QueryContext(), out, nr_bytes, location);
}

idx_t BufferedFileHandle::GetFileSize() {
	D_ASSERT(inner);
	return inner->GetFileSize();
}

string BufferedFileHandle::GetPath() const {
	D_ASSERT(inner);
	return inner->GetPath();
}

unique_ptr<FileHandle> BufferedFileHandle::ExtractInnerFileHandle() {
	buffer.reset();
	state = BufferedFileHandleState::EMPTY;
	range_offset = 0;
	range_size = 0;
	return std::move(inner);
}

} // namespace duckdb
