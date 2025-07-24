#include "duckdb/common/varint.hpp"
#include "duckdb/common/types/varint.hpp"

namespace duckdb {

varint_t::varint_t(ArenaAllocator &allocator, uint32_t blob_size) : string_t(blob_size), allocator(&allocator) {
	if (blob_size > string_t::INLINE_BYTES) {
		// This is a big boy, gotta allocate it
		auto ptr = allocator.Allocate(blob_size);
		SetPointer(reinterpret_cast<char *>(ptr));
	}
}

varint_t varint_t::operator*(const varint_t &rhs) const {
	return *this;
}
varint_t &varint_t::operator+=(const varint_t &rhs) {
	return *this;
}

bool IsLHSBigger(string_t &lhs, string_t &rhs) {
	if (lhs.GetSize() > rhs.GetSize()) {
		return true;
	}
	if (lhs.GetSize() < rhs.GetSize()) {
		return false;
	}
	return false;
}

void AddBinaryBuffers(char *target_buffer, idx_t target_size, const char *source_buffer, idx_t source_size, const char *source_buffer_2, idx_t source_size_2) {
	// Setup header based on sign of first source buffer
	bool is_negative = source_buffer[0] & 0x80;
	Varint::SetHeader(target_buffer, target_size - Varint::VARINT_HEADER_SIZE, is_negative);

	char carry = 0;

	// Indices for source buffers (rightmost byte index)
	idx_t i_a = source_size - 1;
	idx_t i_b = source_size_2 - 1;

	// Index for target buffer (rightmost byte index)
	idx_t i_t = target_size - 1;

	// Add bytes from right to left until at least one source buffer is fully consumed
	while (i_a >= Varint::VARINT_HEADER_SIZE || i_b >= Varint::VARINT_HEADER_SIZE) {
		int val_a = (i_a >= Varint::VARINT_HEADER_SIZE) ? static_cast<unsigned char>(source_buffer[i_a]) : 0;
		int val_b = (i_b >= Varint::VARINT_HEADER_SIZE) ? static_cast<unsigned char>(source_buffer_2[i_b]) : 0;

		int sum = val_a + val_b + carry;
		target_buffer[i_t] = static_cast<char>(sum & 0xFF);
		carry = static_cast<char>((sum >> 8) & 0xFF);

		--i_a;
		--i_b;
		--i_t;
	}

	// Propagate carry in target buffer if needed (up to header boundary)
	while (i_t >= Varint::VARINT_HEADER_SIZE && carry != 0) {
		int sum = static_cast<unsigned char>(target_buffer[i_t]) + carry;
		target_buffer[i_t] = static_cast<char>(sum & 0xFF);
		carry = static_cast<char>((sum >> 8) & 0xFF);
		--i_t;
	}
	D_ASSERT(carry == 0 && "Addition overflowed the buffer size");
}

void AddBinaryBuffersInPlace(char *target_buffer, idx_t target_size, const char *source_buffer, idx_t source_size) {
	D_ASSERT(target_size >= source_size && "Buffer a must be at least as large as b");

	char carry = 0;
	// Add from right (LSB) to left (MSB)
	idx_t i_a = target_size - 1; // last byte index in a
	idx_t i_b = source_size - 1; // last byte index in b

	// Add from right (LSB) to left (MSB), but skip metadata bytes at start
	while (i_b >= Varint::VARINT_HEADER_SIZE) {
		const char target_val = target_buffer[i_a];
		const char source_val = source_buffer[i_b];
		const int sum = target_val + source_val + carry;

		target_buffer[i_a] = static_cast<char>(sum & 0xFF);
		carry = static_cast<char>((sum >> 8) & 0xFF);
		--i_a;
		--i_b;
	}

	// Propagate carry in target if needed
	while (i_a >= Varint::VARINT_HEADER_SIZE && carry != 0) {
		const char target_value = target_buffer[i_a];
		const int sum = target_value + carry;

		target_buffer[i_a] = static_cast<char>(sum & 0xFF);
		carry = static_cast<char>((sum >> 8) & 0xFF);
		--i_a;
	}
	if (i_a == 3) {
		target_buffer[i_a] = 0;
	}

	D_ASSERT(carry == 0 && "Addition overflowed the buffer size");
}

void varint_t::ReallocateVarint() {
	// If we have to reallocate it we just double the current size
	uint32_t current_size = GetSize();
	current_size *=2;
	auto new_target_ptr = reinterpret_cast<char*>(allocator->Allocate(current_size));
	for (idx_t i = 0; i < current_size; ++i) {
		// initialize the whole pointer
		new_target_ptr[i] = 0;
	}
	SetPointer(new_target_ptr);
	SetSizeAndFinalize(current_size);
}

varint_t &varint_t::operator+=(const string_t &rhs) {
	// Let's first figure out if we need realloc, or if we can do the sum in-place
	bool realloc = false;
	auto target_ptr = GetDataWriteable();
	auto source_ptr = rhs.GetData();

	const auto target_size = GetSize();
	const auto source_size = rhs.GetSize();
	const bool same_sign = (target_ptr[0] & 0x80) == (source_ptr[0] & 0x80);

	// If target is smaller than source we absolutely have to realloc
	if (target_size < source_size) {

	}
	if (same_sign && target_size == source_size) {
		// If they both have the same sign and same size there might be a chance..
		// But only if the MSD of the MSB from the data is set
		realloc = (target_ptr[3] & 0x80) || (source_ptr[3] & 0x80);
	}
	if (!realloc) {
		// We for sure are not going to realloc, we can do it in-place
		if (same_sign) {
			AddBinaryBuffersInPlace(target_ptr, target_size, source_ptr, source_size);
			Finalize();
			return *this;
		} else {
			throw NotImplementedException("bla");
		}
	} else {
			idx_t new_target_size = target_size+1;
			auto new_target_ptr = reinterpret_cast<char*>(allocator->Allocate(new_target_size));
			AddBinaryBuffers(new_target_ptr, new_target_size, target_ptr,target_size,source_ptr,source_size);
			if (new_target_ptr[3] == 0) {
				// Turns out we did not need to resize (:
				throw NotImplementedException("dont resize me senpai");
			} else {
				SetPointer(new_target_ptr);
				SetSizeAndFinalize(new_target_size);
				return *this;
			}
		}
}
} // namespace duckdb
