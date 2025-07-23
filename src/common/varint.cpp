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

// varint_t::varint_t(string_t input) {
// 	value = input.GetString();
// }

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

void AddBinaryBuffersInPlace(char *target_buffer, size_t target_size, const char *source_buffer, size_t source_size) {
	D_ASSERT(target_size >= source_size && "Buffer a must be at least as large as b");

	char carry = 0;
	// Add from right (LSB) to left (MSB)
	ssize_t i_a = static_cast<ssize_t>(target_size) - 1; // last byte index in a
	ssize_t i_b = static_cast<ssize_t>(source_size) - 1; // last byte index in b

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
		char target_value = target_buffer[i_a];
		int sum = target_value + carry;

		target_buffer[i_a] = static_cast<char>(sum & 0xFF);
		carry = static_cast<char>((sum >> 8) & 0xFF);
		--i_a;
	}

	D_ASSERT(carry == 0 && "Addition overflowed the buffer size");
}

varint_t &varint_t::operator+=(const string_t &rhs) {
	// Let's first figure out if we need realloc, or if we can do the sum in-place
	bool realloc = false;
	auto target_ptr = GetDataWriteable();
	auto source_ptr = rhs.GetData();

	const auto target_size = GetSize();
	const auto source_size = rhs.GetSize();
	const bool same_sign = (target_ptr[0] & 0x80) == (source_ptr[0] & 0x80);

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
		// Realloc is not 100% guaranteed to happen, but we should assume it does
		throw NotImplementedException("bla");
	}
}
} // namespace duckdb
