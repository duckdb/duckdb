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

varint_t::varint_t(ArenaAllocator &allocator, const char *data, size_t len)
    : string_t(data, static_cast<uint32_t>(len)), allocator(&allocator) {
}

varint_t varint_t::operator*(const varint_t &rhs) const {
	return *this;
}
varint_t &varint_t::operator+=(const varint_t &rhs) {
	return *this;
}

void AddBinaryBuffersInPlace(char *target_buffer, idx_t target_size, const char *source_buffer, idx_t source_size) {
	D_ASSERT(target_size >= source_size && "Buffer a must be at least as large as b");
	uint16_t carry = 0;
	idx_t i_a = target_size - 1;
	idx_t i_b = source_size - 1;

	while (i_b >= Varint::VARINT_HEADER_SIZE) {
		uint8_t target_val = static_cast<uint8_t>(target_buffer[i_a]);
		uint8_t source_val = static_cast<uint8_t>(source_buffer[i_b]);
		uint16_t sum = static_cast<uint16_t>(target_val) + static_cast<uint16_t>(source_val) + carry;
		target_buffer[i_a] = static_cast<char>(sum & 0xFF);
		carry = (sum >> 8) & 0xFF;
		--i_a;
		--i_b;
	}

	while (i_a >= Varint::VARINT_HEADER_SIZE && carry != 0) {
		uint8_t target_val = static_cast<uint8_t>(target_buffer[i_a]);
		uint16_t sum = static_cast<uint16_t>(target_val) + carry;
		target_buffer[i_a] = static_cast<char>(sum & 0xFF);
		carry = (sum >> 8) & 0xFF;
		--i_a;
	}

	D_ASSERT(carry == 0 && "Addition overflowed the buffer size");
}

void varint_t::Trim() {
	auto cur_ptr = GetDataWriteable();
	auto cur_size = GetSize();
	bool is_negative = (cur_ptr[0] & 0x80) == 0;
	char value_to_trim = is_negative ? static_cast<char>(0xFF) : 0;
	idx_t bytes_to_trim = 0;
	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < cur_size; ++i) {
		if (cur_ptr[i] == value_to_trim) {
			bytes_to_trim++;
		} else {
			break;
		}
	}
	if (bytes_to_trim > 0) {
		// This bad-boy is wearing shoe lifts, time to prune it.
		auto new_size = cur_size - bytes_to_trim;
		auto new_target_ptr = reinterpret_cast<char *>(allocator->Allocate(new_size));
		Varint::SetHeader(new_target_ptr, new_size - Varint::VARINT_HEADER_SIZE, is_negative);
		for (idx_t i = Varint::VARINT_HEADER_SIZE; i < new_size; ++i) {
			new_target_ptr[i] = cur_ptr[i+bytes_to_trim];
		}
		varint_t new_varint(*allocator, new_target_ptr, new_size);
		*this = new_varint;
	}
}

void varint_t::Reallocate(idx_t min_size) {
	// When reallocating, we double the size and properly set the new values
	// Notice that this might temporarily create an INVALID varint
	// Be sure to call TRIM, to make it valid again.
	auto current_size = GetSize();
	auto new_size = current_size * 2;
	auto new_target_ptr = reinterpret_cast<char *>(allocator->Allocate(new_size));
	auto old_data = GetData();
	bool is_negative = (old_data[0] & 0x80) == 0;
	// We initialize the new pointer
	// First we do the new header
	Varint::SetHeader(new_target_ptr, new_size - Varint::VARINT_HEADER_SIZE, is_negative);

	// Then we initialize to 0's until we have valid data again
	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < Varint::VARINT_HEADER_SIZE + (new_size - current_size); ++i) {
		// initialize all new values with 0 if positive or 0xFF if negative
		if (!is_negative) {
			new_target_ptr[i] = 0;
		} else {
			new_target_ptr[i] = static_cast<char>(0xFF);
		}
	}
	// Now we copy the old data to the new data
	idx_t j = Varint::VARINT_HEADER_SIZE;
	for (idx_t i = Varint::VARINT_HEADER_SIZE + (new_size - current_size); i < new_size; ++i) {
		new_target_ptr[i] = old_data[j++];
	}
	// Verify we reached the end of the old string
	D_ASSERT(j == GetSize());
	varint_t new_varint(*allocator, new_target_ptr, new_size);
	*this = new_varint;
}

varint_t &varint_t::operator+=(const string_t &rhs) {
	// Let's first figure out if we need realloc, or if we can do the sum in-place
	auto target_ptr = GetDataWriteable();
	auto source_ptr = rhs.GetDataWriteable();

	auto target_size = GetSize();
	auto source_size = rhs.GetSize();
	const bool same_sign = (target_ptr[0] & 0x80) == (source_ptr[0] & 0x80);
	if (target_size < source_size) {
		// We must reallocate
		Reallocate(source_size+1);
		// Get new pointer/size
		target_ptr = GetDataWriteable();
		target_size = GetSize();
	}
	else if (same_sign && target_size == source_size) {
		// If they both have the same sign and same size there might be a chance..
		// But only if the MSD of the MSB from the data is set
		if ((target_ptr[3] & 0x80) != 0 || (source_ptr[3] & 0x80) != 0) {
			// We must reallocate
			Reallocate(target_size+1);
			// Get new pointer/size
			target_ptr = GetDataWriteable();
			target_size = GetSize();
		}
	}
	// We for sure are not going to realloc, we can do it in-place
	if (same_sign) {
		AddBinaryBuffersInPlace(target_ptr, target_size, source_ptr, source_size);
		Finalize();
		return *this;
	} else {
		throw NotImplementedException("bla");
	}
}
} // namespace duckdb
