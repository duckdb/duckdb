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

void PrintBits(unsigned char value) {
	for (int i = 7; i >= 0; --i) {
		std::cout << ((value >> i) & 1);
	}
	std::cout << std::endl;
}

void AddBinaryBuffersInPlace(char *target_buffer, idx_t target_size, const char *source_buffer, idx_t source_size,
                             idx_t target_start_pos) {

	const idx_t header_size = Varint::VARINT_HEADER_SIZE;

	// Parse sign from header (sign bit: 0 = negative, 1 = positive as per your notes)
	bool target_negative = (target_buffer[0] & 0x80) == 0;
	bool source_negative = (source_buffer[0] & 0x80) == 0;
	bool result_positive = !target_negative;
	bool is_target_absolute_bigger = false;
	if (target_negative != source_negative) {
		// check sizes
		if (target_size - target_start_pos > source_size - Varint::VARINT_HEADER_SIZE) {
			result_positive = !target_negative;
			is_target_absolute_bigger = true;
		} else if (target_size - target_start_pos < source_size - Varint::VARINT_HEADER_SIZE) {
			result_positive = target_negative;
			is_target_absolute_bigger = false;
		} else {
			// they have the same size then
			idx_t i = target_start_pos;
			for (; i < target_size; ++i) {
				if (target_buffer[i] > ~source_buffer[i]) {
					is_target_absolute_bigger = true;
					result_positive = !target_negative;
					break;
				} else if (target_buffer[i] < ~source_buffer[i]) {
					is_target_absolute_bigger = false;
					result_positive = target_negative;
					break;
				}
			}
			if (i == target_size) {
				// they are ze same
				result_positive = true;
			}
		}
	}
	// Prepare working pointers (indexes)
	idx_t i_target = target_size - 1; // last byte index in target
	idx_t i_source = source_size - 1; // last byte index in source

	// Carry for addition
	uint16_t carry = 0;
	// Add bytes from right to left (LSB to MSB)
	while (i_target >= header_size) {
		uint8_t target_byte = static_cast<uint8_t>(target_buffer[i_target]);
		uint8_t source_byte;
		if (i_source >= header_size) {
			source_byte = static_cast<uint8_t>(source_buffer[i_source]);
			i_source--;
		} else {
			// Sign-extend source if source shorter than target
			source_byte = source_negative ? 0xFF : 0x00;
		}

		// Add bytes and carry
		uint16_t sum = static_cast<uint16_t>(target_byte) + static_cast<uint16_t>(source_byte) + carry;
		uint8_t result_byte = static_cast<uint8_t>(sum & 0xFF);
		if ( (target_negative || source_negative) && i_target == target_size - 1) {
			// select (-1)::VARINT +  9223372036854775807::VARINT;

			result_byte += 1;
		}
		carry = (sum >> 8) & 0xFF;

		target_buffer[i_target] = static_cast<char>(result_byte);
		i_target--;
	}
	if (result_positive != !target_negative) {
		// We should set the header again
		Varint::SetHeader(target_buffer, target_size - Varint::VARINT_HEADER_SIZE, !result_positive);
	}
}

idx_t varint_t::GetStartDataPos() const {
	auto cur_ptr = GetDataWriteable();
	auto cur_size = GetSize();
	bool is_negative = (cur_ptr[0] & 0x80) == 0;
	char value_to_trim = is_negative ? static_cast<char>(0xFF) : 0;
	idx_t data_start = 0;
	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < cur_size; ++i) {
		if (cur_ptr[i] == value_to_trim) {
			data_start++;
		} else {
			break;
		}
	}
	return data_start;
}
void varint_t::Trim() {
	auto bytes_to_trim = GetStartDataPos();
	auto cur_ptr = GetDataWriteable();
	auto cur_size = GetSize();
	bool is_negative = (cur_ptr[0] & 0x80) == 0;
	// Our data must always have at least header + 1 bytes, so we avoid trimming the value 0
	if (bytes_to_trim > 0 && cur_size - bytes_to_trim > Varint::VARINT_HEADER_SIZE + 1) {
		// This bad-boy is wearing shoe lifts, time to prune it.
		auto new_size = cur_size - bytes_to_trim;
		auto new_target_ptr = reinterpret_cast<char *>(allocator->Allocate(new_size));
		Varint::SetHeader(new_target_ptr, new_size - Varint::VARINT_HEADER_SIZE, is_negative);
		for (idx_t i = Varint::VARINT_HEADER_SIZE; i < new_size; ++i) {
			new_target_ptr[i] = cur_ptr[i + bytes_to_trim];
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
	auto new_size = min_size * 2;
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
		Reallocate(source_size + 1);
		// Get new pointer/size
		target_ptr = GetDataWriteable();
		target_size = GetSize();
	} else if (same_sign) {
		// If they both have the same sign and the MSD of the MSB from the data is set, there is a chance we might
		// need to resize
		bool is_msd_msb_set;
		if ((target_ptr[0] & 0x80) == 0) {
			is_msd_msb_set = (target_ptr[3] & 0x80) == 0 || (source_ptr[3] & 0x80) == 0;
		} else {
			is_msd_msb_set = (target_ptr[3] & 0x80) != 0 || (source_ptr[3] & 0x80) != 0;
		}
		if (is_msd_msb_set) {
			// We must reallocate
			Reallocate(target_size + 1);
			// Get new pointer/size
			target_ptr = GetDataWriteable();
			target_size = GetSize();
		}
	}
	// We for sure are not going to realloc, we can do it in-place
	AddBinaryBuffersInPlace(target_ptr, target_size, source_ptr, source_size,
	                        GetStartDataPos() + Varint::VARINT_HEADER_SIZE);
	Finalize();
	return *this;
}
} // namespace duckdb
