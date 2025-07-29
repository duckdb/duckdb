#include "duckdb/common/varint.hpp"
#include "duckdb/common/types/varint.hpp"

namespace duckdb {
void PrintBits(const char value) {
	for (int i = 7; i >= 0; --i) {
		std::cout << ((value >> i) & 1);
	}
}

void varint_t::Print() const {
	auto ptr = data.GetData();
	auto length = data.GetSize();
	for (idx_t i = 0; i < length; ++i) {
		PrintBits(ptr[i]);
		std::cout << "  ";
	}
	std::cout << std::endl;
}

void VarintIntermediate::Print() const {
	for (idx_t i = 0; i < size; ++i) {
		PrintBits(data[i]);
		std::cout << "  ";
	}
	std::cout << std::endl;
}

VarintIntermediate::VarintIntermediate(const varint_t &value) {
	is_negative = (value.data.GetData()[0] & 0x80) == 0;
	data = reinterpret_cast<data_ptr_t>(value.data.GetDataWriteable() + Varint::VARINT_HEADER_SIZE);
	size = value.data.GetSize() - Varint::VARINT_HEADER_SIZE;
}

uint8_t VarintIntermediate::GetAbsoluteByte(int64_t index) const {
	if (index < 0) {
		// byte-extension
		return 0;
	}
	return is_negative ? static_cast<uint8_t>(~data[index]) : static_cast<uint8_t>(data[index]);
}

int8_t VarintIntermediate::IsAbsoluteBigger(const VarintIntermediate &rhs) const {
	idx_t actual_start_pos = GetStartDataPos();
	idx_t actual_size = size - actual_start_pos;
	if (is_negative != rhs.is_negative) {
		// we have opposing signs, gotta do a bunch of checks to figure out who is the biggest
		// check sizes
		if (actual_size > rhs.size) {
			return 1;
		}
		if (actual_size < rhs.size) {
			return -1;
		} else {
			// they have the same size then
			idx_t target_idx = actual_start_pos;
			idx_t source_idx = 0;
			while (target_idx < size) {
				auto data_byte = GetAbsoluteByte(static_cast<int64_t>(target_idx));
				auto rhs_byte = GetAbsoluteByte(static_cast<int64_t>(source_idx));
				if (data_byte > rhs_byte) {
					return 1;
				} else if (data_byte < rhs_byte) {
					return -1;
				}
				target_idx++;
				source_idx++;
			}
		}
		// If we got here, the values are equal.
		return 0;
	} else {
		return 1;
	}
}

bool VarintIntermediate::IsMSBSet() const {
	if (is_negative) {
		return (data[0] & 0x80) == 0;
	}
	return (data[0] & 0x80) != 0;
}
void VarintIntermediate::Initialize(ArenaAllocator &allocator) {
	D_ASSERT(data == nullptr);
	is_negative = false;
	size = 1;
	data = allocator.Allocate(size);
	// initialize the data
	data[0] = 0;
}

idx_t VarintIntermediate::GetStartDataPos() const {
	uint8_t non_initialized = is_negative ? 0xFF : 0x00;
	idx_t actual_start = 0;
	for (idx_t i = 0; i < size; ++i) {
		if (data[i] == non_initialized) {
			actual_start++;
		} else {
			break;
		}
	}
	return actual_start;
}

void VarintIntermediate::Reallocate(ArenaAllocator &allocator, idx_t min_size) {
	idx_t new_size = size;
	while (new_size < min_size) {
		new_size *= 2;
	}
	auto new_data = allocator.Allocate(new_size);
	// Then we initialize to 0's until we have valid data again
	memset(new_data, is_negative ? 0xFF : 0x00, new_size - size);
	// Copy the old data to the new data
	memcpy(new_data + new_size - size, data, size);
	// Set size and pointer
	data = new_data;
	size = new_size;
}

void VarintIntermediate::Trim() {
	auto actual_start = GetStartDataPos();
	D_ASSERT(actual_start <= size);
	size -= actual_start;
	if (size == 0) {
		// Always keep at least one byte
		size++;
	}
	memmove(data, data + actual_start, size);
}
varint_t VarintIntermediate::ToVarint(ArenaAllocator &allocator) {
	// This must be trimmed before transforming
	Trim();
	varint_t result;
	uint32_t varint_size = Varint::VARINT_HEADER_SIZE + size;
	auto ptr = reinterpret_cast<char *>(allocator.Allocate(varint_size));
	// Set Header
	Varint::SetHeader(ptr, size, is_negative);
	// Copy data
	memcpy(ptr + Varint::VARINT_HEADER_SIZE, data, size);
	result.data = string_t(ptr, varint_size);
	return result;
}

string_t VarintIntermediate::ToVarint(Vector &result_vector) {
	Trim();
	varint_t result;
	uint32_t varint_size = Varint::VARINT_HEADER_SIZE + size;

	auto target = StringVector::EmptyString(result_vector, varint_size);
	auto target_data = target.GetDataWriteable();
	// Set Header
	Varint::SetHeader(target_data, size, is_negative);
	// Copy data
	memcpy(target_data + Varint::VARINT_HEADER_SIZE, data, size);
	target.Finalize();
	return target;
}

void VarintIntermediate::AddInPlace(ArenaAllocator &allocator, const VarintIntermediate &rhs) {
	const bool same_sign = is_negative == rhs.is_negative;
	if (size < rhs.size || (same_sign && (IsMSBSet() || (rhs.IsMSBSet() && size == rhs.size)))) {
		// We must reallocate
		idx_t min_size = size < rhs.size ? rhs.size + 1 : size + 1;
		Reallocate(allocator, min_size);
	}

	auto is_absolute_bigger = IsAbsoluteBigger(rhs);
	bool is_target_absolute_bigger = true;
	if (is_absolute_bigger == 0) {
		// We set this value to 0
		*this = VarintIntermediate();
		Initialize(allocator);
		return;
	} else if (is_absolute_bigger == -1) {
		is_target_absolute_bigger = false;
	}
	bool is_result_negative = is_target_absolute_bigger ? is_negative : rhs.is_negative;

	int64_t i_target = size - 1;     // last byte index in target
	int64_t i_source = rhs.size - 1; // last byte index in source

	// Carry for addition
	uint16_t carry = 0;
	uint16_t borrow = 0;
	// Add bytes from right to left
	while (i_target >= 0) {
		// If the numbers are negative we bit flip them
		uint8_t target_byte = GetAbsoluteByte(i_target);
		uint8_t source_byte = rhs.GetAbsoluteByte(i_source);
		// Add bytes and carry
		uint16_t sum;
		if (is_negative == rhs.is_negative) {
			sum = static_cast<uint16_t>(target_byte) + static_cast<uint16_t>(source_byte) + carry;
			carry = (sum >> 8) & 0xFF;
		} else {
			if (is_target_absolute_bigger) {
				sum = static_cast<uint16_t>(target_byte) - static_cast<uint16_t>(source_byte) - borrow;
				borrow = sum > static_cast<uint16_t>(target_byte) ? 1 : 0;
			} else {
				sum = static_cast<uint16_t>(source_byte) - static_cast<uint16_t>(target_byte) - borrow;
				borrow = sum > static_cast<uint16_t>(source_byte) ? 1 : 0;
			}
		}
		uint8_t result_byte = static_cast<uint8_t>(sum & 0xFF);
		// If the result is not positive, we must flip the bits again
		data[i_target] = is_result_negative ? static_cast<char>(~result_byte) : static_cast<char>(result_byte);
		i_target--;
		i_source--;
	}

	if (is_result_negative != is_negative) {
		// If we are flipping the sign we must be sure that we are flipping all extra bits from our target
		for (idx_t i = 0; i < size - rhs.size; ++i) {
			data[i] = is_result_negative ? static_cast<char>(0xFF) : 0x00;
		}
		is_negative = is_result_negative;
	}
}

void AddBinaryBuffersInPlace(char *target_buffer, idx_t target_size, const char *source_buffer, idx_t source_size,
                             idx_t target_start_pos) {
	constexpr idx_t header_size = Varint::VARINT_HEADER_SIZE;

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
			idx_t target_idx = target_start_pos;
			idx_t source_idx = Varint::VARINT_HEADER_SIZE;
			if (target_negative) {
				while (target_idx < target_size) {
					if (static_cast<uint8_t>(~target_buffer[target_idx]) >
					    static_cast<uint8_t>(source_buffer[source_idx])) {
						is_target_absolute_bigger = true;
						break;
					} else if (static_cast<uint8_t>(~target_buffer[target_idx]) <
					           static_cast<uint8_t>(source_buffer[source_idx])) {
						is_target_absolute_bigger = false;
						break;
					}
					target_idx++;
					source_idx++;
				}
			} else {
				while (target_idx < target_size) {
					if (static_cast<uint8_t>(target_buffer[target_idx]) >
					    static_cast<uint8_t>(~source_buffer[source_idx])) {
						is_target_absolute_bigger = true;
						break;
					} else if (static_cast<uint8_t>(target_buffer[target_idx]) <
					           static_cast<uint8_t>(~source_buffer[source_idx])) {
						is_target_absolute_bigger = false;
						break;
					}
					target_idx++;
					source_idx++;
				}
			}
			if (is_target_absolute_bigger) {
				result_positive = !target_negative;
			} else {
				result_positive = !source_negative;
			}

			if (target_idx == target_size) {
				// they are ze same values, but opposing signs
				// Set target to 0 and skedaddle
				Varint::SetHeader(target_buffer, target_size - Varint::VARINT_HEADER_SIZE, false);
				for (idx_t i = Varint::VARINT_HEADER_SIZE; i < target_size; ++i) {
					target_buffer[i] = 0;
				}
				return;
			}
		}
	}

	// Both numbers have the same sign, we can simply add them.
	idx_t i_target = target_size - 1; // last byte index in target
	idx_t i_source = source_size - 1; // last byte index in source

	// Carry for addition
	uint16_t carry = 0;
	uint16_t borrow = 0;
	// Add bytes from right to left
	while (i_target >= header_size) {
		// If the numbers are negative we bit flip them
		uint8_t target_byte = target_negative ? static_cast<uint8_t>(~target_buffer[i_target])
		                                      : static_cast<uint8_t>(target_buffer[i_target]);
		uint8_t source_byte;
		if (i_source >= header_size) {
			source_byte = source_negative ? static_cast<uint8_t>(~source_buffer[i_source])
			                              : static_cast<uint8_t>(source_buffer[i_source]);
			i_source--;
		} else {
			// Sign-extend source if source shorter than target
			source_byte = 0x00;
		}

		// Add bytes and carry
		uint16_t sum;
		if (target_negative == source_negative) {
			sum = static_cast<uint16_t>(target_byte) + static_cast<uint16_t>(source_byte) + carry;
			carry = (sum >> 8) & 0xFF;
		} else {
			if (is_target_absolute_bigger) {
				sum = static_cast<uint16_t>(target_byte) - static_cast<uint16_t>(source_byte) - borrow;
				borrow = sum > static_cast<uint16_t>(target_byte) ? 1 : 0;
			} else {
				sum = static_cast<uint16_t>(source_byte) - static_cast<uint16_t>(target_byte) - borrow;
				borrow = sum > static_cast<uint16_t>(source_byte) ? 1 : 0;
			}
		}
		uint8_t result_byte = static_cast<uint8_t>(sum & 0xFF);

		// If the result is not positive, we must flip the bits again
		target_buffer[i_target] = !result_positive ? static_cast<char>(~result_byte) : static_cast<char>(result_byte);
		i_target--;
	}

	if (result_positive != !target_negative) {
		// If we are flipping the sign we must be sure that we are flipping all extra bits from our target
		for (idx_t i = Varint::VARINT_HEADER_SIZE; i < target_size - (source_size - Varint::VARINT_HEADER_SIZE); ++i) {
			target_buffer[i] = result_positive ? 0x00 : static_cast<char>(0xFF);
		}
		// We should set the header again
		Varint::SetHeader(target_buffer, target_size - Varint::VARINT_HEADER_SIZE, !result_positive);
	}
}

// idx_t varint_t::GetStartDataPos() const {
// 	auto cur_ptr = data.GetDataWriteable();
// 	auto cur_size = data.GetSize();
// 	bool is_negative = (cur_ptr[0] & 0x80) == 0;
// 	char value_to_trim = is_negative ? static_cast<char>(0xFF) : 0;
// 	idx_t data_start = 0;
// 	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < cur_size; ++i) {
// 		if (cur_ptr[i] == value_to_trim) {
// 			data_start++;
// 		} else {
// 			break;
// 		}
// 	}
// 	return data_start;
// }
// void varint_t::Trim(ArenaAllocator &allocator) {
// 	auto bytes_to_trim = GetStartDataPos();
// 	auto cur_ptr = data.GetDataWriteable();
// 	auto cur_size = data.GetSize();
// 	bool is_negative = (cur_ptr[0] & 0x80) == 0;
// 	// Our data must always have at least header + 1 bytes, so we avoid trimming the value 0
// 	if (bytes_to_trim > 0) {
// 		if (cur_size - bytes_to_trim < Varint::VARINT_HEADER_SIZE + 1) {
// 			// This guy is a 0
// 			while (bytes_to_trim > 0 && cur_size - bytes_to_trim < Varint::VARINT_HEADER_SIZE + 1) {
// 				bytes_to_trim--;
// 			}
// 		}
// 		// This bad-boy is wearing shoe lifts, time to prune it.
// 		auto new_size = cur_size - bytes_to_trim;
// 		auto new_target_ptr = reinterpret_cast<char *>(allocator.Allocate(new_size));
// 		Varint::SetHeader(new_target_ptr, new_size - Varint::VARINT_HEADER_SIZE, is_negative);
// 		for (idx_t i = Varint::VARINT_HEADER_SIZE; i < new_size; ++i) {
// 			new_target_ptr[i] = cur_ptr[i + bytes_to_trim];
// 		}
// 		data = string_t(new_target_ptr, static_cast<uint32_t>(new_size));
// 	}
// }

// void varint_t::Reallocate(ArenaAllocator &allocator, idx_t min_size) {
// 	// When reallocating, we double the size and properly set the new values
// 	// Notice that this might temporarily create an INVALID varint
// 	// Be sure to call TRIM, to make it valid again.
// 	const auto current_size = data.GetSize();
// 	const auto new_size = min_size * 2;
// 	const auto new_target_ptr = reinterpret_cast<char *>(allocator.Allocate(new_size));
// 	const auto old_data = data.GetData();
// 	const bool is_negative = (old_data[0] & 0x80) == 0;
// 	// We initialize the new pointer
// 	// First we do the new header
// 	Varint::SetHeader(new_target_ptr, new_size - Varint::VARINT_HEADER_SIZE, is_negative);
//
// 	// Then we initialize to 0's until we have valid data again
// 	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < Varint::VARINT_HEADER_SIZE + (new_size - current_size); ++i) {
// 		// initialize all new values with 0 if positive or 0xFF if negative
// 		if (!is_negative) {
// 			new_target_ptr[i] = 0;
// 		} else {
// 			new_target_ptr[i] = static_cast<char>(0xFF);
// 		}
// 	}
// 	// Now we copy the old data to the new data
// 	idx_t j = Varint::VARINT_HEADER_SIZE;
// 	for (idx_t i = Varint::VARINT_HEADER_SIZE + (new_size - current_size); i < new_size; ++i) {
// 		new_target_ptr[i] = old_data[j++];
// 	}
// 	// Verify we reached the end of the old string
// 	D_ASSERT(j == data.GetSize());
// 	data = string_t(new_target_ptr, static_cast<uint32_t>(new_size));
// }

// void varint_t::AddInPlace(ArenaAllocator &allocator, const varint_t &rhs) {
// 	// Let's first figure out if we need realloc, or if we can do the sum in-place
// 	auto target_ptr = data.GetDataWriteable();
// 	auto source_ptr = rhs.data.GetDataWriteable();
// 	auto target_size = data.GetSize();
// 	auto source_size = rhs.data.GetSize();
// 	const bool same_sign = (target_ptr[0] & 0x80) == (source_ptr[0] & 0x80);
// 	if (target_size < source_size) {
// 		// We must reallocate
// 		Reallocate(allocator, source_size + 1);
// 		// Get new pointer/size
// 		target_ptr = data.GetDataWriteable();
// 		target_size = data.GetSize();
// 	} else if (same_sign) {
// 		// If they both have the same sign and the most significant bit of the data is set, we need to resize.
// 		bool is_msb;
// 		bool is_negative = (target_ptr[0] & 0x80) == 0;
// 		if (is_negative) {
// 			// if both are negative numbers we care if msb is 0 on target. If both numbers have the same size, we care
// 			// also about the source msb.
// 			is_msb = (target_ptr[3] & 0x80) == 0 || ((source_ptr[3] & 0x80) == 0 && target_size == source_size);
// 		} else {
// 			// If both are positive numbers we care if msb is 1 on target. If both numbers have the same size, we care
// 			// also about the source msb.
// 			is_msb = (target_ptr[3] & 0x80) != 0 || ((source_ptr[3] & 0x80) != 0 && target_size == source_size);
// 		}
// 		if (is_msb) {
// 			// We must reallocate
// 			Reallocate(allocator, target_size + 1);
// 			// Get new pointer/size
// 			target_ptr = data.GetDataWriteable();
// 			target_size = data.GetSize();
// 		}
// 	}
// 	// We for sure are not going to realloc, we can do it in-place
// 	AddBinaryBuffersInPlace(target_ptr, target_size, source_ptr, source_size,
// 	                        GetStartDataPos() + Varint::VARINT_HEADER_SIZE);
// 	data.Finalize();
// }

} // namespace duckdb
