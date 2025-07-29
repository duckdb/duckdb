#include "duckdb/common/varint.hpp"
#include "duckdb/common/types/varint.hpp"
#include <iostream>

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
		PrintBits(static_cast<char>(data[i]));
		std::cout << "  ";
	}
	std::cout << std::endl;
}

VarintIntermediate::VarintIntermediate(const varint_t &value) {
	is_negative = (value.data.GetData()[0] & 0x80) == 0;
	data = reinterpret_cast<data_ptr_t>(value.data.GetDataWriteable() + Varint::VARINT_HEADER_SIZE);
	size = static_cast<uint32_t>(value.data.GetSize()) - Varint::VARINT_HEADER_SIZE;
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

	idx_t rhs_actual_start_pos = rhs.GetStartDataPos();
	idx_t rhs_actual_size = rhs.size - rhs_actual_start_pos;

	if (is_negative != rhs.is_negative) {
		// we have opposing signs, gotta do a bunch of checks to figure out who is the biggest
		// check sizes
		if (actual_size > rhs_actual_size) {
			return 1;
		}
		if (actual_size < rhs_actual_size) {
			return -1;
		} else {
			// they have the same size then
			idx_t target_idx = actual_start_pos;
			idx_t source_idx = rhs_actual_start_pos;
			while (target_idx < size) {
				auto data_byte = GetAbsoluteByte(static_cast<int64_t>(target_idx));
				auto rhs_byte = rhs.GetAbsoluteByte(static_cast<int64_t>(source_idx));
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
	is_negative = false;
	size = 1;
	data = allocator.Allocate(size);
	// initialize the data
	data[0] = 0;
}

uint32_t VarintIntermediate::GetStartDataPos(data_ptr_t data, idx_t size, bool is_negative) {
	uint8_t non_initialized = is_negative ? 0xFF : 0x00;
	uint32_t actual_start = 0;
	for (idx_t i = 0; i < size; ++i) {
		if (data[i] == non_initialized) {
			actual_start++;
		} else {
			break;
		}
	}
	return actual_start;
}

uint32_t VarintIntermediate::GetStartDataPos() const {
	return GetStartDataPos(data, size, is_negative);
}

void VarintIntermediate::Reallocate(ArenaAllocator &allocator, idx_t min_size) {
	if (min_size < size) {
		return;
	}
	uint32_t new_size = size;
	while (new_size <= min_size) {
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

idx_t VarintIntermediate::Trim(data_ptr_t data, uint32_t &size, bool is_negative) {
	auto actual_start = GetStartDataPos(data, size, is_negative);
	if (actual_start == 0) {
		return 0;
	}
	// This bad-boy is wearing shoe lifts, time to prune it.
	D_ASSERT(actual_start <= size);
	size -= actual_start;
	if (size == 0) {
		// Always keep at least one byte
		actual_start = 0;
		size++;
	}
	memmove(data, data + actual_start, size);
	return actual_start;
}

void VarintIntermediate::Trim() {
	Trim(data, size, is_negative);
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

void VarintAddition(data_ptr_t result, int64_t result_start, int64_t result_end, bool is_target_absolute_bigger,
                    const VarintIntermediate &lhs, const VarintIntermediate &rhs) {
	bool is_result_negative = is_target_absolute_bigger ? lhs.is_negative : rhs.is_negative;

	int64_t i_target = lhs.size - 1;   // last byte index in target
	int64_t i_source = rhs.size - 1;   // last byte index in source
	int64_t i_result = result_end - 1; // last byte index in source

	// Carry for addition
	uint16_t carry = 0;
	uint16_t borrow = 0;
	// Add bytes from right to left
	while (i_result >= result_start) {
		// If the numbers are negative we bit flip them
		uint8_t target_byte = lhs.GetAbsoluteByte(i_target);
		uint8_t source_byte = rhs.GetAbsoluteByte(i_source);
		// Add bytes and carry
		uint16_t sum;
		if (lhs.is_negative == rhs.is_negative) {
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
		result[i_result] = is_result_negative ? ~result_byte : result_byte;
		i_target--;
		i_source--;
		i_result--;
	}

	if (is_result_negative != lhs.is_negative) {
		// If we are flipping the sign we must be sure that we are flipping all extra bits from our target
		for (int64_t i = result_start; i < result_end - result_start - rhs.size; ++i) {
			result[i] = is_result_negative ? 0xFF : 0x00;
		}
	}
}

string_t VarintIntermediate::Add(Vector &result_vector, const VarintIntermediate &lhs, const VarintIntermediate &rhs) {
	const bool same_sign = lhs.is_negative == rhs.is_negative;
	const uint32_t actual_size = lhs.size - lhs.GetStartDataPos();
	const uint32_t actual_rhs_size = rhs.size - rhs.GetStartDataPos();
	uint32_t result_size = actual_size;
	if (actual_size < actual_rhs_size || (same_sign && (lhs.IsMSBSet() || (rhs.IsMSBSet() && lhs.size == rhs.size)))) {
		result_size = actual_size < actual_rhs_size ? actual_rhs_size + 1 : actual_size + 1;
	}
	auto is_absolute_bigger = lhs.IsAbsoluteBigger(rhs);
	bool is_target_absolute_bigger = true;
	if (is_absolute_bigger == 0) {
		// We set this value to 0
		auto target = StringVector::EmptyString(result_vector, result_size);
		auto target_data = target.GetDataWriteable();
		Varint::SetHeader(target_data, 1, false);
		target_data[Varint::VARINT_HEADER_SIZE] = 0;
		return target;

	} else if (is_absolute_bigger == -1) {
		is_target_absolute_bigger = false;
	}
	if (result_size == 0) {
		result_size++;
	}
	result_size += Varint::VARINT_HEADER_SIZE;

	auto target = StringVector::EmptyString(result_vector, result_size);
	auto target_data = target.GetDataWriteable();
	VarintAddition(reinterpret_cast<data_ptr_t>(target_data), Varint::VARINT_HEADER_SIZE, result_size,
	               is_target_absolute_bigger, lhs, rhs);
	bool is_result_negative = is_target_absolute_bigger ? lhs.is_negative : rhs.is_negative;
	auto result_size_data = result_size - Varint::VARINT_HEADER_SIZE;
	Trim(reinterpret_cast<data_ptr_t>(target_data + Varint::VARINT_HEADER_SIZE), result_size_data, is_result_negative);
	Varint::SetHeader(target_data, result_size_data, is_result_negative);
	target.SetSizeAndFinalize(result_size_data + Varint::VARINT_HEADER_SIZE);
	return target;
}
void VarintIntermediate::AddInPlace(ArenaAllocator &allocator, const VarintIntermediate &rhs) {
	const bool same_sign = is_negative == rhs.is_negative;
	idx_t actual_size = size - GetStartDataPos();
	idx_t actual_rhs_size = rhs.size - rhs.GetStartDataPos();
	if (actual_size < actual_rhs_size || (same_sign && (IsMSBSet() || (rhs.IsMSBSet() && size == rhs.size)))) {
		// We must reallocate
		idx_t min_size = actual_size < actual_rhs_size ? actual_rhs_size + 1 : size + 1;
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
	VarintAddition(data, 0, size, is_target_absolute_bigger, *this, rhs);
	if (is_result_negative != is_negative) {
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

} // namespace duckdb
