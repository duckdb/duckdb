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
	std::cout << '\n';
}

void VarintIntermediate::Print() const {
	for (idx_t i = 0; i < size; ++i) {
		PrintBits(static_cast<char>(data[i]));
		std::cout << "  ";
	}
	std::cout << '\n';
}

VarintIntermediate::VarintIntermediate(const varint_t &value) {
	is_negative = (value.data.GetData()[0] & 0x80) == 0;
	data = reinterpret_cast<data_ptr_t>(value.data.GetDataWriteable() + Varint::VARINT_HEADER_SIZE);
	size = static_cast<uint32_t>(value.data.GetSize()) - Varint::VARINT_HEADER_SIZE;
}

VarintIntermediate::VarintIntermediate(uint8_t *value, idx_t ptr_size) {
	is_negative = (value[0] & 0x80) == 0;
	data = value + Varint::VARINT_HEADER_SIZE;
	size = static_cast<uint32_t>(ptr_size) - Varint::VARINT_HEADER_SIZE;
}

uint8_t VarintIntermediate::GetAbsoluteByte(int64_t index) const {
	if (index < 0) {
		// byte-extension
		return 0;
	}
	return is_negative ? static_cast<uint8_t>(~data[index]) : static_cast<uint8_t>(data[index]);
}

AbsoluteNumberComparison VarintIntermediate::IsAbsoluteBigger(const VarintIntermediate &rhs) const {
	idx_t actual_start_pos = GetStartDataPos();
	idx_t actual_size = size - actual_start_pos;

	idx_t rhs_actual_start_pos = rhs.GetStartDataPos();
	idx_t rhs_actual_size = rhs.size - rhs_actual_start_pos;

	// we have opposing signs, gotta do a bunch of checks to figure out who is the biggest
	// check sizes
	if (actual_size > rhs_actual_size) {
		return GREATER;
	}
	if (actual_size < rhs_actual_size) {
		return SMALLER;
	} else {
		// they have the same size then
		idx_t target_idx = actual_start_pos;
		idx_t source_idx = rhs_actual_start_pos;
		while (target_idx < size) {
			auto data_byte = GetAbsoluteByte(static_cast<int64_t>(target_idx));
			auto rhs_byte = rhs.GetAbsoluteByte(static_cast<int64_t>(source_idx));
			if (data_byte > rhs_byte) {
				return GREATER;
			} else if (data_byte < rhs_byte) {
				return SMALLER;
			}
			target_idx++;
			source_idx++;
		}
	}
	// If we got here, the values are equal.
	return EQUAL;
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

bool VarintIntermediate::OverOrUnderflow(data_ptr_t data, idx_t size, bool is_negative) {
	if (size <= Varint::MAX_DATA_SIZE) {
		return false;
	}
	// variable that stores a fully unset byte can safely be ignored
	uint8_t byte_to_compare = is_negative ? 0xFF : 0x00;
	// we will basically check if any byte has any set bit up to Varint::MAX_DATA_SIZE, if so, that's an under/overflow
	idx_t data_pos = 0;
	for (idx_t i = size; i > Varint::MAX_DATA_SIZE; i--) {
		if (data[data_pos++] != byte_to_compare) {
			return true;
		}
	}
	return false;
}

bool VarintIntermediate::OverOrUnderflow() const {
	return OverOrUnderflow(data, size, is_negative);
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

void VarintAddition(data_ptr_t result, int64_t result_end, bool is_target_absolute_bigger,
                    const VarintIntermediate &lhs, const VarintIntermediate &rhs) {
	bool is_result_negative = is_target_absolute_bigger ? lhs.is_negative : rhs.is_negative;

	int64_t i_target = lhs.size - 1;   // last byte index in target
	int64_t i_source = rhs.size - 1;   // last byte index in source
	int64_t i_result = result_end - 1; // last byte index in result

	// Carry for addition
	uint16_t carry = 0;
	uint16_t borrow = 0;
	// Add bytes from right to left
	while (i_result >= 0) {
		// If the numbers are negative, we bit flip them
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
		for (int64_t i = 0; i < result_end - rhs.size; ++i) {
			result[i] = is_result_negative ? 0xFF : 0x00;
		}
	}
}

string_t VarintIntermediate::Negate(Vector &result_vector) const {

	auto target = StringVector::EmptyString(result_vector, size + Varint::VARINT_HEADER_SIZE);
	auto ptr = target.GetDataWriteable();

	if (!is_negative && size == 1 && data[0] == 0x00) {
		// If we have a zero, we just do a copy
		Varint::SetHeader(ptr, size, is_negative);
		for (idx_t i = 0; i < size; ++i) {
			ptr[i + Varint::VARINT_HEADER_SIZE] = static_cast<char>(data[i]);
		}
	} else {
		// Otherwise, we set the header with a flip on the signal
		Varint::SetHeader(ptr, size, !is_negative);
		for (idx_t i = 0; i < size; ++i) {
			// And flip all the data bits
			ptr[i + Varint::VARINT_HEADER_SIZE] = static_cast<char>(~data[i]);
		}
	}

	return target;
}

void VarintIntermediate::NegateInPlace() {
	if (!is_negative && size == 1 && data[0] == 0x00) {
		// this is a zero, there is no negation
		return;
	}
	is_negative = !is_negative;
	for (size_t i = 0; i < size; i++) {
		data[i] = ~data[i]; // flip each byte of the pointer
	}
}

string ProduceOverUnderFlowError(bool is_result_negative, idx_t actual_start, idx_t data_size) {
	// We must throw an error, usually we should print the numbers, but I have a feeling that it won't be possible
	// here.
	std::ostringstream error;
	if (is_result_negative) {
		error << "Underflow ";
	} else {
		error << "Overflow ";
	}
	error << "in Varint Operation. A Varint can hold max " << Varint::MAX_DATA_SIZE
	      << " data bytes. Current varint has " << data_size - actual_start << " bytes.";
	return error.str();
}

string_t VarintIntermediate::Add(Vector &result_vector, const VarintIntermediate &lhs, const VarintIntermediate &rhs) {
	const bool same_sign = lhs.is_negative == rhs.is_negative;
	const uint32_t actual_size = lhs.size - lhs.GetStartDataPos();
	const uint32_t actual_rhs_size = rhs.size - rhs.GetStartDataPos();
	uint32_t result_size = actual_size;
	if (actual_size < actual_rhs_size || (same_sign && (lhs.IsMSBSet() || (rhs.IsMSBSet() && lhs.size == rhs.size)))) {
		result_size = actual_size < actual_rhs_size ? actual_rhs_size + 1 : actual_size + 1;
	}
	bool is_target_absolute_bigger = true;
	if (result_size == 0) {
		result_size++;
	}
	result_size += Varint::VARINT_HEADER_SIZE;
	if (lhs.is_negative != rhs.is_negative) {
		auto is_absolute_bigger = lhs.IsAbsoluteBigger(rhs);
		if (is_absolute_bigger == EQUAL) {
			// We set this value to 0
			auto target = StringVector::EmptyString(result_vector, result_size);
			auto target_data = target.GetDataWriteable();
			Varint::SetHeader(target_data, 1, false);
			target_data[Varint::VARINT_HEADER_SIZE] = 0;
			return target;

		} else if (is_absolute_bigger == SMALLER) {
			is_target_absolute_bigger = false;
		}
	}

	auto target = StringVector::EmptyString(result_vector, result_size);
	auto result_size_data = result_size - Varint::VARINT_HEADER_SIZE;

	auto target_data = target.GetDataWriteable();
	VarintAddition(reinterpret_cast<data_ptr_t>(target_data + Varint::VARINT_HEADER_SIZE), result_size_data,
	               is_target_absolute_bigger, lhs, rhs);
	bool is_result_negative = is_target_absolute_bigger ? lhs.is_negative : rhs.is_negative;
	if (OverOrUnderflow(reinterpret_cast<data_ptr_t>(target_data + Varint::VARINT_HEADER_SIZE), result_size_data,
	                    is_result_negative)) {
		auto actual_start = GetStartDataPos(reinterpret_cast<data_ptr_t>(target_data + Varint::VARINT_HEADER_SIZE),
		                                    result_size_data, is_result_negative);
		throw OutOfRangeException(ProduceOverUnderFlowError(is_result_negative, actual_start, result_size_data));
	}
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
	bool is_target_absolute_bigger = true;
	if (rhs.is_negative != is_negative) {
		auto is_absolute_bigger = IsAbsoluteBigger(rhs);
		if (is_absolute_bigger == EQUAL) {
			// We set this value to 0
			*this = VarintIntermediate();
			Initialize(allocator);
			return;
		} else if (is_absolute_bigger == SMALLER) {
			is_target_absolute_bigger = false;
		}
	}

	bool is_result_negative = is_target_absolute_bigger ? is_negative : rhs.is_negative;
	VarintAddition(data, size, is_target_absolute_bigger, *this, rhs);
	if (is_result_negative != is_negative) {
		is_negative = is_result_negative;
	}
	if (OverOrUnderflow()) {
		// We must throw an error, usually we should print the numbers, but I have a feeling that it won't be possible
		// here.
		throw OutOfRangeException(ProduceOverUnderFlowError(is_result_negative, GetStartDataPos(), size));
	}
}

} // namespace duckdb
