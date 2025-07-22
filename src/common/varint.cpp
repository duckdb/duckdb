#include "duckdb/common/varint.hpp"
#include "duckdb/common/types/varint.hpp"

namespace duckdb {

template <class T>
varint_t IntegerToVarint(T int_value) {
	// Determine if the number is negative
	bool is_negative = int_value < 0;
	// Determine the number of data bytes
	uint64_t abs_value;
	if (is_negative) {
		if (int_value == std::numeric_limits<T>::min()) {
			abs_value = static_cast<uint64_t>(std::numeric_limits<T>::max()) + 1;
		} else {
			abs_value = static_cast<uint64_t>(std::abs(static_cast<int64_t>(int_value)));
		}
	} else {
		abs_value = static_cast<uint64_t>(int_value);
	}
	uint32_t data_byte_size;
	if (abs_value != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size = (abs_value == 0) ? 1 : static_cast<uint32_t>(std::ceil(std::log2(abs_value + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(abs_value) / 8.0));
	}
	varint_t result;

	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	// result.value = make_uniq_array<char>(blob_size);
	// result.size = blob_size;
	// auto writable_blob = &result.value[0];
	// Varint::SetHeader(writable_blob, data_byte_size, is_negative);
	//
	// // Add data bytes to the blob, starting off after header bytes
	// idx_t wb_idx = Varint::VARINT_HEADER_SIZE;
	// for (int i = static_cast<int>(data_byte_size) - 1; i >= 0; --i) {
	// 	if (is_negative) {
	// 		writable_blob[wb_idx++] = static_cast<char>(~(abs_value >> i * 8 & 0xFF));
	// 	} else {
	// 		writable_blob[wb_idx++] = static_cast<char>(abs_value >> i * 8 & 0xFF);
	// 	}
	// }
	return result;
}

varint_t::varint_t(int64_t numeric_value) {
	*this = IntegerToVarint(numeric_value);
}

// varint_t::varint_t(string_t input) {
// 	value = input.GetString();
// }

bool varint_t::operator==(const varint_t &rhs) const {
	return value == rhs.value;
}
bool varint_t::operator!=(const varint_t &rhs) const {
	return value != rhs.value;
}

bool varint_t::operator<(const varint_t &rhs) const {
	return value < rhs.value;
}

bool varint_t::operator<=(const varint_t &rhs) const {
	return value.GetDataWriteable() <= rhs.value.GetDataWriteable();
}

bool varint_t::operator>(const varint_t &rhs) const {
	return value > rhs.value;
}
bool varint_t::operator>=(const varint_t &rhs) const {
	return value.GetDataWriteable() >= rhs.value.GetDataWriteable();
}

varint_t varint_t::operator*(const varint_t &rhs) const {
	return *this;
}
varint_t &varint_t::operator+=(const varint_t &rhs) {
	return *this;
}
} // namespace duckdb
