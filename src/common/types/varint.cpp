#include "duckdb/common/types/varint.hpp"

namespace duckdb {

void Varint::Verify(const string_t &input) {
#ifdef DEBUG
	// Size must be >= 4
	idx_t varint_bytes = input.GetSize();
	if (varint_bytes < 4) {
		throw InternalException("Varint number of bytes is invalid, current number of bytes is %d", varint_bytes);
	}
	// Bytes in header must quantify the number of data bytes
	auto varint_ptr = input.GetData();
	bool is_negative = (varint_ptr[0] & 0x80) == 0;
	uint32_t number_of_bytes = 0;
	char mask = 0x7F;
	if (is_negative) {
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[0] & mask) << 16 & 0xFF0000;
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[1]) << 8 & 0xFF00;
		;
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[2]) & 0xFF;
	} else {
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[0] & mask) << 16 & 0xFF0000;
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[1]) << 8 & 0xFF00;
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[2]) & 0xFF;
	}
	if (number_of_bytes != varint_bytes - 3) {
		throw InternalException("The number of bytes set in the Varint header: %d bytes. Does not "
		                        "match the number of bytes encountered as the varint data: %d bytes.",
		                        number_of_bytes, varint_bytes - 3);
	}
	//  No bytes between 4 and end can be 0, unless total size == 4
	if (varint_bytes > 4) {
		if (is_negative) {
			if (~varint_ptr[3] == 0) {
				throw InternalException("Invalid top data bytes set to 0 for VARINT values");
			}
		} else {
			if (varint_ptr[3] == 0) {
				throw InternalException("Invalid top data bytes set to 0 for VARINT values");
			}
		}
	}
#endif
}
} // namespace duckdb
