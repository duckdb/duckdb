#ifdef UUID_LIBUUID
#include <uuid/uuid.h>
#endif

#ifdef UUID_CFUUID
#include <CoreFoundation/CFUUID.h>
#endif

#ifdef UUID_WINDOWS
#include <objbase.h>
#endif

#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

bool UUID::FromString(string str, hugeint_t &result) {
	auto hex2char = [](char ch) -> unsigned char {
		if (ch >= '0' && ch <= '9') {
			return ch - '0';
		}
		if (ch >= 'a' && ch <= 'f') {
			return 10 + ch - 'a';
		}
		if (ch >= 'A' && ch <= 'F') {
			return 10 + ch - 'A';
		}
		return 0;
	};
	auto is_hex = [](char ch) -> bool {
		return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
	};

	if (str.empty()) {
		return false;
	}
	int has_braces = 0;
	if (str.front() == '{') {
		has_braces = 1;
	}
	if (has_braces && str.back() != '}') {
		return false;
	}

	result.lower = 0;
	result.upper = 0;
	size_t count = 0;
	for (size_t i = has_braces; i < str.size() - has_braces; ++i) {
		if (str[i] == '-') {
			continue;
		}
		if (count >= 32 || !is_hex(str[i])) {
			return false;
		}
		if (count >= 16) {
			result.lower = (result.lower << 4) | hex2char(str[i]);
		} else {
			result.upper = (result.upper << 4) | hex2char(str[i]);
		}
		count++;
	}
	// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
	result.upper ^= (int64_t(1) << 63);
	return count == 32;
}

void UUID::ToString(hugeint_t input, char *buf) {
	auto byte_to_hex = [](char byte_val, char *buf, idx_t &pos) {
		static char const HEX_DIGITS[] = "0123456789abcdef";
		buf[pos++] = HEX_DIGITS[(byte_val >> 4) & 0xf];
		buf[pos++] = HEX_DIGITS[byte_val & 0xf];
	};

	// Flip back before convert to string
	int64_t upper = input.upper ^ (int64_t(1) << 63);
	idx_t pos = 0;
	byte_to_hex(upper >> 56 & 0xFF, buf, pos);
	byte_to_hex(upper >> 48 & 0xFF, buf, pos);
	byte_to_hex(upper >> 40 & 0xFF, buf, pos);
	byte_to_hex(upper >> 32 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(upper >> 24 & 0xFF, buf, pos);
	byte_to_hex(upper >> 16 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(upper >> 8 & 0xFF, buf, pos);
	byte_to_hex(upper & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(input.lower >> 56 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 48 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(input.lower >> 40 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 32 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 24 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 16 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 8 & 0xFF, buf, pos);
	byte_to_hex(input.lower & 0xFF, buf, pos);
}

// This is the linux friendly implementation, but it could work on other
// systems that have libuuid available
#ifdef UUID_LIBUUID
hugeint_t UUID::Random() {
	uuid_t data;
	uuid_generate(data);
	hugeint_t result;
	result.upper = 0;
	result.upper |= ((int64_t)data[0] << 56);
	result.upper |= ((int64_t)data[1] << 48);
	result.upper |= ((int64_t)data[2] << 40);
	result.upper |= ((int64_t)data[3] << 32);
	result.upper |= ((int64_t)data[4] << 24);
	result.upper |= ((int64_t)data[5] << 16);
	result.upper |= ((int64_t)data[6] << 8);
	result.upper |= data[7];
	result.lower = 0;
	result.lower |= ((uint64_t)data[8] << 56);
	result.lower |= ((uint64_t)data[9] << 48);
	result.lower |= ((uint64_t)data[10] << 40);
	result.lower |= ((uint64_t)data[11] << 32);
	result.lower |= ((uint64_t)data[12] << 24);
	result.lower |= ((uint64_t)data[13] << 16);
	result.lower |= ((uint64_t)data[14] << 8);
	result.lower |= data[15];

	// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
	result.upper ^= (int64_t(1) << 63);
	return result;
}
#endif

// this is the mac and ios version
#ifdef UUID_CFUUID
hugeint_t UUID::Random() {
	auto newId = CFUUIDCreate(NULL);
	auto bytes = CFUUIDGetUUIDBytes(newId);
	CFRelease(newId);

	hugeint_t result;
	result.upper = 0;
	result.upper |= ((int64_t)bytes.byte0 << 56);
	result.upper |= ((int64_t)bytes.byte1 << 48);
	result.upper |= ((int64_t)bytes.byte2 << 40);
	result.upper |= ((int64_t)bytes.byte3 << 32);
	result.upper |= ((int64_t)bytes.byte4 << 24);
	result.upper |= ((int64_t)bytes.byte5 << 16);
	result.upper |= ((int64_t)bytes.byte6 << 8);
	result.upper |= bytes.byte7;
	result.lower = 0;
	result.lower |= ((uint64_t)bytes.byte8 << 56);
	result.lower |= ((uint64_t)bytes.byte9 << 48);
	result.lower |= ((uint64_t)bytes.byte10 << 40);
	result.lower |= ((uint64_t)bytes.byte11 << 32);
	result.lower |= ((uint64_t)bytes.byte12 << 24);
	result.lower |= ((uint64_t)bytes.byte13 << 16);
	result.lower |= ((uint64_t)bytes.byte14 << 8);
	result.lower |= bytes.byte15;

	// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
	result.upper ^= (int64_t(1) << 63);
	return result;
}
#endif

// obviously this is the windows version
#ifdef UUID_WINDOWS
hugeint_t UUID::Random() {
	GUID newId;
	CoCreateGuid(&newId);

	hugeint_t result;
	result.upper = 0;
	result.upper |= ((int64_t)((newId.Data1 >> 24) & 0xFF) << 56);
	result.upper |= ((int64_t)((newId.Data1 >> 16) & 0xFF) << 48);
	result.upper |= ((int64_t)((newId.Data1 >> 8) & 0xFF) << 40);
	result.upper |= ((int64_t)((newId.Data1) & 0xFF) << 32);
	result.upper |= ((int64_t)((newId.Data2 >> 8) & 0xFF) << 24);
	result.upper |= ((int64_t)((newId.Data2) & 0xFF) << 16);
	result.upper |= ((int64_t)((newId.Data3 >> 8) & 0xFF) << 8);
	result.upper |= ((newId.Data3) & 0xFF);
	result.lower = 0;
	result.lower |= ((uint64_t)newId.Data4[0] << 56);
	result.lower |= ((uint64_t)newId.Data4[1] << 48);
	result.lower |= ((uint64_t)newId.Data4[2] << 40);
	result.lower |= ((uint64_t)newId.Data4[3] << 32);
	result.lower |= ((uint64_t)newId.Data4[4] << 24);
	result.lower |= ((uint64_t)newId.Data4[5] << 16);
	result.lower |= ((uint64_t)newId.Data4[6] << 8);
	result.lower |= newId.Data4[7];

	// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
	result.upper ^= (int64_t(1) << 63);
	return result;
}
#endif

} // namespace duckdb
