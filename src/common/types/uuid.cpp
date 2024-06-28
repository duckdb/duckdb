#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

bool UUID::FromString(const string &str, hugeint_t &result) {
	auto hex2char = [](char ch) -> unsigned char {
		if (ch >= '0' && ch <= '9') {
			return UnsafeNumericCast<unsigned char>(ch - '0');
		}
		if (ch >= 'a' && ch <= 'f') {
			return UnsafeNumericCast<unsigned char>(10 + ch - 'a');
		}
		if (ch >= 'A' && ch <= 'F') {
			return UnsafeNumericCast<unsigned char>(10 + ch - 'A');
		}
		return 0;
	};
	auto is_hex = [](char ch) -> bool {
		return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
	};

	if (str.empty()) {
		return false;
	}
	idx_t has_braces = 0;
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
	result.upper ^= (uint64_t(1) << 63);
	return count == 32;
}

void UUID::ToString(hugeint_t input, char *buf) {
	auto byte_to_hex = [](uint64_t byte_val, char *buf, idx_t &pos) {
		D_ASSERT(byte_val <= 0xFF);
		static char const HEX_DIGITS[] = "0123456789abcdef";
		buf[pos++] = HEX_DIGITS[(byte_val >> 4) & 0xf];
		buf[pos++] = HEX_DIGITS[byte_val & 0xf];
	};

	// Flip back before convert to string
	int64_t upper = int64_t(uint64_t(input.upper) ^ (uint64_t(1) << 63));
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

hugeint_t UUID::FromUHugeint(uhugeint_t input) {
	hugeint_t result;
	result.lower = input.lower;
	if (input.upper > uint64_t(NumericLimits<int64_t>::Maximum())) {
		result.upper = int64_t(input.upper - uint64_t(NumericLimits<int64_t>::Maximum()) - 1);
	} else {
		result.upper = int64_t(input.upper) - NumericLimits<int64_t>::Maximum() - 1;
	}
	return result;
}

hugeint_t UUID::GenerateRandomUUID(RandomEngine &engine) {
	uint8_t bytes[16];
	for (int i = 0; i < 16; i += 4) {
		*reinterpret_cast<uint32_t *>(bytes + i) = engine.NextRandomInteger();
	}
	// variant must be 10xxxxxx
	bytes[8] &= 0xBF;
	bytes[8] |= 0x80;
	// version must be 0100xxxx
	bytes[6] &= 0x4F;
	bytes[6] |= 0x40;

	hugeint_t result;
	result.upper = 0;
	result.upper |= ((int64_t)bytes[0] << 56);
	result.upper |= ((int64_t)bytes[1] << 48);
	result.upper |= ((int64_t)bytes[2] << 40);
	result.upper |= ((int64_t)bytes[3] << 32);
	result.upper |= ((int64_t)bytes[4] << 24);
	result.upper |= ((int64_t)bytes[5] << 16);
	result.upper |= ((int64_t)bytes[6] << 8);
	result.upper |= bytes[7];
	result.lower = 0;
	result.lower |= ((uint64_t)bytes[8] << 56);
	result.lower |= ((uint64_t)bytes[9] << 48);
	result.lower |= ((uint64_t)bytes[10] << 40);
	result.lower |= ((uint64_t)bytes[11] << 32);
	result.lower |= ((uint64_t)bytes[12] << 24);
	result.lower |= ((uint64_t)bytes[13] << 16);
	result.lower |= ((uint64_t)bytes[14] << 8);
	result.lower |= bytes[15];
	return result;
}

hugeint_t UUID::GenerateRandomUUID() {
	RandomEngine engine;
	return GenerateRandomUUID(engine);
}

} // namespace duckdb
