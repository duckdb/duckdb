#include "duckdb/common/types/uuid.hpp"

#include <sstream>
#include <iomanip>
#include <inttypes.h>

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
	return count == 32;
}

void UUID::ToString(hugeint_t input, char *buff) {
	std::snprintf(buff, 36, "%08" PRIx64 "-%04" PRIx64 "-%04" PRIx64 "-%04" PRIx64 "-%011" PRIx64,
	              input.upper >> 32 & 0xFFFFFFFF, input.upper >> 16 & 0xFFFF, input.upper & 0xFFFF,
	              input.lower >> 48 & 0xFFFF, input.lower >> 4 & 0xFFFFFFFFFFF);
	// snprintf always append null termintor at the end
	// which requires buff size be 37
	// but our buffer size is 36
	char last[2];
	std::snprintf(last, 2, "%01" PRIx64, input.lower & 0xF);
	buff[35] = last[0];
}

} // namespace duckdb
