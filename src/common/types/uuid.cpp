#include "duckdb/common/types/uuid.hpp"

#include <sstream>
#include <iomanip>

namespace duckdb {

bool UUID::FromString(string str, hugeint_t &result) {
	auto hex2char = [](char ch) -> unsigned char
    {
         if (ch >= '0' && ch <= '9')
            return ch - '0';
         if (ch >= 'a' && ch <= 'f')
            return 10 + ch - 'a';
         if (ch >= 'A' && ch <= 'F')
            return 10 + ch - 'A';
         return 0;
    };
	auto is_hex = [](char ch) -> bool
	{
         return
			 (ch >= '0' && ch <= '9') ||
			 (ch >= 'a' && ch <= 'f') ||
			 (ch >= 'A' && ch <= 'F');
	};


	if (str.empty()) return false;
	int hasBraces = 0;
	if (str.front() == '{') hasBraces = 1;
	if (hasBraces && str.back() != '}') return false;

	result.lower = 0;
	result.upper = 0;
	size_t count = 0;
	for (size_t i = hasBraces; i < str.size() - hasBraces; ++i)
	{
		if (str[i] == '-') continue;
		if (count >= 32 || !is_hex(str[i])) return false;
		if (count >= 16) {
			result.lower = (result.lower<<4) | hex2char(str[i]);
		} else {
			result.upper = (result.upper<<4) | hex2char(str[i]);
		}
		count++;
	}
	return count == 32;
}

void UUID::ToString(hugeint_t input, char *buff) {
	std::snprintf(buff, 9, "%08llx", input.upper>>32&0xFFFFFFFF);
	buff[8] = '-';
	std::snprintf(buff+9, 5, "%04llx", input.upper>>16&0xFFFF);
	buff[13] = '-';
	std::snprintf(buff+14, 5, "%04llx", input.upper&0xFFFF);
	buff[18] = '-';
	std::snprintf(buff+19, 5, "%04llx", input.lower>>48&0xFFFF);
	buff[23] = '-';
	std::snprintf(buff+24, 12, "%011llx", input.lower>>4&0xFFFFFFFFFFFF);
	// snprintf always append null termintor at the end
	// which requires buff size be 37
	// but our buffer size is 36
	char last[2];
	std::snprintf(last, 2, "%01llx", input.lower&0xF);
	buff[35] = last[0];
}

} // namespace duckdb
