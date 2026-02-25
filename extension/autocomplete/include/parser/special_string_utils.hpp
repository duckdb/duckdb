#pragma once
#include "duckdb/common/common.hpp"

namespace duckdb {

enum class SpecialStringCharacter { STANDARD = 0, NATIONAL_STRING, HEXADECIMAL_STRING, ESCAPE_STRING };

struct SpecialStringInfo {
	SpecialStringCharacter type;
	idx_t prefix_len;
};

static SpecialStringInfo GetSpecialStringInfo(const string &text) {
	if (text.empty()) {
		return {SpecialStringCharacter::STANDARD, 0};
	}
	char c = text[0];
	if (c == '\'') {
		return {SpecialStringCharacter::STANDARD, 1};
	}

	// Check if second char is a quote to confirm it's a special string
	if (text.size() > 1 && text[1] == '\'') {
		switch (c) {
		case 'N':
		case 'n':
			return {SpecialStringCharacter::NATIONAL_STRING, 2};
		case 'X':
		case 'x':
			return {SpecialStringCharacter::HEXADECIMAL_STRING, 2};
		case 'E':
		case 'e':
			return {SpecialStringCharacter::ESCAPE_STRING, 2};
		default:
			return {SpecialStringCharacter::STANDARD, 1};
		}
	}
	return {SpecialStringCharacter::STANDARD, 1};
}
} // namespace duckdb
