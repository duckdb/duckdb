#include "macaddr.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include <iostream>

namespace duckdb {

static bool MacAddrError(string_t input, string *error_message, string error) {
	string e = "Failed to convert string \"" + input.GetString() + "\" to macaddr: " + error;
	HandleCastError::AssignError(e, error_message);
	return false;
}

bool MACAddr::TryParse(string_t input, MACAddr &result, string *error_message) {
	auto data = input.GetDataUnsafe();

	int vals[6] = {0, 0, 0, 0, 0, 0};

	if (data[2] != ':' && data[4] != ':' && data[6] != ':' && data[8] != ':' && data[10] != ':') {
		return MacAddrError(data, error_message, "invalid input syntax for type macaddr");
	}

	int c = 0;
	for (int i = 0; i < 6; i++) {
		uint8_t number;
		auto substr = string_t(data + (c), 2);
		if (!TryCast::Operation<string_t, uint8_t>(substr, number)) {
			return MacAddrError(input, error_message, "Expected a number between 0 and 255 " + substr.GetString());
		}
		c = c + 3;
		vals[i] = number;
	}

	for (int i = 0; i < 6; i++) {
		if (vals[i] < 0 || vals[i] > 255) {
			return MacAddrError(data, error_message, "invalid octet value in macaddr");
		}
	}

	result.a = vals[0] << 8 | vals[1];
	result.b = vals[2] << 8 | vals[3];
	result.c = vals[4] << 8 | vals[5];

	return true;
}

string MACAddr::ToString() const {
	unsigned short vals[3] = {this->a, this->b, this->c};
	string result;

	for (idx_t i = 0; i < 3; i++) {
		if (i > 0) {
			result += ":";
		}
		auto str = to_string((vals[i] & 0xFF00) >> 8) + ":" + to_string((vals[i] & 0xFF));
		result += str;
	}

	return result;
}

MACAddr MACAddr::FromString(string_t input) {
	MACAddr result;
	string error_message;
	if (!TryParse(input, result, &error_message)) {
		throw ConversionException(error_message);
	}
	return result;
}

} // namespace duckdb
