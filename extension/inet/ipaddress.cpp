#include "ipaddress.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

IPAddress::IPAddress() : type(IPAddressType::IP_ADDRESS_INVALID) {
}

IPAddress::IPAddress(IPAddressType type, hugeint_t address, uint16_t mask) : type(type), address(address), mask(mask) {
}

IPAddress IPAddress::FromIPv4(int32_t address, uint16_t mask) {
	return IPAddress(IPAddressType::IP_ADDRESS_V4, address, mask);
}
IPAddress IPAddress::FromIPv6(hugeint_t address, uint16_t mask) {
	return IPAddress(IPAddressType::IP_ADDRESS_V6, address, mask);
}

static bool IPAddressError(string_t input, string *error_message, string error) {
	string e = "Failed to convert string \"" + input.GetString() + "\" to inet: " + error;
	HandleCastError::AssignError(e, error_message);
	return false;
}

bool IPAddress::TryParse(string_t input, IPAddress &result, string *error_message) {
	auto data = input.GetData();
	auto size = input.GetSize();
	idx_t c = 0;
	idx_t number_count = 0;
	uint32_t address = 0;
	result.type = IPAddressType::IP_ADDRESS_V4;
parse_number:
	idx_t start = c;
	while (c < size && data[c] >= '0' && data[c] <= '9') {
		c++;
	}
	if (start == c) {
		return IPAddressError(input, error_message, "Expected a number");
	}
	uint8_t number;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), number)) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 255");
	}
	address <<= 8;
	address += number;
	number_count++;
	result.address = address;
	if (number_count == 4) {
		goto parse_mask;
	} else {
		goto parse_dot;
	}
parse_dot:
	if (c == size || data[c] != '.') {
		return IPAddressError(input, error_message, "Expected a dot");
	}
	c++;
	goto parse_number;
parse_mask:
	if (c == size) {
		// no mask, set to default
		result.mask = IPAddress::IPV4_DEFAULT_MASK;
		return true;
	}
	if (data[c] != '/') {
		return IPAddressError(input, error_message, "Expected a slash");
	}
	c++;
	start = c;
	while (c < size && data[c] >= '0' && data[c] <= '9') {
		c++;
	}
	uint8_t mask;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), mask)) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 32");
	}
	if (mask > 32) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 32");
	}
	result.mask = mask;
	return true;
}

string IPAddress::ToString() const {
	string result;
	for (idx_t i = 0; i < 4; i++) {
		if (i > 0) {
			result += ".";
		}
		uint8_t byte = Hugeint::Cast<uint8_t>((address >> (3 - i) * 8) & 0xFF);
		auto str = to_string(byte);
		result += str;
	}
	if (mask != IPAddress::IPV4_DEFAULT_MASK) {
		result += "/" + to_string(mask);
	}
	return result;
}

IPAddress IPAddress::FromString(string_t input) {
	IPAddress result;
	string error_message;
	if (!TryParse(input, result, &error_message)) {
		throw ConversionException(error_message);
	}
	return result;
}

} // namespace duckdb
