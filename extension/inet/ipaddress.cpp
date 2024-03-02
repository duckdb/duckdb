#include "ipaddress.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/string_util.hpp"

#include <sstream>
#include <ios>

namespace duckdb {

constexpr static const int32_t HEX_BITSIZE = 4;
constexpr static const int32_t MAX_QUIBBLE_DIGITS = 4;
constexpr static const idx_t QUIBBLES_PER_HALF = 4;

IPAddress::IPAddress() : type(IPAddressType::IP_ADDRESS_INVALID) {
}

IPAddress::IPAddress(IPAddressType type, uhugeint_t address, uint16_t mask) : type(type), address(address), mask(mask) {
}

IPAddress IPAddress::FromIPv4(int32_t address, uint16_t mask) {
	return IPAddress(IPAddressType::IP_ADDRESS_V4, address, mask);
}
IPAddress IPAddress::FromIPv6(uhugeint_t address, uint16_t mask) {
	return IPAddress(IPAddressType::IP_ADDRESS_V6, address, mask);
}

static bool IPAddressError(string_t input, CastParameters &parameters, string error) {
	string e = "Failed to convert string \"" + input.GetString() + "\" to inet: " + error;
	HandleCastError::AssignError(e, parameters);
	return false;
}

// Even though inet_pton() and inet_ntop() exist in network libraries, the
// parsing and formatting functions are implemented in-line here to ensure
// consistent behavior across implementations as well as provide better error
// messages.
//
// Additionally, there wouldn't be much code savings since using
// those functions would need to use pre-processor directives between Windows
// and POSIX systems, temporary structures would need to be created and data
// copied into and of them, and careful bytes swapping would be necessary to get
// the resulting values into the proper native types.

static bool TryParseIPv4(string_t input, IPAddress &result, CastParameters &parameters) {
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
		return IPAddressError(input, parameters, "Expected a number");
	}
	uint8_t number;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), number)) {
		return IPAddressError(input, parameters, "Expected a number between 0 and 255");
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
		return IPAddressError(input, parameters, "Expected a dot");
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
		return IPAddressError(input, parameters, "Expected a slash");
	}
	c++;
	start = c;
	while (c < size && data[c] >= '0' && data[c] <= '9') {
		c++;
	}
	uint8_t mask;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), mask)) {
		return IPAddressError(input, parameters, "Expected a number between 0 and 32");
	}
	if (mask > 32) {
		return IPAddressError(input, parameters, "Expected a number between 0 and 32");
	}
	result.mask = mask;
	return true;
}

/*
  IPv6 addresses are 128-bit values.

  When written, these are broken up into 8 16-bit values and represented as up
  to 4 hexadecimal digits. Formally, these values are called hextets, but
  informally they can be called quibbles. This comes from the fact there are 4
  "nibbles" (4-bit) values, so quad-nibble, or quibble.

  A series of 2 or more zero quibbles can be written as a double-colon, "::".
  This can be done only once, for the longest run of zero quibbles, in a given
  address.

  For example:

    2001:db8:0:0:0:cef3:35:363

  becomes

    2001:db8::cef3:35:363

  Both address representations are considered valid, but the compressed form is
  canonical and should be preferred in textual output. More examples can be
  found in test cases, such as test/sql/inet/test_ipv6_inet_type.test.
*/
static void ParseQuibble(uint16_t &result, const char *buf, idx_t len) {
	result = 0;
	for (idx_t c = 0; c < len; ++c) {
		result = (result << HEX_BITSIZE) + StringUtil::GetHexValue(buf[c]);
	}
}

/*
Compute the bitshift to store or retrieve a given quibble from one of the halves
of an address.
 */
static idx_t QuibbleHalfAddressBitShift(const idx_t quibble, bool &is_upper) {
	const idx_t this_offset = quibble % QUIBBLES_PER_HALF;
	const idx_t quibble_shift = (QUIBBLES_PER_HALF - 1) - this_offset;
	is_upper = quibble < QUIBBLES_PER_HALF;

	return quibble_shift * IPAddress::IPV6_QUIBBLE_BITS;
}

static bool TryParseIPv6(string_t input, IPAddress &result, CastParameters &parameters) {
	auto data = input.GetData();
	auto size = input.GetSize();
	idx_t c = 0;
	int parsed_quibble_count = 0;
	uint16_t quibbles[IPAddress::IPV6_NUM_QUIBBLE] = {};
	int first_quibble_count = -1;
	result.type = IPAddressType::IP_ADDRESS_V6;
	result.mask = IPAddress::IPV6_DEFAULT_MASK;
	while (c < size && parsed_quibble_count < IPAddress::IPV6_NUM_QUIBBLE) {
		// Find and parse the next quibble
		auto start = c;
		while (c < size && StringUtil::CharacterIsHex(data[c])) {
			++c;
		}
		idx_t len = c - start;
		if (len > MAX_QUIBBLE_DIGITS) {
			return IPAddressError(input, parameters, "Expected 4 or fewer hex digits");
		}

		if (c < size && data[c] == '.') {
			// This might be the IPv4 dotted decimal form, but it must occur at the end
			// so find the full length, and confirm only valid characters are present.
			c = start;
			while (c < size && (StringUtil::CharacterIsDigit(data[c]) || data[c] == '.')) {
				++c;
			}

			// c must either be at the end, or pointing to the "/" of the prefix mask.
			if (c < size && data[c] != '/') {
				return IPAddressError(input, parameters, "IPv4 format can only be used for the final 2 quibbles.");
			}

			IPAddress ipv4;
			if (!TryParseIPv4(string_t(&data[start], c - start), ipv4, parameters)) {
				return false;
			}

			// Put the ipv4 parsed 2 quibbles into the proper address location.
			quibbles[parsed_quibble_count++] = ipv4.address.lower >> IPAddress::IPV6_QUIBBLE_BITS;
			quibbles[parsed_quibble_count++] = ipv4.address.lower & 0xffff;
			continue;
		}

		if (c < size && data[c] != ':' && data[c] != '/') {
			return IPAddressError(input, parameters, "Unexpected character found");
		}

		if (len > 0) {
			ParseQuibble(quibbles[parsed_quibble_count++], &data[start], len);
		}

		// Check for double colon
		if (c + 1 < size && data[c] == ':' && data[c + 1] == ':') {
			if (first_quibble_count != -1) {
				return IPAddressError(input, parameters, "Encountered more than one double-colon");
			}
			// Special check for another colon, any other invalid character will
			// be caught in the main loop
			if (c + 2 < size && data[c + 2] == ':') {
				return IPAddressError(input, parameters, "Encountered more than two consecutive colons");
			}
			first_quibble_count = parsed_quibble_count;
			++c;
		}

		// Parse the mask if specified
		if (c < size && data[c] == '/') {
			start = ++c;
			while (c < size && StringUtil::CharacterIsDigit(data[c])) {
				++c;
			}
			uint8_t mask;
			if (!TryCast::Operation<string_t, uint8_t>(string_t(&data[start], c - start), mask)) {
				return IPAddressError(input, parameters, "Expected a number between 0 and 128");
			}
			if (mask > IPAddress::IPV6_DEFAULT_MASK) {
				return IPAddressError(input, parameters, "Expected a number between 0 and 128");
			}
			result.mask = mask;
			break;
		}
		++c;
	}

	if (parsed_quibble_count < IPAddress::IPV6_NUM_QUIBBLE && first_quibble_count == -1) {
		return IPAddressError(input, parameters, "Expected 8 sets of 4 hex digits.");
	}

	if (c < size) {
		return IPAddressError(input, parameters, "Unexpected extra characters");
	}

	// Operate on each half of the 128 bit address directly to make the bit operations much more
	// efficient.
	result.address.upper = 0;
	result.address.lower = 0;

	idx_t output_idx = 0;
	for (int parsed_idx = 0; parsed_idx < parsed_quibble_count; ++parsed_idx, ++output_idx) {
		if (parsed_idx == first_quibble_count) {
			// All the quibbles before the double-colon were output, now skip
			// to where the bottom set was defined.
			int missing_quibbles = IPAddress::IPV6_NUM_QUIBBLE - parsed_quibble_count;
			if (missing_quibbles == 0) {
				return IPAddressError(input, parameters, "Invalid double-colon, too many hex digits.");
			}
			// Advanced the output by the number of missing quibbles, they will be zero.
			output_idx += missing_quibbles;
		}

		bool is_upper;
		const idx_t bitshift = QuibbleHalfAddressBitShift(output_idx, is_upper);
		if (is_upper) {
			result.address.upper |= static_cast<uint64_t>(quibbles[parsed_idx]) << bitshift;
		} else {
			result.address.lower |= static_cast<uint64_t>(quibbles[parsed_idx]) << bitshift;
		}
	}

	return true;
}

bool IPAddress::TryParse(string_t input, IPAddress &result, CastParameters &parameters) {
	auto data = input.GetData();
	auto size = input.GetSize();
	// Start by detecting whether the string is an IPv4 or IPv6 address, or neither.
	idx_t c = 0;
	while (c < size && StringUtil::CharacterIsHex(data[c])) {
		c++;
	}
	if (c == size) {
		return IPAddressError(input, parameters, "Expected an IP address");
	}

	// IPv6 can start with a colon
	if (data[c] == ':') {
		return TryParseIPv6(input, result, parameters);
	}

	if (c == 0) {
		return IPAddressError(input, parameters, "Expected a number");
	}
	if (data[c] == '.') {
		return TryParseIPv4(input, result, parameters);
	}

	return IPAddressError(input, parameters, "Expected an IP address");
}

static string ToStringIPv4(const uhugeint_t &address, const uint8_t mask) {
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

static string ToStringIPv6(const IPAddress &addr) {
	uint16_t quibbles[IPAddress::IPV6_NUM_QUIBBLE];
	idx_t zero_run = 0;
	idx_t zero_start = 0;
	// The total number of quibbles can't be a start index, so use it to track
	// when a zero run is not in progress.
	idx_t this_zero_start = IPAddress::IPV6_NUM_QUIBBLE;

	// Convert the packed bits into quibbles while looking for the maximum run of zeros
	for (idx_t i = 0; i < IPAddress::IPV6_NUM_QUIBBLE; ++i) {
		bool is_upper;
		const idx_t bitshift = QuibbleHalfAddressBitShift(i, is_upper);
		// Operate on each half separately to make the bit operations more efficient.
		if (is_upper) {
			quibbles[i] = Hugeint::Cast<uint16_t>((addr.address.upper >> bitshift) & 0xFFFF);
		} else {
			quibbles[i] = Hugeint::Cast<uint16_t>((addr.address.lower >> bitshift) & 0xFFFF);
		}

		if (quibbles[i] == 0 && this_zero_start == IPAddress::IPV6_NUM_QUIBBLE) {
			this_zero_start = i;
		} else if (quibbles[i] != 0 && this_zero_start != IPAddress::IPV6_NUM_QUIBBLE) {
			// This is the end of the current run of zero quibbles
			idx_t this_run = i - this_zero_start;
			// Save this run if it is larger than previous runs. If it is equal,
			// the left-most should be used according to the standard, so keep
			// the previous start value. Also per the standard, do not count a
			// single zero quibble as a run.
			if (this_run > 1 && this_run > zero_run) {
				zero_run = this_run;
				zero_start = this_zero_start;
			}
			this_zero_start = IPAddress::IPV6_NUM_QUIBBLE;
		}
	}

	// Handle a zero run through the end of the address
	if (this_zero_start != IPAddress::IPV6_NUM_QUIBBLE) {
		idx_t this_run = IPAddress::IPV6_NUM_QUIBBLE - this_zero_start;
		if (this_run > 1 && this_run > zero_run) {
			zero_run = this_run;
			zero_start = this_zero_start;
		}
	}

	const idx_t zero_end = zero_start + zero_run;
	std::ostringstream result;
	result << std::hex;

	for (idx_t i = 0; i < IPAddress::IPV6_NUM_QUIBBLE; ++i) {
		if (i > 0) {
			result << ":";
		}

		if (i < zero_end && i >= zero_start) {
			// Handle the special case of the run being at the beginning
			if (i == 0) {
				result << ":";
			}
			// Adjust the index to skip past the zero quibbles
			i = zero_end - 1;

			// Handle the special case of the run being at the end
			if (i == IPAddress::IPV6_NUM_QUIBBLE - 1) {
				result << ":";
			}
		} else if (
		    // Deprecated IPv4 form with all leading zeros (except handle special case ::1)
		    (i == 6 && zero_start == 0 && zero_end == 6 && quibbles[7] != 1)
		    // Ipv4-mapped addresses: ::ffff:111.222.33.44
		    || (i == 6 && zero_start == 0 && zero_end == 5 && quibbles[5] == 0xffff)
		    // Ipv4 translated addresses: ::ffff:0:111.222.33.44
		    || (i == 6 && zero_start == 0 && zero_end == 4 && quibbles[4] == 0xffff && quibbles[5] == 0)) {
			// Pass along the lower 2 quibbles, and use the IPv4 default mask to suppress
			// ToStringIPv4 from trying to print a mask value
			result << ToStringIPv4(addr.address & 0xffffffff, IPAddress::IPV4_DEFAULT_MASK);
			break;
		} else {
			result << quibbles[i];
		}
	}

	if (addr.mask != IPAddress::IPV6_DEFAULT_MASK) {
		result << "/" << std::dec << addr.mask;
	}
	return result.str();
}

string IPAddress::ToString() const {
	if (type == IPAddressType::IP_ADDRESS_V4) {
		return ToStringIPv4(this->address, this->mask);
	}

	if (type == IPAddressType::IP_ADDRESS_V6) {
		return ToStringIPv6(*this);
	}

	throw ConversionException("Invalid IPAddress");
}

IPAddress IPAddress::FromString(string_t input) {
	IPAddress result;
	CastParameters parameters;
	auto success = TryParse(input, result, parameters);
	if (!success) {
		throw InternalException("Not successful but no exception was thrown");
	}
	return result;
}

} // namespace duckdb
