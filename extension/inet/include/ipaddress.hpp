//===----------------------------------------------------------------------===//
//                         DuckDB
//
// ipaddress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/uhugeint.hpp"

namespace duckdb {
struct CastParameters;

enum class IPAddressType : uint8_t { IP_ADDRESS_INVALID = 0, IP_ADDRESS_V4 = 1, IP_ADDRESS_V6 = 2 };

class IPAddress {
public:
	constexpr static const int32_t IPV4_DEFAULT_MASK = 32;
	constexpr static const int32_t IPV6_DEFAULT_MASK = 128;
	constexpr static const int32_t IPV6_QUIBBLE_BITS = 16;
	constexpr static const int32_t IPV6_NUM_QUIBBLE = 8;

public:
	IPAddress();
	IPAddress(IPAddressType type, uhugeint_t address, uint16_t mask);

	IPAddressType type;
	uhugeint_t address;
	uint16_t mask;

public:
	static IPAddress FromIPv4(int32_t address, uint16_t mask);
	static IPAddress FromIPv6(uhugeint_t address, uint16_t mask);
	static bool TryParse(string_t input, IPAddress &result, CastParameters &parameters);
	static IPAddress FromString(string_t input);

	string ToString() const;
	IPAddress Netmask() const;
	IPAddress Network() const;
	IPAddress Broadcast() const;
};
} // namespace duckdb
