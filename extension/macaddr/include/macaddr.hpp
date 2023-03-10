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

namespace duckdb {

class MACAddr {
public:
	/*
	 * This is the internal storage format for MAC addresses:
	 */
	unsigned short a;
	unsigned short b;
	unsigned short c;

public:
	MACAddr() {
		a = b = c = 0;
	}
	MACAddr(unsigned short a, unsigned short b, unsigned short c) {
		this->a = a;
		this->b = b;
		this->c = c;
	}

public:
	static bool TryParse(string_t input, MACAddr &result, string *error_message);
	static MACAddr FromString(string_t input);

	string ToString() const;
};
} // namespace duckdb
