//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sql_identifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include <ostream>

namespace duckdb {

class String;

// Helper class to support custom overloading
// Escaping " and quoting the value with "
class SQLIdentifier {
public:
	explicit SQLIdentifier(const string &raw_string) : raw_string(raw_string) {
	}

	//! Emits an optionally quoted identifier including required escapes (i.e. ident -> ident, table -> "table")
	static string ToString(const string &identifier);

public:
	string raw_string;
};

inline string operator+(const SQLIdentifier &lhs, const string &rhs) { return SQLIdentifier::ToString(lhs.raw_string) + rhs; }
inline string operator+(const SQLIdentifier &lhs, const char *rhs) { return SQLIdentifier::ToString(lhs.raw_string) + rhs; }
inline string operator+(const string &lhs, const SQLIdentifier &rhs) { return lhs + SQLIdentifier::ToString(rhs.raw_string); }
inline string operator+(const char *lhs, const SQLIdentifier &rhs) { return lhs + SQLIdentifier::ToString(rhs.raw_string); }
inline string &operator+=(string &lhs, const SQLIdentifier &rhs) { return lhs += SQLIdentifier::ToString(rhs.raw_string); }
inline std::ostream &operator<<(std::ostream &os, const SQLIdentifier &val) { return os << SQLIdentifier::ToString(val.raw_string); }

// Helper class to support custom overloading
// Escaping ' and quoting the value with '
class SQLString {
public:
	explicit SQLString(const string &raw_string) : raw_string(raw_string) {
	}

	//! Emits a quoted string literal including required escapes (i.e. ident -> 'ident')
	static string ToString(const string &literal);

public:
	string raw_string;
};

inline string operator+(const SQLString &lhs, const string &rhs) { return SQLString::ToString(lhs.raw_string) + rhs; }
inline string operator+(const SQLString &lhs, const char *rhs) { return SQLString::ToString(lhs.raw_string) + rhs; }
inline string operator+(const string &lhs, const SQLString &rhs) { return lhs + SQLString::ToString(rhs.raw_string); }
inline string operator+(const char *lhs, const SQLString &rhs) { return lhs + SQLString::ToString(rhs.raw_string); }
inline string &operator+=(string &lhs, const SQLString &rhs) { return lhs += SQLString::ToString(rhs.raw_string); }
inline std::ostream &operator<<(std::ostream &os, const SQLString &val) { return os << SQLString::ToString(val.raw_string); }

} // namespace duckdb
