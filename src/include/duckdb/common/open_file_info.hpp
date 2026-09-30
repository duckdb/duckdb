//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/open_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ExtendedOpenFileInfo {
	unordered_map<string, Value> options;

public:
	//! Set an option to a value that originates from the user, casting it to the type the option is read back
	//! as and storing it under its canonical name. Throws when the value cannot be converted to that type.
	//! Options the core does not know about are stored as-is, for extensions to interpret
	DUCKDB_API void SetUserOption(const string &name, const Value &value);
	//! Read an option as a T - returns false when the option is not set, throws when it is set to a value
	//! that cannot be read as a T. Only the types specialized below are supported
	template <class T>
	bool TryGetOption(const string &name, T &result) const;
};

//! A boolean option - anything that casts to BOOLEAN is accepted
template <>
DUCKDB_API bool ExtendedOpenFileInfo::TryGetOption(const string &name, bool &result) const;
//! A string option - VARCHAR and BLOB are both stored as a string and both are accepted
template <>
DUCKDB_API bool ExtendedOpenFileInfo::TryGetOption(const string &name, string &result) const;
//! An unsigned integer option - anything that casts to UBIGINT is accepted
template <>
DUCKDB_API bool ExtendedOpenFileInfo::TryGetOption(const string &name, idx_t &result) const;

struct OpenFileInfo {
	OpenFileInfo() = default;
	OpenFileInfo(string path_p) // NOLINT: allow implicit conversion from string
	    : path(std::move(path_p)) {
	}

	string path;
	shared_ptr<ExtendedOpenFileInfo> extended_info;

public:
	bool operator<(const OpenFileInfo &rhs) const {
		return path < rhs.path;
	}
};

} // namespace duckdb
