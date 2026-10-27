//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/open_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ExtendedOpenFileInfo {
	unordered_map<string, Value> options;

	//! The option that names the file secret to open the file with (SecretManager::RegisterFileSecret)
	static constexpr const char *FILE_SECRET_OPTION = "secret";

public:
	//! Read an option as a T - returns false when the option is not set, throws when it is set to a value
	//! that cannot be read as a T. Only the types specialized below are supported
	template <class T>
	bool TryGetOption(const string &name, T &result) const;
	//! The name of the file secret to open the file with - false when the file names none
	DUCKDB_API bool TryGetFileSecret(string &name) const;
};

//! A boolean option - anything that casts to BOOLEAN is accepted
template <>
DUCKDB_API bool ExtendedOpenFileInfo::TryGetOption(const string &name, bool &result) const;
//! A string option - VARCHAR and BLOB are read back as-is, other values as their text representation
template <>
DUCKDB_API bool ExtendedOpenFileInfo::TryGetOption(const string &name, string &result) const;
//! An unsigned integer option - anything that casts to UBIGINT is accepted
template <>
DUCKDB_API bool ExtendedOpenFileInfo::TryGetOption(const string &name, idx_t &result) const;
//! A timestamp option - anything that casts to TIMESTAMP is accepted
template <>
DUCKDB_API bool ExtendedOpenFileInfo::TryGetOption(const string &name, timestamp_t &result) const;

struct OpenFileInfo {
	OpenFileInfo() = default;
	OpenFileInfo(string path_p) // NOLINT: allow implicit conversion from string
	    : path(std::move(path_p)) {
	}

	string path;
	shared_ptr<ExtendedOpenFileInfo> extended_info;

	//! The field of a file struct that holds the path of the file
	static constexpr const char *PATH_FIELD = "filename";

public:
	//! The file a value specifies: a VARCHAR path, or a STRUCT holding the path in its "filename" field and the
	//! options to open the file with in its other fields (a VARIANT holding either is unpacked). Errors name the
	//! function that was given the file
	DUCKDB_API static OpenFileInfo FromValue(const Value &input, const Identifier &function_name);
	//! The inverse of FromValue: the path as a VARCHAR when there are no options, a file struct otherwise
	DUCKDB_API Value ToValue() const;
	//! Whether the file is opened with a file secret. Reads of such a file use no cache that reads of its path share
	bool HasFileSecret() const {
		return extended_info && extended_info->options.count(ExtendedOpenFileInfo::FILE_SECRET_OPTION) > 0;
	}
	//! The name of the file secret the file is opened with - "" when it has none
	DUCKDB_API string GetFileSecret() const;
	//! This file with the file secret `name` when it has none. The extended information is copied, because copies of
	//! an OpenFileInfo share it
	DUCKDB_API OpenFileInfo WithFileSecret(const string &name) const;

	bool operator<(const OpenFileInfo &rhs) const {
		return path < rhs.path;
	}
};

} // namespace duckdb
