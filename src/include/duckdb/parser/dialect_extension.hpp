//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/dialect_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
struct DBConfig;

//! A named SQL dialect that can customize the PEG parser.
class DialectExtension {
public:
	explicit DialectExtension(string name_p) : name(std::move(name_p)) {
	}

	string name;

	static void Register(DBConfig &config, DialectExtension extension);
};

} // namespace duckdb
