//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/dialect_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"

namespace duckdb {
struct DBConfig;

//! A named SQL dialect that can customize the PEG parser.
class DialectExtension {
public:
	explicit DialectExtension(Identifier name_p) : name(std::move(name_p)) {
	}

	Identifier name;

	static void Register(DBConfig &config, DialectExtension extension);
};

} // namespace duckdb
