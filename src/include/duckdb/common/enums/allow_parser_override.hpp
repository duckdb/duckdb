//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/allow_parser_override.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class AllowParserOverride : uint8_t {
	DEFAULT_OVERRIDE,
	FALLBACK_OVERRIDE,
	STRICT_OVERRIDE,
	STRICT_WHEN_SUPPORTED
};

} // namespace duckdb
