//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/debug_statement_verification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DebugStatementVerification : uint8_t {
	NONE,
	COPY_STATEMENT,
	REPARSE_STATEMENT,
	SERIALIZE_STATEMENT,
	PREPARED_STATEMENT
};

} // namespace duckdb
