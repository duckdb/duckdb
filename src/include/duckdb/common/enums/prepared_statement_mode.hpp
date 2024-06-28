//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/prepared_statement_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class PreparedStatementMode : uint8_t {
	PREPARE_ONLY,
	PREPARE_AND_EXECUTE,
};

} // namespace duckdb
