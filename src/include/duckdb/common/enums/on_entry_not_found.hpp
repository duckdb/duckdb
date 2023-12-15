//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/on_entry_not_found.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class OnEntryNotFound : uint8_t { THROW_EXCEPTION = 0, RETURN_NULL = 1 };

} // namespace duckdb
