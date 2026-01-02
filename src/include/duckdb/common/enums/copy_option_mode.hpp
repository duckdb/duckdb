//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/copy_option_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class CopyOptionMode { WRITE_ONLY, READ_ONLY, READ_WRITE };

} // namespace duckdb
