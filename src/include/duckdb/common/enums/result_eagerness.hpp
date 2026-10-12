//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/result_eagerness.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! When the retention of a result is settled. FORCED settles it on retained at submission, so
//! producers never wait for a consumer and no stream can be opened on the handle. AUTO leaves the
//! decision to the statement and the consumer
enum class ResultEagerness : uint8_t { FORCED, AUTO };

} // namespace duckdb
