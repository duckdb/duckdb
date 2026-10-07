//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/view_security_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! The security type of a view - secure views act as an optimization barrier
enum class ViewSecurityType : uint8_t { REGULAR_VIEW = 0, SECURE_VIEW = 1 };

} // namespace duckdb
