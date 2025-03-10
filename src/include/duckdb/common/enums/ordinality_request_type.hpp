//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/ordinality_request_type.hpp
//
//
//===----------------------------------------------------------------------===//


#pragma once

namespace duckdb {

enum class ordinality_request : uint32_t {
  NOT_REQUESTED = 0,
  REQUESTED = 1
};

} // namespace duckdb

