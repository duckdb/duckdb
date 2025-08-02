//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/ordinality_request_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

enum class OrdinalityType : uint8_t { WITHOUT_ORDINALITY = 0, WITH_ORDINALITY = 1 };

} // namespace duckdb
