//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/compressed_materialization_origin.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class CompressedMaterializationOrigin : uint8_t { NONE, CAST, COMPRESS, DECOMPRESS };

} // namespace duckdb
