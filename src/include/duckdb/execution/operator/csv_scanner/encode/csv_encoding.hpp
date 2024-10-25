
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/encode/csv_encoding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

namespace duckdb {

enum class CSVEncoding : uint8_t { UTF_8 = 0, UTF_16 = 1, LATIN_1 = 2 };

}
