//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/string_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

#include <string>

namespace duckdb {
/**
 * Hex Utility Functions
 * Note that these are not the most efficient implementations (i.e., they copy
 * memory) and therefore they should only be used for debug messages and other
 * such things.
 */
class BinaryUtil {
public:
	static void ToBase16(char *in, char *out, size_t len);
};
} // namespace duckdb
