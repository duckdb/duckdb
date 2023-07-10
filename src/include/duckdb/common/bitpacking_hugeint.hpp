//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bitpacking_hugeint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bitpacking.hpp"

namespace duckdb {

struct HugeIntPacker {
	static void Pack(const hugeint_t *__restrict in, uint32_t *__restrict out, bitpacking_width_t width);
	static void Unpack(const uint32_t *__restrict in, hugeint_t *__restrict out, bitpacking_width_t width);
};


} // namespace hugeint
