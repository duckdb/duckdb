//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/varint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
//! The Varint class is a static class that holds helper functions for the Varint
//! type.
class Varint {
public:
	DUCKDB_API static void Verify(const string_t &input);
};
} // namespace duckdb
