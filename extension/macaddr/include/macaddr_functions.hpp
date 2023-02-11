//===----------------------------------------------------------------------===//
//                         DuckDB
//
// inet_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "macaddr.hpp"

namespace duckdb {

struct MACAddrFunctions {
	static bool CastVarcharToMAC(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static bool CastMACToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
};

} // namespace duckdb
