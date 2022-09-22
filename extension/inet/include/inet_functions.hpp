//===----------------------------------------------------------------------===//
//                         DuckDB
//
// inet_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "ipaddress.hpp"

namespace duckdb {

struct INetFunctions {
	static bool CastVarcharToINET(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static bool CastINETToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

	static void Host(DataChunk &args, ExpressionState &state, Vector &result);
	static void Subtract(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
