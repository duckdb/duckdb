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
	static void Family(DataChunk &args, ExpressionState &state, Vector &result);
	static void Netmask(DataChunk &args, ExpressionState &state, Vector &result);
	static void Network(DataChunk &args, ExpressionState &state, Vector &result);
	static void Broadcast(DataChunk &args, ExpressionState &state, Vector &result);
	static void Escape(DataChunk &args, ExpressionState &state, Vector &result);
	static void Unescape(DataChunk &args, ExpressionState &state, Vector &result);
	static void Subtract(DataChunk &args, ExpressionState &state, Vector &result);
	static void Add(DataChunk &args, ExpressionState &state, Vector &result);
	static void ContainsLeft(DataChunk &args, ExpressionState &state, Vector &result);
	static void ContainsRight(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
