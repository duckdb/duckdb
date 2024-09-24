//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/date_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {
class BoundFunctionExpression;

struct StrfTimeFun {
  static ScalarFunctionSet GetFunctions();

  static void RegisterFunction(BuiltinFunctions &set);
};

struct StrpTimeFun {
  static ScalarFunctionSet GetFunctions();

  static void RegisterFunction(BuiltinFunctions &set);
};

struct TryStrpTimeFun {
  static ScalarFunctionSet GetFunctions();

  static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
