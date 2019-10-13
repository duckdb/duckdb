//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/date_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct Age {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct DatePart {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Year {
    static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
