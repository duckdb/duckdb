//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/trigonometric_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct Sin {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Cos {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Tan {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Asin {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Acos {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Atan {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Cot {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Atan2 {
    static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
