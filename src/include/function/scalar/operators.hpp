//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct Add {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Subtract {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Multiply {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Divide {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Mod {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct LeftShift {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct RightShift {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseAnd {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseOr {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseXor {
    static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb

