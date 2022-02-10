//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/geometry_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct MakePointFun {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct GeometryFromText {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct GeometryFromWKB {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct GeometryAsText {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct GeometryGetX {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct GeometryCentroid {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct GeometryDistance {
    static void RegisterFunction(BuiltinFunctions &set);
};

}