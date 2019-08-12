//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/nextval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct Nextval {
    static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
