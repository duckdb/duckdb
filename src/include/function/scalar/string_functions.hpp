//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/string_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace re2 {
	class RE2;
}

namespace duckdb {

struct Lower {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Upper {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Concat {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct ConcatWS {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Length {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Like {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Regexp {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct Substring {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct RegexpMatchesBindData : public FunctionData {
    RegexpMatchesBindData(std::unique_ptr<re2::RE2> constant_pattern, string range_min, string range_max, bool range_success);
	~RegexpMatchesBindData();

    std::unique_ptr<re2::RE2> constant_pattern;
    string range_min, range_max;
    bool range_success;

    unique_ptr<FunctionData> Copy() override;
};

} // namespace duckdb
