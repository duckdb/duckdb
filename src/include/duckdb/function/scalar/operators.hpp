//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/arch.h"

namespace duckdb {

struct SET_ARCH(AddFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(SubtractFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(MultiplyFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(DivideFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(ModFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(LeftShiftFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(RightShiftFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(BitwiseAndFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(BitwiseOrFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(BitwiseXorFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SET_ARCH(BitwiseNotFun) {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
