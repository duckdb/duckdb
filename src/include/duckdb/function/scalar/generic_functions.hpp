//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/generic_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {
class BoundFunctionExpression;

struct AliasFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LeastFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct GreatestFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StatsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct TypeOfFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ConstantOrNull {
	static ScalarFunction GetFunction(LogicalType return_type);
	static unique_ptr<FunctionData> Bind(Value value);
	static bool IsConstantOrNull(BoundFunctionExpression &expr, const Value &val);
};

struct CurrentSettingFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SystemFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ExportAggregateFunctionBindData : public FunctionData {
	AggregateFunction &function;

	explicit ExportAggregateFunctionBindData(AggregateFunction &function_p) : function(function_p) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<ExportAggregateFunctionBindData>(function);
	}
};

struct ExportAggregateFunction {
	static AggregateFunction GetFunction(LogicalType &return_type, AggregateFunction &bound_function);
};

} // namespace duckdb
