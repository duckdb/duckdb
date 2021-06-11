#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include <cmath>

namespace duckdb {

struct NextAfterBindData : public FunctionData {
	explicit NextAfterBindData(Value &approximated_to_p) : approximated_to(approximated_to_p) {
	}
	Vector approximated_to;
};

struct NextAfterOperatorDouble {
	template <class T>
	static inline T Operation(T input, T approximate_to) {
		return nextafter(input, approximate_to);
	}
};

struct NextAfterOperatorFloat {
	template <class T>
	static inline T Operation(T input, T approximate_to) {
		return nextafterf(input, approximate_to);
	}
};

template <class T>
static void NextAfterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (NextAfterBindData &)*func_expr.bind_info;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	if (args.GetTypes()[0] == LogicalType::DOUBLE) {
		BinaryExecutor::Execute<T, T, T, NextAfterOperatorDouble>(args.data[0], info.approximated_to, result,
		                                                          args.size());
	} else if (args.GetTypes()[0] == LogicalType::FLOAT) {
		BinaryExecutor::Execute<T, T, T, NextAfterOperatorFloat>(args.data[0], info.approximated_to, result,
		                                                         args.size());
	} else {
		throw NotImplementedException("Unimplemented type for NextAfter Function");
	}
}

unique_ptr<FunctionData> BindNextAfter(ClientContext &context, ScalarFunction &function,
                                       vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("Next After approximation variable must be a constant parameter");
	}
	Value approx_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	if (approx_val.type() != LogicalType::FLOAT && approx_val.type() != LogicalType::DOUBLE) {
		throw NotImplementedException("Unimplemented type for NextAfter Function");
	}
	arguments.pop_back();
	return make_unique<NextAfterBindData>(approx_val);
}

void NextAfterFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet next_after_fun("nextafter");
	next_after_fun.AddFunction(ScalarFunction("nextafter", {LogicalType::DOUBLE, LogicalType::DOUBLE},
	                                          LogicalType::DOUBLE, NextAfterFunction<double>, false, BindNextAfter));
	next_after_fun.AddFunction(ScalarFunction("nextafter", {LogicalType::FLOAT, LogicalType::FLOAT}, LogicalType::FLOAT,
	                                          NextAfterFunction<float>, false, BindNextAfter));
	set.AddFunction(next_after_fun);
}
} // namespace duckdb
