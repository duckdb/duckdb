#include "duckdb/function/scalar/comparison_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"

namespace duckdb {

void BetweenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &between_expr = state.expr.Cast<BoundBetweenExpression>();
	bool upper_inclusive = between_expr.UpperInclusive();
	bool lower_inclusive = between_expr.LowerInclusive();

	Vector intermediate1(LogicalType::BOOLEAN);
	Vector intermediate2(LogicalType::BOOLEAN);

	auto &input = args.data[0];
	auto &lower = args.data[1];
	auto &upper = args.data[2];
	auto count = args.size();

	if (upper_inclusive && lower_inclusive) {
		VectorOperations::GreaterThanEquals(input, lower, intermediate1, count);
		VectorOperations::LessThanEquals(input, upper, intermediate2, count);
	} else if (lower_inclusive) {
		VectorOperations::GreaterThanEquals(input, lower, intermediate1, count);
		VectorOperations::LessThan(input, upper, intermediate2, count);
	} else if (upper_inclusive) {
		VectorOperations::GreaterThan(input, lower, intermediate1, count);
		VectorOperations::LessThanEquals(input, upper, intermediate2, count);
	} else {
		VectorOperations::GreaterThan(input, lower, intermediate1, count);
		VectorOperations::LessThan(input, upper, intermediate2, count);
	}
	VectorOperations::And(intermediate1, intermediate2, result, count);
}

#ifndef DUCKDB_SMALLER_BINARY
struct BothInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct LowerInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

struct UpperInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct ExclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

template <class OP>
static idx_t BetweenLoopTypeSwitch(Vector &input, Vector &lower, Vector &upper, SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TernaryExecutor::Select<int8_t, int8_t, int8_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	case PhysicalType::INT16:
		return TernaryExecutor::Select<int16_t, int16_t, int16_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT32:
		return TernaryExecutor::Select<int32_t, int32_t, int32_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT64:
		return TernaryExecutor::Select<int64_t, int64_t, int64_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT128:
		return TernaryExecutor::Select<hugeint_t, hugeint_t, hugeint_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                    false_sel);
	case PhysicalType::UINT8:
		return TernaryExecutor::Select<uint8_t, uint8_t, uint8_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::UINT16:
		return TernaryExecutor::Select<uint16_t, uint16_t, uint16_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT32:
		return TernaryExecutor::Select<uint32_t, uint32_t, uint32_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT64:
		return TernaryExecutor::Select<uint64_t, uint64_t, uint64_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT128:
		return TernaryExecutor::Select<uhugeint_t, uhugeint_t, uhugeint_t, OP>(input, lower, upper, sel, count,
		                                                                       true_sel, false_sel);
	case PhysicalType::FLOAT:
		return TernaryExecutor::Select<float, float, float, OP>(input, lower, upper, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return TernaryExecutor::Select<double, double, double, OP>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	case PhysicalType::VARCHAR:
		return TernaryExecutor::Select<string_t, string_t, string_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::INTERVAL:
		return TernaryExecutor::Select<interval_t, interval_t, interval_t, OP>(input, lower, upper, sel, count,
		                                                                       true_sel, false_sel);
	default:
		throw InvalidTypeException(input.GetType(), "Invalid type for BETWEEN");
	}
}

idx_t BetweenSelect(DataChunk &args, ExpressionState &state, SelectionVector *true_sel, SelectionVector *false_sel) {
	auto &between_expr = state.expr.Cast<BoundBetweenExpression>();
	bool upper_inclusive = between_expr.UpperInclusive();
	bool lower_inclusive = between_expr.LowerInclusive();

	auto &input = args.data[0];
	auto &lower = args.data[1];
	auto &upper = args.data[2];
	auto count = args.size();

	if (upper_inclusive && lower_inclusive) {
		return BetweenLoopTypeSwitch<BothInclusiveBetweenOperator>(input, lower, upper, nullptr, count, true_sel,
		                                                           false_sel);
	} else if (lower_inclusive) {
		return BetweenLoopTypeSwitch<LowerInclusiveBetweenOperator>(input, lower, upper, nullptr, count, true_sel,
		                                                            false_sel);
	} else if (upper_inclusive) {
		return BetweenLoopTypeSwitch<UpperInclusiveBetweenOperator>(input, lower, upper, nullptr, count, true_sel,
		                                                            false_sel);
	} else {
		return BetweenLoopTypeSwitch<ExclusiveBetweenOperator>(input, lower, upper, nullptr, count, true_sel,
		                                                       false_sel);
	}
}
#endif

unique_ptr<FunctionData> BindBetweenFun(BindScalarFunctionInput &input) {
	throw InvalidInputException("Between function cannot be called directly");
}

string BetweenToString(FunctionToStringInput &input) {
	return BetweenExpression::ToString(*input.children[0], *input.children[1], *input.children[2]);
}

ExpressionType BetweenGetExpressionType(FunctionToStringInput &input) {
	return ExpressionType::COMPARE_BETWEEN;
}

ScalarFunction BetweenFun::GetFunction() {
	ScalarFunction between_fun("__between", {LogicalType::ANY, LogicalType::ANY, LogicalType::ANY},
	                           LogicalType::BOOLEAN, BetweenFunction, BindBetweenFun);
	between_fun.SetToStringCallback(BetweenToString);
	between_fun.SetGetExpressionTypeCallback(BetweenGetExpressionType);
#ifndef DUCKDB_SMALLER_BINARY
	between_fun.SetSelectCallback(BetweenSelect);
#endif
	return between_fun;
}

} // namespace duckdb
