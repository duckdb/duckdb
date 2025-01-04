#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/operator/abs.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "core_functions/scalar/math_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <cmath>
#include <cstdint>
#include <errno.h>
#include <limits>
#include <type_traits>

namespace duckdb {

template <class TR, class OP>
static scalar_function_t GetScalarIntegerUnaryFunctionFixedReturn(const LogicalType &type) {
	scalar_function_t function;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		function = &ScalarFunction::UnaryFunction<int8_t, TR, OP>;
		break;
	case LogicalTypeId::SMALLINT:
		function = &ScalarFunction::UnaryFunction<int16_t, TR, OP>;
		break;
	case LogicalTypeId::INTEGER:
		function = &ScalarFunction::UnaryFunction<int32_t, TR, OP>;
		break;
	case LogicalTypeId::BIGINT:
		function = &ScalarFunction::UnaryFunction<int64_t, TR, OP>;
		break;
	case LogicalTypeId::HUGEINT:
		function = &ScalarFunction::UnaryFunction<hugeint_t, TR, OP>;
		break;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarIntegerUnaryFunctionFixedReturn");
	}
	return function;
}

//===--------------------------------------------------------------------===//
// nextafter
//===--------------------------------------------------------------------===//
struct NextAfterOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA base, TB exponent) {
		throw NotImplementedException("Unimplemented type for NextAfter Function");
	}

	template <class TA, class TB, class TR>
	static inline double Operation(double input, double approximate_to) {
		return nextafter(input, approximate_to);
	}
	template <class TA, class TB, class TR>
	static inline float Operation(float input, float approximate_to) {
		return nextafterf(input, approximate_to);
	}
};

ScalarFunctionSet NextAfterFun::GetFunctions() {
	ScalarFunctionSet next_after_fun;
	next_after_fun.AddFunction(
	    ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                   ScalarFunction::BinaryFunction<double, double, double, NextAfterOperator>));
	next_after_fun.AddFunction(ScalarFunction({LogicalType::FLOAT, LogicalType::FLOAT}, LogicalType::FLOAT,
	                                          ScalarFunction::BinaryFunction<float, float, float, NextAfterOperator>));
	return next_after_fun;
}

//===--------------------------------------------------------------------===//
// abs
//===--------------------------------------------------------------------===//
static unique_ptr<BaseStatistics> PropagateAbsStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 1);
	// can only propagate stats if the children have stats
	auto &lstats = child_stats[0];
	Value new_min, new_max;
	bool potential_overflow = true;
	if (NumericStats::HasMinMax(lstats)) {
		switch (expr.return_type.InternalType()) {
		case PhysicalType::INT8:
			potential_overflow = NumericStats::Min(lstats).GetValue<int8_t>() == NumericLimits<int8_t>::Minimum();
			break;
		case PhysicalType::INT16:
			potential_overflow = NumericStats::Min(lstats).GetValue<int16_t>() == NumericLimits<int16_t>::Minimum();
			break;
		case PhysicalType::INT32:
			potential_overflow = NumericStats::Min(lstats).GetValue<int32_t>() == NumericLimits<int32_t>::Minimum();
			break;
		case PhysicalType::INT64:
			potential_overflow = NumericStats::Min(lstats).GetValue<int64_t>() == NumericLimits<int64_t>::Minimum();
			break;
		default:
			return nullptr;
		}
	}
	if (potential_overflow) {
		new_min = Value(expr.return_type);
		new_max = Value(expr.return_type);
	} else {
		// no potential overflow

		// compute stats
		auto current_min = NumericStats::Min(lstats).GetValue<int64_t>();
		auto current_max = NumericStats::Max(lstats).GetValue<int64_t>();

		int64_t min_val, max_val;

		if (current_min < 0 && current_max < 0) {
			// if both min and max are below zero, then min=abs(cur_max) and max=abs(cur_min)
			min_val = AbsValue(current_max);
			max_val = AbsValue(current_min);
		} else if (current_min < 0) {
			D_ASSERT(current_max >= 0);
			// if min is below zero and max is above 0, then min=0 and max=max(cur_max, abs(cur_min))
			min_val = 0;
			max_val = MaxValue(AbsValue(current_min), current_max);
		} else {
			// if both current_min and current_max are > 0, then the abs is a no-op and can be removed entirely
			*input.expr_ptr = std::move(input.expr.children[0]);
			return child_stats[0].ToUnique();
		}
		new_min = Value::Numeric(expr.return_type, min_val);
		new_max = Value::Numeric(expr.return_type, max_val);
		expr.function.function = ScalarFunction::GetScalarUnaryFunction<AbsOperator>(expr.return_type);
	}
	auto stats = NumericStats::CreateEmpty(expr.return_type);
	NumericStats::SetMin(stats, new_min);
	NumericStats::SetMax(stats, new_max);
	stats.CopyValidity(lstats);
	return stats.ToUnique();
}

template <class OP>
unique_ptr<FunctionData> DecimalUnaryOpBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::SMALLINT);
		break;
	case PhysicalType::INT32:
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::INTEGER);
		break;
	case PhysicalType::INT64:
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::BIGINT);
		break;
	default:
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::HUGEINT);
		break;
	}
	bound_function.arguments[0] = decimal_type;
	bound_function.return_type = decimal_type;
	return nullptr;
}

ScalarFunctionSet AbsOperatorFun::GetFunctions() {
	ScalarFunctionSet abs;
	for (auto &type : LogicalType::Numeric()) {
		switch (type.id()) {
		case LogicalTypeId::DECIMAL:
			abs.AddFunction(ScalarFunction({type}, type, nullptr, DecimalUnaryOpBind<AbsOperator>));
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT: {
			ScalarFunction function({type}, type, ScalarFunction::GetScalarUnaryFunction<TryAbsOperator>(type));
			function.statistics = PropagateAbsStats;
			abs.AddFunction(function);
			break;
		}
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			abs.AddFunction(ScalarFunction({type}, type, ScalarFunction::NopFunction));
			break;
		default:
			abs.AddFunction(ScalarFunction({type}, type, ScalarFunction::GetScalarUnaryFunction<AbsOperator>(type)));
			break;
		}
	}
	for (auto &func : abs.functions) {
		BaseScalarFunction::SetReturnsError(func);
	}
	return abs;
}

//===--------------------------------------------------------------------===//
// bit_count
//===--------------------------------------------------------------------===//
struct BitCntOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		using TU = typename std::make_unsigned<TA>::type;
		TR count = 0;
		for (auto value = TU(input); value; ++count) {
			value &= (value - 1);
		}
		return count;
	}
};

struct HugeIntBitCntOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		using TU = typename std::make_unsigned<int64_t>::type;
		TR count = 0;

		for (auto value = TU(input.upper); value; ++count) {
			value &= (value - 1);
		}
		for (auto value = TU(input.lower); value; ++count) {
			value &= (value - 1);
		}
		return count;
	}
};

struct BitStringBitCntOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		TR count = Bit::BitCount(input);
		return count;
	}
};

ScalarFunctionSet BitCountFun::GetFunctions() {
	ScalarFunctionSet functions;
	functions.AddFunction(ScalarFunction({LogicalType::TINYINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int8_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::SMALLINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int16_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::INTEGER}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int32_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int64_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<hugeint_t, int8_t, HugeIntBitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIGINT,
	                                     ScalarFunction::UnaryFunction<string_t, idx_t, BitStringBitCntOperator>));
	return functions;
}

//===--------------------------------------------------------------------===//
// sign
//===--------------------------------------------------------------------===//
struct SignOperator {
	template <class TA, class TR>
	static TR Operation(TA input) {
		if (input == TA(0)) {
			return 0;
		} else if (input > TA(0)) {
			return 1;
		} else {
			return -1;
		}
	}
};

template <>
int8_t SignOperator::Operation(float input) {
	if (input == 0 || Value::IsNan(input)) {
		return 0;
	} else if (input > 0) {
		return 1;
	} else {
		return -1;
	}
}

template <>
int8_t SignOperator::Operation(double input) {
	if (input == 0 || Value::IsNan(input)) {
		return 0;
	} else if (input > 0) {
		return 1;
	} else {
		return -1;
	}
}

ScalarFunctionSet SignFun::GetFunctions() {
	ScalarFunctionSet sign;
	for (auto &type : LogicalType::Numeric()) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			continue;
		} else {
			sign.AddFunction(
			    ScalarFunction({type}, LogicalType::TINYINT,
			                   ScalarFunction::GetScalarUnaryFunctionFixedReturn<int8_t, SignOperator>(type)));
		}
	}
	return sign;
}

//===--------------------------------------------------------------------===//
// ceil
//===--------------------------------------------------------------------===//
struct CeilOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::ceil(left);
	}
};

template <class T, class POWERS_OF_TEN, class OP>
static void GenericRoundFunctionDecimal(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	OP::template Operation<T, POWERS_OF_TEN>(input, DecimalType::GetScale(func_expr.children[0]->return_type), result);
}

template <class OP>
unique_ptr<FunctionData> BindGenericRoundFunctionDecimal(ClientContext &context, ScalarFunction &bound_function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	// ceil essentially removes the scale
	auto &decimal_type = arguments[0]->return_type;
	auto scale = DecimalType::GetScale(decimal_type);
	auto width = DecimalType::GetWidth(decimal_type);
	if (scale == 0) {
		bound_function.function = ScalarFunction::NopFunction;
	} else {
		switch (decimal_type.InternalType()) {
		case PhysicalType::INT16:
			bound_function.function = GenericRoundFunctionDecimal<int16_t, NumericHelper, OP>;
			break;
		case PhysicalType::INT32:
			bound_function.function = GenericRoundFunctionDecimal<int32_t, NumericHelper, OP>;
			break;
		case PhysicalType::INT64:
			bound_function.function = GenericRoundFunctionDecimal<int64_t, NumericHelper, OP>;
			break;
		default:
			bound_function.function = GenericRoundFunctionDecimal<hugeint_t, Hugeint, OP>;
			break;
		}
	}
	bound_function.arguments[0] = decimal_type;
	bound_function.return_type = LogicalType::DECIMAL(width, 0);
	return nullptr;
}

struct CeilDecimalOperator {
	template <class T, class POWERS_OF_TEN_CLASS>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = UnsafeNumericCast<T>(POWERS_OF_TEN_CLASS::POWERS_OF_TEN[scale]);
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input <= 0) {
				// below 0 we floor the number (e.g. -10.5 -> -10)
				return UnsafeNumericCast<T>(input / power_of_ten);
			} else {
				// above 0 we ceil the number
				return UnsafeNumericCast<T>(((input - 1) / power_of_ten) + 1);
			}
		});
	}
};

ScalarFunctionSet CeilFun::GetFunctions() {
	ScalarFunctionSet ceil;
	for (auto &type : LogicalType::Numeric()) {
		scalar_function_t func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		if (type.IsIntegral()) {
			// no ceil for integral numbers
			continue;
		}
		switch (type.id()) {
		case LogicalTypeId::FLOAT:
			func = ScalarFunction::UnaryFunction<float, float, CeilOperator>;
			break;
		case LogicalTypeId::DOUBLE:
			func = ScalarFunction::UnaryFunction<double, double, CeilOperator>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = BindGenericRoundFunctionDecimal<CeilDecimalOperator>;
			break;
		default:
			throw InternalException("Unimplemented numeric type for function \"ceil\"");
		}
		ceil.AddFunction(ScalarFunction({type}, type, func, bind_func));
	}
	return ceil;
}

//===--------------------------------------------------------------------===//
// floor
//===--------------------------------------------------------------------===//
struct FloorOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::floor(left);
	}
};

struct FloorDecimalOperator {
	template <class T, class POWERS_OF_TEN_CLASS>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = UnsafeNumericCast<T>(POWERS_OF_TEN_CLASS::POWERS_OF_TEN[scale]);
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input < 0) {
				// below 0 we ceil the number (e.g. -10.5 -> -11)
				return UnsafeNumericCast<T>(((input + 1) / power_of_ten) - 1);
			} else {
				// above 0 we floor the number
				return UnsafeNumericCast<T>(input / power_of_ten);
			}
		});
	}
};

ScalarFunctionSet FloorFun::GetFunctions() {
	ScalarFunctionSet floor;
	for (auto &type : LogicalType::Numeric()) {
		scalar_function_t func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		if (type.IsIntegral()) {
			// no floor for integral numbers
			continue;
		}
		switch (type.id()) {
		case LogicalTypeId::FLOAT:
			func = ScalarFunction::UnaryFunction<float, float, FloorOperator>;
			break;
		case LogicalTypeId::DOUBLE:
			func = ScalarFunction::UnaryFunction<double, double, FloorOperator>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = BindGenericRoundFunctionDecimal<FloorDecimalOperator>;
			break;
		default:
			throw InternalException("Unimplemented numeric type for function \"floor\"");
		}
		floor.AddFunction(ScalarFunction({type}, type, func, bind_func));
	}
	return floor;
}

//===--------------------------------------------------------------------===//
// trunc
//===--------------------------------------------------------------------===//
struct TruncOperator {
	// Integer truncation is a NOP
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::trunc(left);
	}
};

struct TruncDecimalOperator {
	template <class T, class POWERS_OF_TEN_CLASS>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = UnsafeNumericCast<T>(POWERS_OF_TEN_CLASS::POWERS_OF_TEN[scale]);
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			//	Always floor
			return UnsafeNumericCast<T>((input / power_of_ten));
		});
	}
};

ScalarFunctionSet TruncFun::GetFunctions() {
	ScalarFunctionSet trunc;
	for (auto &type : LogicalType::Numeric()) {
		scalar_function_t func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		//	Truncation of integers gets generated by some tools (e.g., Tableau/JDBC:Postgres)
		switch (type.id()) {
		case LogicalTypeId::FLOAT:
			func = ScalarFunction::UnaryFunction<float, float, TruncOperator>;
			break;
		case LogicalTypeId::DOUBLE:
			func = ScalarFunction::UnaryFunction<double, double, TruncOperator>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = BindGenericRoundFunctionDecimal<TruncDecimalOperator>;
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::UHUGEINT:
			func = ScalarFunction::NopFunction;
			break;
		default:
			throw InternalException("Unimplemented numeric type for function \"trunc\"");
		}
		trunc.AddFunction(ScalarFunction({type}, type, func, bind_func));
	}
	return trunc;
}

//===--------------------------------------------------------------------===//
// round
//===--------------------------------------------------------------------===//
struct RoundOperatorPrecision {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB precision) {
		double rounded_value;
		if (precision < 0) {
			double modifier = std::pow(10, -TA(precision));
			rounded_value = (std::round(input / modifier)) * modifier;
			if (std::isinf(rounded_value) || std::isnan(rounded_value)) {
				return 0;
			}
		} else {
			double modifier = std::pow(10, TA(precision));
			rounded_value = (std::round(input * modifier)) / modifier;
			if (std::isinf(rounded_value) || std::isnan(rounded_value)) {
				return input;
			}
		}
		return LossyNumericCast<TR>(rounded_value);
	}
};

struct RoundOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		double rounded_value = round(input);
		if (std::isinf(rounded_value) || std::isnan(rounded_value)) {
			return input;
		}
		return LossyNumericCast<TR>(rounded_value);
	}
};

struct RoundDecimalOperator {
	template <class T, class POWERS_OF_TEN_CLASS>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = UnsafeNumericCast<T>(POWERS_OF_TEN_CLASS::POWERS_OF_TEN[scale]);
		T addition = power_of_ten / 2;
		// regular round rounds towards the nearest number
		// in case of a tie we round away from zero
		// i.e. -10.5 -> -11, 10.5 -> 11
		// we implement this by adding (positive) or subtracting (negative) 0.5
		// and then flooring the number
		// e.g. 10.5 + 0.5 = 11, floor(11) = 11
		//      10.4 + 0.5 = 10.9, floor(10.9) = 10
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input < 0) {
				input -= addition;
			} else {
				input += addition;
			}
			return UnsafeNumericCast<T>(input / power_of_ten);
		});
	}
};

struct RoundPrecisionFunctionData : public FunctionData {
	explicit RoundPrecisionFunctionData(int32_t target_scale) : target_scale(target_scale) {
	}

	int32_t target_scale;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RoundPrecisionFunctionData>(target_scale);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RoundPrecisionFunctionData>();
		return target_scale == other.target_scale;
	}
};

template <class T, class POWERS_OF_TEN_CLASS>
static void DecimalRoundNegativePrecisionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RoundPrecisionFunctionData>();
	auto source_scale = DecimalType::GetScale(func_expr.children[0]->return_type);
	auto width = DecimalType::GetWidth(func_expr.children[0]->return_type);
	if (info.target_scale <= -int32_t(width - source_scale)) {
		// scale too big for width
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		result.SetValue(0, Value::INTEGER(0));
		return;
	}
	T divide_power_of_ten = UnsafeNumericCast<T>(POWERS_OF_TEN_CLASS::POWERS_OF_TEN[-info.target_scale + source_scale]);
	T multiply_power_of_ten = UnsafeNumericCast<T>(POWERS_OF_TEN_CLASS::POWERS_OF_TEN[-info.target_scale]);
	T addition = divide_power_of_ten / 2;

	UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
		if (input < 0) {
			input -= addition;
		} else {
			input += addition;
		}
		return UnsafeNumericCast<T>(input / divide_power_of_ten * multiply_power_of_ten);
	});
}

template <class T, class POWERS_OF_TEN_CLASS>
static void DecimalRoundPositivePrecisionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RoundPrecisionFunctionData>();
	auto source_scale = DecimalType::GetScale(func_expr.children[0]->return_type);
	T power_of_ten = UnsafeNumericCast<T>(POWERS_OF_TEN_CLASS::POWERS_OF_TEN[source_scale - info.target_scale]);
	T addition = power_of_ten / 2;
	UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
		if (input < 0) {
			input -= addition;
		} else {
			input += addition;
		}
		return UnsafeNumericCast<T>(input / power_of_ten);
	});
}

unique_ptr<FunctionData> BindDecimalRoundPrecision(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	auto &decimal_type = arguments[0]->return_type;
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw NotImplementedException("ROUND(DECIMAL, INTEGER) with non-constant precision is not supported");
	}
	Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]).DefaultCastAs(LogicalType::INTEGER);
	if (val.IsNull()) {
		throw NotImplementedException("ROUND(DECIMAL, INTEGER) with non-constant precision is not supported");
	}
	// our new precision becomes the round value
	// e.g. ROUND(DECIMAL(18,3), 1) -> DECIMAL(18,1)
	// but ONLY if the round value is positive
	// if it is negative the scale becomes zero
	// i.e. ROUND(DECIMAL(18,3), -1) -> DECIMAL(18,0)
	int32_t round_value = IntegerValue::Get(val);
	uint8_t target_scale;
	auto width = DecimalType::GetWidth(decimal_type);
	auto scale = DecimalType::GetScale(decimal_type);
	if (round_value < 0) {
		target_scale = 0;
		switch (decimal_type.InternalType()) {
		case PhysicalType::INT16:
			bound_function.function = DecimalRoundNegativePrecisionFunction<int16_t, NumericHelper>;
			break;
		case PhysicalType::INT32:
			bound_function.function = DecimalRoundNegativePrecisionFunction<int32_t, NumericHelper>;
			break;
		case PhysicalType::INT64:
			bound_function.function = DecimalRoundNegativePrecisionFunction<int64_t, NumericHelper>;
			break;
		default:
			bound_function.function = DecimalRoundNegativePrecisionFunction<hugeint_t, Hugeint>;
			break;
		}
	} else {
		if (round_value >= (int32_t)scale) {
			// if round_value is bigger than or equal to scale we do nothing
			bound_function.function = ScalarFunction::NopFunction;
			target_scale = scale;
		} else {
			target_scale = NumericCast<uint8_t>(round_value);
			switch (decimal_type.InternalType()) {
			case PhysicalType::INT16:
				bound_function.function = DecimalRoundPositivePrecisionFunction<int16_t, NumericHelper>;
				break;
			case PhysicalType::INT32:
				bound_function.function = DecimalRoundPositivePrecisionFunction<int32_t, NumericHelper>;
				break;
			case PhysicalType::INT64:
				bound_function.function = DecimalRoundPositivePrecisionFunction<int64_t, NumericHelper>;
				break;
			default:
				bound_function.function = DecimalRoundPositivePrecisionFunction<hugeint_t, Hugeint>;
				break;
			}
		}
	}
	bound_function.arguments[0] = decimal_type;
	bound_function.return_type = LogicalType::DECIMAL(width, target_scale);
	return make_uniq<RoundPrecisionFunctionData>(round_value);
}

ScalarFunctionSet RoundFun::GetFunctions() {
	ScalarFunctionSet round;
	for (auto &type : LogicalType::Numeric()) {
		scalar_function_t round_prec_func = nullptr;
		scalar_function_t round_func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		bind_scalar_function_t bind_prec_func = nullptr;
		if (type.IsIntegral()) {
			// no round for integral numbers
			continue;
		}
		switch (type.id()) {
		case LogicalTypeId::FLOAT:
			round_func = ScalarFunction::UnaryFunction<float, float, RoundOperator>;
			round_prec_func = ScalarFunction::BinaryFunction<float, int32_t, float, RoundOperatorPrecision>;
			break;
		case LogicalTypeId::DOUBLE:
			round_func = ScalarFunction::UnaryFunction<double, double, RoundOperator>;
			round_prec_func = ScalarFunction::BinaryFunction<double, int32_t, double, RoundOperatorPrecision>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = BindGenericRoundFunctionDecimal<RoundDecimalOperator>;
			bind_prec_func = BindDecimalRoundPrecision;
			break;
		default:
			throw InternalException("Unimplemented numeric type for function \"floor\"");
		}
		round.AddFunction(ScalarFunction({type}, type, round_func, bind_func));
		round.AddFunction(ScalarFunction({type, LogicalType::INTEGER}, type, round_prec_func, bind_prec_func));
	}
	return round;
}

//===--------------------------------------------------------------------===//
// exp
//===--------------------------------------------------------------------===//
struct ExpOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::exp(left);
	}
};

ScalarFunction ExpFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, ExpOperator>);
}

//===--------------------------------------------------------------------===//
// pow
//===--------------------------------------------------------------------===//
struct PowOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA base, TB exponent) {
		return std::pow(base, exponent);
	}
};

ScalarFunction PowOperatorFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::BinaryFunction<double, double, double, PowOperator>);
}

//===--------------------------------------------------------------------===//
// sqrt
//===--------------------------------------------------------------------===//
struct SqrtOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < 0) {
			throw OutOfRangeException("cannot take square root of a negative number");
		}
		return std::sqrt(input);
	}
};

ScalarFunction SqrtFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, SqrtOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// cbrt
//===--------------------------------------------------------------------===//
struct CbRtOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::cbrt(left);
	}
};

ScalarFunction CbrtFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, CbRtOperator>);
}

//===--------------------------------------------------------------------===//
// ln
//===--------------------------------------------------------------------===//

struct LnOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < 0) {
			throw OutOfRangeException("cannot take logarithm of a negative number");
		}
		if (input == 0) {
			throw OutOfRangeException("cannot take logarithm of zero");
		}
		return std::log(input);
	}
};

ScalarFunction LnFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, LnOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// log
//===--------------------------------------------------------------------===//
struct Log10Operator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < 0) {
			throw OutOfRangeException("cannot take logarithm of a negative number");
		}
		if (input == 0) {
			throw OutOfRangeException("cannot take logarithm of zero");
		}
		return std::log10(input);
	}
};

ScalarFunction Log10Fun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, Log10Operator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// log with base
//===--------------------------------------------------------------------===//
struct LogBaseOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA b, TB x) {
		auto divisor = Log10Operator::Operation<TA, TR>(b);
		if (divisor == 0) {
			throw OutOfRangeException("divison by zero in based logarithm");
		}
		return Log10Operator::Operation<TB, TR>(x) / divisor;
	}
};

ScalarFunctionSet LogFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                                 ScalarFunction::UnaryFunction<double, double, Log10Operator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                                 ScalarFunction::BinaryFunction<double, double, double, LogBaseOperator>));
	for (auto &function : funcs.functions) {
		BaseScalarFunction::SetReturnsError(function);
	}
	return funcs;
}

//===--------------------------------------------------------------------===//
// log2
//===--------------------------------------------------------------------===//
struct Log2Operator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < 0) {
			throw OutOfRangeException("cannot take logarithm of a negative number");
		}
		if (input == 0) {
			throw OutOfRangeException("cannot take logarithm of zero");
		}
		return std::log2(input);
	}
};

ScalarFunction Log2Fun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, Log2Operator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// pi
//===--------------------------------------------------------------------===//
static void PiFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	Value pi_value = Value::DOUBLE(PI);
	result.Reference(pi_value);
}

ScalarFunction PiFun::GetFunction() {
	return ScalarFunction({}, LogicalType::DOUBLE, PiFunction);
}

//===--------------------------------------------------------------------===//
// degrees
//===--------------------------------------------------------------------===//
struct DegreesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return left * (180 / PI);
	}
};

ScalarFunction DegreesFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, DegreesOperator>);
}

//===--------------------------------------------------------------------===//
// radians
//===--------------------------------------------------------------------===//
struct RadiansOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return left * (PI / 180);
	}
};

ScalarFunction RadiansFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, RadiansOperator>);
}

//===--------------------------------------------------------------------===//
// isnan
//===--------------------------------------------------------------------===//
struct IsNanOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Value::IsNan(input);
	}
};

ScalarFunctionSet IsNanFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(ScalarFunction({LogicalType::FLOAT}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<float, bool, IsNanOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<double, bool, IsNanOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// signbit
//===--------------------------------------------------------------------===//
struct SignBitOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return std::signbit(input);
	}
};

ScalarFunctionSet SignBitFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(ScalarFunction({LogicalType::FLOAT}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<float, bool, SignBitOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<double, bool, SignBitOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// isinf
//===--------------------------------------------------------------------===//
struct IsInfiniteOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return !Value::IsNan(input) && !Value::IsFinite(input);
	}
};

template <>
bool IsInfiniteOperator::Operation(date_t input) {
	return !Value::IsFinite(input);
}

template <>
bool IsInfiniteOperator::Operation(timestamp_t input) {
	return !Value::IsFinite(input);
}

ScalarFunctionSet IsInfiniteFun::GetFunctions() {
	ScalarFunctionSet funcs("isinf");
	funcs.AddFunction(ScalarFunction({LogicalType::FLOAT}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<float, bool, IsInfiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<double, bool, IsInfiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<date_t, bool, IsInfiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<timestamp_t, bool, IsInfiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<timestamp_t, bool, IsInfiniteOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// isfinite
//===--------------------------------------------------------------------===//
struct IsFiniteOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Value::IsFinite(input);
	}
};

ScalarFunctionSet IsFiniteFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(ScalarFunction({LogicalType::FLOAT}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<float, bool, IsFiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<double, bool, IsFiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<date_t, bool, IsFiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<timestamp_t, bool, IsFiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<timestamp_t, bool, IsFiniteOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// sin
//===--------------------------------------------------------------------===//
template <class OP>
struct NoInfiniteDoubleWrapper {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		if (DUCKDB_UNLIKELY(!Value::IsFinite(input))) {
			if (Value::IsNan(input)) {
				return input;
			}
			throw OutOfRangeException("input value %lf is out of range for numeric function", input);
		}
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct SinOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return std::sin(input);
	}
};

ScalarFunction SinFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<SinOperator>>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// cos
//===--------------------------------------------------------------------===//
struct CosOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::cos(input);
	}
};

ScalarFunction CosFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<CosOperator>>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// tan
//===--------------------------------------------------------------------===//
struct TanOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::tan(input);
	}
};

ScalarFunction TanFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<TanOperator>>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// asin
//===--------------------------------------------------------------------===//
struct ASinOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < -1 || input > 1) {
			throw InvalidInputException("ASIN is undefined outside [-1,1]");
		}
		return (double)std::asin(input);
	}
};

ScalarFunction AsinFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<ASinOperator>>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// atan
//===--------------------------------------------------------------------===//
struct ATanOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::atan(input);
	}
};

ScalarFunction AtanFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, ATanOperator>);
}

//===--------------------------------------------------------------------===//
// atan2
//===--------------------------------------------------------------------===//
struct ATan2 {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return (double)std::atan2(left, right);
	}
};

ScalarFunction Atan2Fun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::BinaryFunction<double, double, double, ATan2>);
}

//===--------------------------------------------------------------------===//
// acos
//===--------------------------------------------------------------------===//
struct ACos {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < -1 || input > 1) {
			throw InvalidInputException("ACOS is undefined outside [-1,1]");
		}
		return (double)std::acos(input);
	}
};

ScalarFunction AcosFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<ACos>>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// cosh
//===--------------------------------------------------------------------===//
struct CoshOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::cosh(input);
	}
};

ScalarFunction CoshFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, CoshOperator>);
}

//===--------------------------------------------------------------------===//
// acosh
//===--------------------------------------------------------------------===//
struct AcoshOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::acosh(input);
	}
};

ScalarFunction AcoshFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, AcoshOperator>);
}

//===--------------------------------------------------------------------===//
// sinh
//===--------------------------------------------------------------------===//
struct SinhOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::sinh(input);
	}
};

ScalarFunction SinhFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, SinhOperator>);
}

//===--------------------------------------------------------------------===//
// asinh
//===--------------------------------------------------------------------===//
struct AsinhOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::asinh(input);
	}
};

ScalarFunction AsinhFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, AsinhOperator>);
}

//===--------------------------------------------------------------------===//
// tanh
//===--------------------------------------------------------------------===//
struct TanhOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::tanh(input);
	}
};

ScalarFunction TanhFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, TanhOperator>);
}

//===--------------------------------------------------------------------===//
// atanh
//===--------------------------------------------------------------------===//
struct AtanhOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < -1 || input > 1) {
			throw InvalidInputException("ATANH is undefined outside [-1,1]");
		}
		if (input == -1 || input == 1) {
			return INFINITY;
		}
		return (double)std::atanh(input);
	}
};

ScalarFunction AtanhFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, AtanhOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// cot
//===--------------------------------------------------------------------===//
template <class OP>
struct NoInfiniteNoZeroDoubleWrapper {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		if (DUCKDB_UNLIKELY(!Value::IsFinite(input))) {
			if (Value::IsNan(input)) {
				return input;
			}
			throw OutOfRangeException("input value %lf is out of range for numeric function", input);
		}
		if (DUCKDB_UNLIKELY((double)input == 0.0 || (double)input == -0.0)) {
			throw OutOfRangeException("input value %lf is out of range for numeric function cotangent", input);
		}
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct CotOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 1.0 / (double)std::tan(input);
	}
};

ScalarFunction CotFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, NoInfiniteNoZeroDoubleWrapper<CotOperator>>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// gamma
//===--------------------------------------------------------------------===//
struct GammaOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input == 0) {
			throw OutOfRangeException("cannot take gamma of zero");
		}
		return std::tgamma(input);
	}
};

ScalarFunction GammaFun::GetFunction() {
	auto func = ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                           ScalarFunction::UnaryFunction<double, double, GammaOperator>);
	BaseScalarFunction::SetReturnsError(func);
	return func;
}

//===--------------------------------------------------------------------===//
// gamma
//===--------------------------------------------------------------------===//
struct LogGammaOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input == 0) {
			throw OutOfRangeException("cannot take log gamma of zero");
		}
		return std::lgamma(input);
	}
};

ScalarFunction LogGammaFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                        ScalarFunction::UnaryFunction<double, double, LogGammaOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// factorial(), !
//===--------------------------------------------------------------------===//
struct FactorialOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		TR ret = 1;
		for (TA i = 2; i <= left; i++) {
			if (!TryMultiplyOperator::Operation(ret, TR(i), ret)) {
				throw OutOfRangeException("Value out of range");
			}
		}
		return ret;
	}
};

ScalarFunction FactorialOperatorFun::GetFunction() {
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::HUGEINT,
	                        ScalarFunction::UnaryFunction<int32_t, hugeint_t, FactorialOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

//===--------------------------------------------------------------------===//
// even
//===--------------------------------------------------------------------===//
struct EvenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		double value;
		if (left >= 0) {
			value = std::ceil(left);
		} else {
			value = std::ceil(-left);
			value = -value;
		}
		if (std::floor(value / 2) * 2 != value) {
			if (left >= 0) {
				return value += 1;
			}
			return value -= 1;
		}
		return value;
	}
};

ScalarFunction EvenFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, EvenOperator>);
}

//===--------------------------------------------------------------------===//
// gcd
//===--------------------------------------------------------------------===//

// should be replaced with std::gcd in a newer C++ standard
template <class TA>
TA GreatestCommonDivisor(TA left, TA right) {
	TA a = left;
	TA b = right;

	// This protects the following modulo operations from a corner case,
	// where we would get a runtime error due to an integer overflow.
	if ((left == NumericLimits<TA>::Minimum() && right == -1) ||
	    (left == -1 && right == NumericLimits<TA>::Minimum())) {
		return 1;
	}

	while (true) {
		if (a == 0) {
			return TryAbsOperator::Operation<TA, TA>(b);
		}
		b %= a;

		if (b == 0) {
			return TryAbsOperator::Operation<TA, TA>(a);
		}
		a %= b;
	}
}

struct GreatestCommonDivisorOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return GreatestCommonDivisor(left, right);
	}
};

ScalarFunctionSet GreatestCommonDivisorFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, GreatestCommonDivisorOperator>));
	funcs.AddFunction(
	    ScalarFunction({LogicalType::HUGEINT, LogicalType::HUGEINT}, LogicalType::HUGEINT,
	                   ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, GreatestCommonDivisorOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// lcm
//===--------------------------------------------------------------------===//

// should be replaced with std::lcm in a newer C++ standard
struct LeastCommonMultipleOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		if (left == 0 || right == 0) {
			return 0;
		}
		TR result;
		if (!TryMultiplyOperator::Operation<TA, TB, TR>(left, right / GreatestCommonDivisor(left, right), result)) {
			throw OutOfRangeException("lcm value is out of range");
		}
		return TryAbsOperator::Operation<TR, TR>(result);
	}
};

ScalarFunctionSet LeastCommonMultipleFun::GetFunctions() {
	ScalarFunctionSet funcs;

	funcs.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, LeastCommonMultipleOperator>));
	funcs.AddFunction(
	    ScalarFunction({LogicalType::HUGEINT, LogicalType::HUGEINT}, LogicalType::HUGEINT,
	                   ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, LeastCommonMultipleOperator>));
	for (auto &function : funcs.functions) {
		BaseScalarFunction::SetReturnsError(function);
	}
	return funcs;
}

} // namespace duckdb
