#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <limits>

namespace duckdb {

template <class OP>
static scalar_function_t GetScalarIntegerFunction(PhysicalType type) {
	scalar_function_t function;
	switch (type) {
	case PhysicalType::INT8:
		function = &ScalarFunction::BinaryFunction<int8_t, int8_t, int8_t, OP>;
		break;
	case PhysicalType::INT16:
		function = &ScalarFunction::BinaryFunction<int16_t, int16_t, int16_t, OP>;
		break;
	case PhysicalType::INT32:
		function = &ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, OP>;
		break;
	case PhysicalType::INT64:
		function = &ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, OP>;
		break;
	case PhysicalType::INT128:
		function = &ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, OP>;
		break;
	case PhysicalType::UINT8:
		function = &ScalarFunction::BinaryFunction<uint8_t, uint8_t, uint8_t, OP>;
		break;
	case PhysicalType::UINT16:
		function = &ScalarFunction::BinaryFunction<uint16_t, uint16_t, uint16_t, OP>;
		break;
	case PhysicalType::UINT32:
		function = &ScalarFunction::BinaryFunction<uint32_t, uint32_t, uint32_t, OP>;
		break;
	case PhysicalType::UINT64:
		function = &ScalarFunction::BinaryFunction<uint64_t, uint64_t, uint64_t, OP>;
		break;
	case PhysicalType::UINT128:
		function = &ScalarFunction::BinaryFunction<uhugeint_t, uhugeint_t, uhugeint_t, OP>;
		break;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarBinaryFunction: %s", TypeIdToString(type));
	}
	return function;
}

template <class OP>
static scalar_function_t GetScalarBinaryFunction(PhysicalType type) {
	scalar_function_t function;
	switch (type) {
	case PhysicalType::FLOAT:
		function = &ScalarFunction::BinaryFunction<float, float, float, OP>;
		break;
	case PhysicalType::DOUBLE:
		function = &ScalarFunction::BinaryFunction<double, double, double, OP>;
		break;
	default:
		function = GetScalarIntegerFunction<OP>(type);
		break;
	}
	return function;
}

//===--------------------------------------------------------------------===//
// + [add]
//===--------------------------------------------------------------------===//
struct AddPropagateStatistics {
	template <class T, class OP>
	static bool Operation(LogicalType type, BaseStatistics &lstats, BaseStatistics &rstats, Value &new_min,
	                      Value &new_max) {
		T min, max;
		// new min is min+min
		if (!OP::Operation(NumericStats::GetMin<T>(lstats), NumericStats::GetMin<T>(rstats), min)) {
			return true;
		}
		// new max is max+max
		if (!OP::Operation(NumericStats::GetMax<T>(lstats), NumericStats::GetMax<T>(rstats), max)) {
			return true;
		}
		new_min = Value::Numeric(type, min);
		new_max = Value::Numeric(type, max);
		return false;
	}
};

struct SubtractPropagateStatistics {
	template <class T, class OP>
	static bool Operation(LogicalType type, BaseStatistics &lstats, BaseStatistics &rstats, Value &new_min,
	                      Value &new_max) {
		T min, max;
		if (!OP::Operation(NumericStats::GetMin<T>(lstats), NumericStats::GetMax<T>(rstats), min)) {
			return true;
		}
		if (!OP::Operation(NumericStats::GetMax<T>(lstats), NumericStats::GetMin<T>(rstats), max)) {
			return true;
		}
		new_min = Value::Numeric(type, min);
		new_max = Value::Numeric(type, max);
		return false;
	}
};

struct DecimalArithmeticBindData : public FunctionData {
	DecimalArithmeticBindData() : check_overflow(false) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto res = make_uniq<DecimalArithmeticBindData>();
		res->check_overflow = check_overflow;
		return std::move(res);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto other = other_p.Cast<DecimalArithmeticBindData>();
		return other.check_overflow == check_overflow;
	}

	bool check_overflow;
};

template <class OP, class PROPAGATE, class BASEOP>
static unique_ptr<BaseStatistics> PropagateNumericStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 2);
	// can only propagate stats if the children have stats
	auto &lstats = child_stats[0];
	auto &rstats = child_stats[1];
	Value new_min, new_max;
	bool potential_overflow = true;
	if (NumericStats::HasMinMax(lstats) && NumericStats::HasMinMax(rstats)) {
		switch (expr.return_type.InternalType()) {
		case PhysicalType::INT8:
			potential_overflow =
			    PROPAGATE::template Operation<int8_t, OP>(expr.return_type, lstats, rstats, new_min, new_max);
			break;
		case PhysicalType::INT16:
			potential_overflow =
			    PROPAGATE::template Operation<int16_t, OP>(expr.return_type, lstats, rstats, new_min, new_max);
			break;
		case PhysicalType::INT32:
			potential_overflow =
			    PROPAGATE::template Operation<int32_t, OP>(expr.return_type, lstats, rstats, new_min, new_max);
			break;
		case PhysicalType::INT64:
			potential_overflow =
			    PROPAGATE::template Operation<int64_t, OP>(expr.return_type, lstats, rstats, new_min, new_max);
			break;
		default:
			return nullptr;
		}
	}
	if (potential_overflow) {
		new_min = Value(expr.return_type);
		new_max = Value(expr.return_type);
	} else {
		// no potential overflow: replace with non-overflowing operator
		if (input.bind_data) {
			auto &bind_data = input.bind_data->Cast<DecimalArithmeticBindData>();
			bind_data.check_overflow = false;
		}
		expr.function.function = GetScalarIntegerFunction<BASEOP>(expr.return_type.InternalType());
	}
	auto result = NumericStats::CreateEmpty(expr.return_type);
	NumericStats::SetMin(result, new_min);
	NumericStats::SetMax(result, new_max);
	result.CombineValidity(lstats, rstats);
	return result.ToUnique();
}

template <bool IS_MODULO = false>
unique_ptr<DecimalArithmeticBindData> BindDecimalArithmetic(ClientContext &context, ScalarFunction &bound_function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = make_uniq<DecimalArithmeticBindData>();

	// get the max width and scale of the input arguments
	uint8_t max_width = 0, max_scale = 0, max_width_over_scale = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i]->return_type.id() == LogicalTypeId::UNKNOWN) {
			continue;
		}
		uint8_t width, scale;
		auto can_convert = arguments[i]->return_type.GetDecimalProperties(width, scale);
		if (!can_convert) {
			throw InternalException("Could not convert type %s to a decimal.", arguments[i]->return_type.ToString());
		}
		max_width = MaxValue<uint8_t>(width, max_width);
		max_scale = MaxValue<uint8_t>(scale, max_scale);
		max_width_over_scale = MaxValue<uint8_t>(width - scale, max_width_over_scale);
	}
	D_ASSERT(max_width > 0);
	uint8_t required_width = MaxValue<uint8_t>(max_scale + max_width_over_scale, max_width);
	if (!IS_MODULO) {
		// for addition/subtraction, we add 1 to the width to ensure we don't overflow
		required_width = NumericCast<uint8_t>(required_width + 1);
		if (required_width > Decimal::MAX_WIDTH_INT64 && max_width <= Decimal::MAX_WIDTH_INT64) {
			// we don't automatically promote past the hugeint boundary to avoid the large hugeint performance penalty
			bind_data->check_overflow = true;
			required_width = Decimal::MAX_WIDTH_INT64;
		}
	}
	if (required_width > Decimal::MAX_WIDTH_DECIMAL) {
		// target width does not fit in decimal at all: truncate the scale and perform overflow detection
		bind_data->check_overflow = true;
		required_width = Decimal::MAX_WIDTH_DECIMAL;
	}
	// arithmetic between two decimal arguments: check the types of the input arguments
	LogicalType result_type = LogicalType::DECIMAL(required_width, max_scale);
	// we cast all input types to the specified type
	for (idx_t i = 0; i < arguments.size(); i++) {
		// first check if the cast is necessary
		// if the argument has a matching scale and internal type as the output type, no casting is necessary
		auto &argument_type = arguments[i]->return_type;
		uint8_t width, scale;
		argument_type.GetDecimalProperties(width, scale);
		if (scale == DecimalType::GetScale(result_type) && argument_type.InternalType() == result_type.InternalType()) {
			bound_function.arguments[i] = argument_type;
		} else {
			bound_function.arguments[i] = result_type;
		}
	}
	bound_function.return_type = result_type;
	return bind_data;
}

template <class OP, class OPOVERFLOWCHECK, bool IS_SUBTRACT = false>
unique_ptr<FunctionData> BindDecimalAddSubtract(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindDecimalArithmetic(context, bound_function, arguments);

	// now select the physical function to execute
	auto &result_type = bound_function.return_type;
	if (bind_data->check_overflow) {
		bound_function.function = GetScalarBinaryFunction<OPOVERFLOWCHECK>(result_type.InternalType());
	} else {
		bound_function.function = GetScalarBinaryFunction<OP>(result_type.InternalType());
	}
	if (result_type.InternalType() != PhysicalType::INT128 && result_type.InternalType() != PhysicalType::UINT128) {
		if (IS_SUBTRACT) {
			bound_function.statistics =
			    PropagateNumericStats<TryDecimalSubtract, SubtractPropagateStatistics, SubtractOperator>;
		} else {
			bound_function.statistics = PropagateNumericStats<TryDecimalAdd, AddPropagateStatistics, AddOperator>;
		}
	}
	return std::move(bind_data);
}

static void SerializeDecimalArithmetic(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                       const ScalarFunction &function) {
	auto &bind_data = bind_data_p->Cast<DecimalArithmeticBindData>();
	serializer.WriteProperty(100, "check_overflow", bind_data.check_overflow);
	serializer.WriteProperty(101, "return_type", function.return_type);
	serializer.WriteProperty(102, "arguments", function.arguments);
}

// TODO this is partially duplicated from the bind
template <class OP, class OPOVERFLOWCHECK, bool IS_SUBTRACT = false>
unique_ptr<FunctionData> DeserializeDecimalArithmetic(Deserializer &deserializer, ScalarFunction &bound_function) {

	//	// re-change the function pointers
	auto check_overflow = deserializer.ReadProperty<bool>(100, "check_overflow");
	auto return_type = deserializer.ReadProperty<LogicalType>(101, "return_type");
	auto arguments = deserializer.ReadProperty<vector<LogicalType>>(102, "arguments");
	if (check_overflow) {
		bound_function.function = GetScalarBinaryFunction<OPOVERFLOWCHECK>(return_type.InternalType());
	} else {
		bound_function.function = GetScalarBinaryFunction<OP>(return_type.InternalType());
	}
	bound_function.statistics = nullptr; // TODO we likely dont want to do stats prop again
	bound_function.return_type = return_type;
	bound_function.arguments = arguments;

	auto bind_data = make_uniq<DecimalArithmeticBindData>();
	bind_data->check_overflow = check_overflow;
	return std::move(bind_data);
}

unique_ptr<FunctionData> NopDecimalBind(ClientContext &context, ScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments) {
	bound_function.return_type = arguments[0]->return_type;
	bound_function.arguments[0] = arguments[0]->return_type;
	return nullptr;
}

ScalarFunction AddFun::GetFunction(const LogicalType &type) {
	D_ASSERT(type.IsNumeric());
	if (type.id() == LogicalTypeId::DECIMAL) {
		return ScalarFunction("+", {type}, type, ScalarFunction::NopFunction, NopDecimalBind);
	} else {
		return ScalarFunction("+", {type}, type, ScalarFunction::NopFunction);
	}
}

ScalarFunction AddFun::GetFunction(const LogicalType &left_type, const LogicalType &right_type) {
	if (left_type.IsNumeric() && left_type.id() == right_type.id()) {
		if (left_type.id() == LogicalTypeId::DECIMAL) {
			auto function = ScalarFunction("+", {left_type, right_type}, left_type, nullptr,
			                               BindDecimalAddSubtract<AddOperator, DecimalAddOverflowCheck>);
			function.serialize = SerializeDecimalArithmetic;
			function.deserialize = DeserializeDecimalArithmetic<AddOperator, DecimalAddOverflowCheck>;
			return function;
		} else if (left_type.IsIntegral()) {
			return ScalarFunction("+", {left_type, right_type}, left_type,
			                      GetScalarIntegerFunction<AddOperatorOverflowCheck>(left_type.InternalType()), nullptr,
			                      nullptr, PropagateNumericStats<TryAddOperator, AddPropagateStatistics, AddOperator>);
		} else {
			return ScalarFunction("+", {left_type, right_type}, left_type,
			                      GetScalarBinaryFunction<AddOperator>(left_type.InternalType()));
		}
	}

	switch (left_type.id()) {
	case LogicalTypeId::DATE:
		if (right_type.id() == LogicalTypeId::INTEGER) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::DATE,
			                      ScalarFunction::BinaryFunction<date_t, int32_t, date_t, AddOperator>);
		} else if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
			                      ScalarFunction::BinaryFunction<date_t, interval_t, timestamp_t, AddOperator>);
		} else if (right_type.id() == LogicalTypeId::TIME) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
			                      ScalarFunction::BinaryFunction<date_t, dtime_t, timestamp_t, AddOperator>);
		} else if (right_type.id() == LogicalTypeId::TIME_TZ) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP_TZ,
			                      ScalarFunction::BinaryFunction<date_t, dtime_tz_t, timestamp_t, AddOperator>);
		}
		break;
	case LogicalTypeId::INTEGER:
		if (right_type.id() == LogicalTypeId::DATE) {
			return ScalarFunction("+", {left_type, right_type}, right_type,
			                      ScalarFunction::BinaryFunction<int32_t, date_t, date_t, AddOperator>);
		}
		break;
	case LogicalTypeId::INTERVAL:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::INTERVAL,
			                      ScalarFunction::BinaryFunction<interval_t, interval_t, interval_t, AddOperator>);
		} else if (right_type.id() == LogicalTypeId::DATE) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
			                      ScalarFunction::BinaryFunction<interval_t, date_t, timestamp_t, AddOperator>);
		} else if (right_type.id() == LogicalTypeId::TIME) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIME,
			                      ScalarFunction::BinaryFunction<interval_t, dtime_t, dtime_t, AddTimeOperator>);
		} else if (right_type.id() == LogicalTypeId::TIME_TZ) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIME_TZ,
			                      ScalarFunction::BinaryFunction<interval_t, dtime_tz_t, dtime_tz_t, AddTimeOperator>);
		} else if (right_type.id() == LogicalTypeId::TIMESTAMP) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
			                      ScalarFunction::BinaryFunction<interval_t, timestamp_t, timestamp_t, AddOperator>);
		}
		break;
	case LogicalTypeId::TIME:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIME,
			                      ScalarFunction::BinaryFunction<dtime_t, interval_t, dtime_t, AddTimeOperator>);
		} else if (right_type.id() == LogicalTypeId::DATE) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
			                      ScalarFunction::BinaryFunction<dtime_t, date_t, timestamp_t, AddOperator>);
		}
		break;
	case LogicalTypeId::TIME_TZ:
		if (right_type.id() == LogicalTypeId::DATE) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP_TZ,
			                      ScalarFunction::BinaryFunction<dtime_tz_t, date_t, timestamp_t, AddOperator>);
		} else if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIME_TZ,
			                      ScalarFunction::BinaryFunction<dtime_tz_t, interval_t, dtime_tz_t, AddTimeOperator>);
		}
		break;
	case LogicalTypeId::TIMESTAMP:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
			                      ScalarFunction::BinaryFunction<timestamp_t, interval_t, timestamp_t, AddOperator>);
		}
		break;
	default:
		break;
	}
	// LCOV_EXCL_START
	throw NotImplementedException("AddFun for types %s, %s", EnumUtil::ToString(left_type.id()),
	                              EnumUtil::ToString(right_type.id()));
	// LCOV_EXCL_STOP
}

void AddFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("+");
	for (auto &type : LogicalType::Numeric()) {
		// unary add function is a nop, but only exists for numeric types
		functions.AddFunction(GetFunction(type));
		// binary add function adds two numbers together
		functions.AddFunction(GetFunction(type, type));
	}
	// we can add integers to dates
	functions.AddFunction(GetFunction(LogicalType::DATE, LogicalType::INTEGER));
	functions.AddFunction(GetFunction(LogicalType::INTEGER, LogicalType::DATE));
	// we can add intervals together
	functions.AddFunction(GetFunction(LogicalType::INTERVAL, LogicalType::INTERVAL));
	// we can add intervals to dates/times/timestamps
	functions.AddFunction(GetFunction(LogicalType::DATE, LogicalType::INTERVAL));
	functions.AddFunction(GetFunction(LogicalType::INTERVAL, LogicalType::DATE));

	functions.AddFunction(GetFunction(LogicalType::TIME, LogicalType::INTERVAL));
	functions.AddFunction(GetFunction(LogicalType::INTERVAL, LogicalType::TIME));

	functions.AddFunction(GetFunction(LogicalType::TIMESTAMP, LogicalType::INTERVAL));
	functions.AddFunction(GetFunction(LogicalType::INTERVAL, LogicalType::TIMESTAMP));

	functions.AddFunction(GetFunction(LogicalType::TIME_TZ, LogicalType::INTERVAL));
	functions.AddFunction(GetFunction(LogicalType::INTERVAL, LogicalType::TIME_TZ));

	// we can add times to dates
	functions.AddFunction(GetFunction(LogicalType::TIME, LogicalType::DATE));
	functions.AddFunction(GetFunction(LogicalType::DATE, LogicalType::TIME));

	// we can add times with time zones (offsets) to dates
	functions.AddFunction(GetFunction(LogicalType::TIME_TZ, LogicalType::DATE));
	functions.AddFunction(GetFunction(LogicalType::DATE, LogicalType::TIME_TZ));

	// we can add lists together
	functions.AddFunction(ListConcatFun::GetFunction());

	set.AddFunction(functions);

	functions.name = "add";
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// - [subtract]
//===--------------------------------------------------------------------===//
struct NegateOperator {
	template <class T>
	static bool CanNegate(T input) {
		using Limits = NumericLimits<T>;
		return !(Limits::IsSigned() && Limits::Minimum() == input);
	}

	template <class TA, class TR>
	static inline TR Operation(TA input) {
		auto cast = (TR)input;
		if (!CanNegate<TR>(cast)) {
			throw OutOfRangeException("Overflow in negation of integer!");
		}
		return -cast;
	}
};

template <>
bool NegateOperator::CanNegate(float input) {
	return true;
}

template <>
bool NegateOperator::CanNegate(double input) {
	return true;
}

template <>
interval_t NegateOperator::Operation(interval_t input) {
	interval_t result;
	result.months = NegateOperator::Operation<int32_t, int32_t>(input.months);
	result.days = NegateOperator::Operation<int32_t, int32_t>(input.days);
	result.micros = NegateOperator::Operation<int64_t, int64_t>(input.micros);
	return result;
}

struct DecimalNegateBindData : public FunctionData {
	DecimalNegateBindData() : bound_type(LogicalTypeId::INVALID) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto res = make_uniq<DecimalNegateBindData>();
		res->bound_type = bound_type;
		return std::move(res);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto other = other_p.Cast<DecimalNegateBindData>();
		return other.bound_type == bound_type;
	}

	LogicalTypeId bound_type;
};

unique_ptr<FunctionData> DecimalNegateBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {

	auto bind_data = make_uniq<DecimalNegateBindData>();

	auto &decimal_type = arguments[0]->return_type;
	auto width = DecimalType::GetWidth(decimal_type);
	if (width <= Decimal::MAX_WIDTH_INT16) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<NegateOperator>(LogicalTypeId::SMALLINT);
	} else if (width <= Decimal::MAX_WIDTH_INT32) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<NegateOperator>(LogicalTypeId::INTEGER);
	} else if (width <= Decimal::MAX_WIDTH_INT64) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<NegateOperator>(LogicalTypeId::BIGINT);
	} else {
		D_ASSERT(width <= Decimal::MAX_WIDTH_INT128);
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<NegateOperator>(LogicalTypeId::HUGEINT);
	}
	decimal_type.Verify();
	bound_function.arguments[0] = decimal_type;
	bound_function.return_type = decimal_type;
	return nullptr;
}

struct NegatePropagateStatistics {
	template <class T>
	static bool Operation(LogicalType type, BaseStatistics &istats, Value &new_min, Value &new_max) {
		auto max_value = NumericStats::GetMax<T>(istats);
		auto min_value = NumericStats::GetMin<T>(istats);
		if (!NegateOperator::CanNegate<T>(min_value) || !NegateOperator::CanNegate<T>(max_value)) {
			return true;
		}
		// new min is -max
		new_min = Value::Numeric(type, NegateOperator::Operation<T, T>(max_value));
		// new max is -min
		new_max = Value::Numeric(type, NegateOperator::Operation<T, T>(min_value));
		return false;
	}
};

static unique_ptr<BaseStatistics> NegateBindStatistics(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 1);
	// can only propagate stats if the children have stats
	auto &istats = child_stats[0];
	Value new_min, new_max;
	bool potential_overflow = true;
	if (NumericStats::HasMinMax(istats)) {
		switch (expr.return_type.InternalType()) {
		case PhysicalType::INT8:
			potential_overflow =
			    NegatePropagateStatistics::Operation<int8_t>(expr.return_type, istats, new_min, new_max);
			break;
		case PhysicalType::INT16:
			potential_overflow =
			    NegatePropagateStatistics::Operation<int16_t>(expr.return_type, istats, new_min, new_max);
			break;
		case PhysicalType::INT32:
			potential_overflow =
			    NegatePropagateStatistics::Operation<int32_t>(expr.return_type, istats, new_min, new_max);
			break;
		case PhysicalType::INT64:
			potential_overflow =
			    NegatePropagateStatistics::Operation<int64_t>(expr.return_type, istats, new_min, new_max);
			break;
		default:
			return nullptr;
		}
	}
	if (potential_overflow) {
		new_min = Value(expr.return_type);
		new_max = Value(expr.return_type);
	}
	auto stats = NumericStats::CreateEmpty(expr.return_type);
	NumericStats::SetMin(stats, new_min);
	NumericStats::SetMax(stats, new_max);
	stats.CopyValidity(istats);
	return stats.ToUnique();
}

ScalarFunction SubtractFun::GetFunction(const LogicalType &type) {
	if (type.id() == LogicalTypeId::INTERVAL) {
		return ScalarFunction("-", {type}, type, ScalarFunction::UnaryFunction<interval_t, interval_t, NegateOperator>);
	} else if (type.id() == LogicalTypeId::DECIMAL) {
		return ScalarFunction("-", {type}, type, nullptr, DecimalNegateBind, nullptr, NegateBindStatistics);
	} else {
		D_ASSERT(type.IsNumeric());
		return ScalarFunction("-", {type}, type, ScalarFunction::GetScalarUnaryFunction<NegateOperator>(type), nullptr,
		                      nullptr, NegateBindStatistics);
	}
}

ScalarFunction SubtractFun::GetFunction(const LogicalType &left_type, const LogicalType &right_type) {
	if (left_type.IsNumeric() && left_type.id() == right_type.id()) {
		if (left_type.id() == LogicalTypeId::DECIMAL) {
			auto function =
			    ScalarFunction("-", {left_type, right_type}, left_type, nullptr,
			                   BindDecimalAddSubtract<SubtractOperator, DecimalSubtractOverflowCheck, true>);
			function.serialize = SerializeDecimalArithmetic;
			function.deserialize = DeserializeDecimalArithmetic<SubtractOperator, DecimalSubtractOverflowCheck>;
			return function;
		} else if (left_type.IsIntegral()) {
			return ScalarFunction(
			    "-", {left_type, right_type}, left_type,
			    GetScalarIntegerFunction<SubtractOperatorOverflowCheck>(left_type.InternalType()), nullptr, nullptr,
			    PropagateNumericStats<TrySubtractOperator, SubtractPropagateStatistics, SubtractOperator>);

		} else {
			return ScalarFunction("-", {left_type, right_type}, left_type,
			                      GetScalarBinaryFunction<SubtractOperator>(left_type.InternalType()));
		}
	}

	switch (left_type.id()) {
	case LogicalTypeId::DATE:
		if (right_type.id() == LogicalTypeId::DATE) {
			return ScalarFunction("-", {left_type, right_type}, LogicalType::BIGINT,
			                      ScalarFunction::BinaryFunction<date_t, date_t, int64_t, SubtractOperator>);

		} else if (right_type.id() == LogicalTypeId::INTEGER) {
			return ScalarFunction("-", {left_type, right_type}, LogicalType::DATE,
			                      ScalarFunction::BinaryFunction<date_t, int32_t, date_t, SubtractOperator>);
		} else if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction("-", {left_type, right_type}, LogicalType::TIMESTAMP,
			                      ScalarFunction::BinaryFunction<date_t, interval_t, timestamp_t, SubtractOperator>);
		}
		break;
	case LogicalTypeId::TIMESTAMP:
		if (right_type.id() == LogicalTypeId::TIMESTAMP) {
			return ScalarFunction(
			    "-", {left_type, right_type}, LogicalType::INTERVAL,
			    ScalarFunction::BinaryFunction<timestamp_t, timestamp_t, interval_t, SubtractOperator>);
		} else if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction(
			    "-", {left_type, right_type}, LogicalType::TIMESTAMP,
			    ScalarFunction::BinaryFunction<timestamp_t, interval_t, timestamp_t, SubtractOperator>);
		}
		break;
	case LogicalTypeId::INTERVAL:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction("-", {left_type, right_type}, LogicalType::INTERVAL,
			                      ScalarFunction::BinaryFunction<interval_t, interval_t, interval_t, SubtractOperator>);
		}
		break;
	case LogicalTypeId::TIME:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction("-", {left_type, right_type}, LogicalType::TIME,
			                      ScalarFunction::BinaryFunction<dtime_t, interval_t, dtime_t, SubtractTimeOperator>);
		}
		break;
	case LogicalTypeId::TIME_TZ:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			return ScalarFunction(
			    "-", {left_type, right_type}, LogicalType::TIME_TZ,
			    ScalarFunction::BinaryFunction<dtime_tz_t, interval_t, dtime_tz_t, SubtractTimeOperator>);
		}
		break;
	default:
		break;
	}
	// LCOV_EXCL_START
	throw NotImplementedException("SubtractFun for types %s, %s", EnumUtil::ToString(left_type.id()),
	                              EnumUtil::ToString(right_type.id()));
	// LCOV_EXCL_STOP
}

void SubtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("-");
	for (auto &type : LogicalType::Numeric()) {
		// unary subtract function, negates the input (i.e. multiplies by -1)
		functions.AddFunction(GetFunction(type));
		// binary subtract function "a - b", subtracts b from a
		functions.AddFunction(GetFunction(type, type));
	}
	// we can subtract dates from each other
	functions.AddFunction(GetFunction(LogicalType::DATE, LogicalType::DATE));
	// we can subtract integers from dates
	functions.AddFunction(GetFunction(LogicalType::DATE, LogicalType::INTEGER));
	// we can subtract timestamps from each other
	functions.AddFunction(GetFunction(LogicalType::TIMESTAMP, LogicalType::TIMESTAMP));
	// we can subtract intervals from each other
	functions.AddFunction(GetFunction(LogicalType::INTERVAL, LogicalType::INTERVAL));
	// we can subtract intervals from dates/times/timestamps, but not the other way around
	functions.AddFunction(GetFunction(LogicalType::DATE, LogicalType::INTERVAL));
	functions.AddFunction(GetFunction(LogicalType::TIME, LogicalType::INTERVAL));
	functions.AddFunction(GetFunction(LogicalType::TIMESTAMP, LogicalType::INTERVAL));
	functions.AddFunction(GetFunction(LogicalType::TIME_TZ, LogicalType::INTERVAL));
	// we can negate intervals
	functions.AddFunction(GetFunction(LogicalType::INTERVAL));
	set.AddFunction(functions);

	functions.name = "subtract";
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// * [multiply]
//===--------------------------------------------------------------------===//
struct MultiplyPropagateStatistics {
	template <class T, class OP>
	static bool Operation(LogicalType type, BaseStatistics &lstats, BaseStatistics &rstats, Value &new_min,
	                      Value &new_max) {
		// statistics propagation on the multiplication is slightly less straightforward because of negative numbers
		// the new min/max depend on the signs of the input types
		// if both are positive the result is [lmin * rmin][lmax * rmax]
		// if lmin/lmax are negative the result is [lmin * rmax][lmax * rmin]
		// etc
		// rather than doing all this switcheroo we just multiply all combinations of lmin/lmax with rmin/rmax
		// and check what the minimum/maximum value is
		T lvals[] {NumericStats::GetMin<T>(lstats), NumericStats::GetMax<T>(lstats)};
		T rvals[] {NumericStats::GetMin<T>(rstats), NumericStats::GetMax<T>(rstats)};
		T min = NumericLimits<T>::Maximum();
		T max = NumericLimits<T>::Minimum();
		// multiplications
		for (idx_t l = 0; l < 2; l++) {
			for (idx_t r = 0; r < 2; r++) {
				T result;
				if (!OP::Operation(lvals[l], rvals[r], result)) {
					// potential overflow
					return true;
				}
				if (result < min) {
					min = result;
				}
				if (result > max) {
					max = result;
				}
			}
		}
		new_min = Value::Numeric(type, min);
		new_max = Value::Numeric(type, max);
		return false;
	}
};

unique_ptr<FunctionData> BindDecimalMultiply(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {

	auto bind_data = make_uniq<DecimalArithmeticBindData>();

	uint8_t result_width = 0, result_scale = 0;
	uint8_t max_width = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i]->return_type.id() == LogicalTypeId::UNKNOWN) {
			continue;
		}
		uint8_t width, scale;
		auto can_convert = arguments[i]->return_type.GetDecimalProperties(width, scale);
		if (!can_convert) {
			throw InternalException("Could not convert type %s to a decimal?", arguments[i]->return_type.ToString());
		}
		if (width > max_width) {
			max_width = width;
		}
		result_width += width;
		result_scale += scale;
	}
	D_ASSERT(max_width > 0);
	if (result_scale > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException(
		    "Needed scale %d to accurately represent the multiplication result, but this is out of range of the "
		    "DECIMAL type. Max scale is %d; could not perform an accurate multiplication. Either add a cast to DOUBLE, "
		    "or add an explicit cast to a decimal with a lower scale.",
		    result_scale, Decimal::MAX_WIDTH_DECIMAL);
	}
	if (result_width > Decimal::MAX_WIDTH_INT64 && max_width <= Decimal::MAX_WIDTH_INT64 &&
	    result_scale < Decimal::MAX_WIDTH_INT64) {
		bind_data->check_overflow = true;
		result_width = Decimal::MAX_WIDTH_INT64;
	}
	if (result_width > Decimal::MAX_WIDTH_DECIMAL) {
		bind_data->check_overflow = true;
		result_width = Decimal::MAX_WIDTH_DECIMAL;
	}
	LogicalType result_type = LogicalType::DECIMAL(result_width, result_scale);
	// since our scale is the summation of our input scales, we do not need to cast to the result scale
	// however, we might need to cast to the correct internal type
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument_type = arguments[i]->return_type;
		if (argument_type.InternalType() == result_type.InternalType()) {
			bound_function.arguments[i] = argument_type;
		} else {
			uint8_t width, scale;
			if (!argument_type.GetDecimalProperties(width, scale)) {
				scale = 0;
			}

			bound_function.arguments[i] = LogicalType::DECIMAL(result_width, scale);
		}
	}
	result_type.Verify();
	bound_function.return_type = result_type;
	// now select the physical function to execute
	if (bind_data->check_overflow) {
		bound_function.function = GetScalarBinaryFunction<DecimalMultiplyOverflowCheck>(result_type.InternalType());
	} else {
		bound_function.function = GetScalarBinaryFunction<MultiplyOperator>(result_type.InternalType());
	}
	if (result_type.InternalType() != PhysicalType::INT128) {
		bound_function.statistics =
		    PropagateNumericStats<TryDecimalMultiply, MultiplyPropagateStatistics, MultiplyOperator>;
	}
	return std::move(bind_data);
}

void MultiplyFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("*");
	for (auto &type : LogicalType::Numeric()) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			ScalarFunction function({type, type}, type, nullptr, BindDecimalMultiply);
			function.serialize = SerializeDecimalArithmetic;
			function.deserialize = DeserializeDecimalArithmetic<MultiplyOperator, DecimalMultiplyOverflowCheck>;
			functions.AddFunction(function);
		} else if (TypeIsIntegral(type.InternalType())) {
			functions.AddFunction(ScalarFunction(
			    {type, type}, type, GetScalarIntegerFunction<MultiplyOperatorOverflowCheck>(type.InternalType()),
			    nullptr, nullptr,
			    PropagateNumericStats<TryMultiplyOperator, MultiplyPropagateStatistics, MultiplyOperator>));
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, GetScalarBinaryFunction<MultiplyOperator>(type.InternalType())));
		}
	}
	functions.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::BIGINT}, LogicalType::INTERVAL,
	                   ScalarFunction::BinaryFunction<interval_t, int64_t, interval_t, MultiplyOperator>));
	functions.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::INTERVAL}, LogicalType::INTERVAL,
	                   ScalarFunction::BinaryFunction<int64_t, interval_t, interval_t, MultiplyOperator>));
	set.AddFunction(functions);

	functions.name = "multiply";
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// / [divide]
//===--------------------------------------------------------------------===//
template <>
float DivideOperator::Operation(float left, float right) {
	auto result = left / right;
	return result;
}

template <>
double DivideOperator::Operation(double left, double right) {
	auto result = left / right;
	return result;
}

template <>
hugeint_t DivideOperator::Operation(hugeint_t left, hugeint_t right) {
	if (right.lower == 0 && right.upper == 0) {
		throw InternalException("Hugeint division by zero!");
	}
	return left / right;
}

template <>
interval_t DivideOperator::Operation(interval_t left, int64_t right) {
	left.days = UnsafeNumericCast<int32_t>(left.days / right);
	left.months = UnsafeNumericCast<int32_t>(left.months / right);
	left.micros /= right;
	return left;
}

struct BinaryNumericDivideWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		if (left == NumericLimits<LEFT_TYPE>::Minimum() && right == -1) {
			throw OutOfRangeException("Overflow in division of %d / %d", left, right);
		} else if (right == 0) {
			mask.SetInvalid(idx);
			return left;
		} else {
			return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
		}
	}

	static bool AddsNulls() {
		return true;
	}
};

struct BinaryZeroIsNullWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		if (right == 0) {
			mask.SetInvalid(idx);
			return left;
		} else {
			return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
		}
	}

	static bool AddsNulls() {
		return true;
	}
};

struct BinaryNumericDivideHugeintWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		if (left == NumericLimits<LEFT_TYPE>::Minimum() && right == -1) {
			throw OutOfRangeException("Overflow in division of %s / %s", left.ToString(), right.ToString());
		} else if (right == 0) {
			mask.SetInvalid(idx);
			return left;
		} else {
			return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
		}
	}

	static bool AddsNulls() {
		return true;
	}
};

template <class TA, class TB, class TC, class OP, class ZWRAPPER = BinaryZeroIsNullWrapper>
static void BinaryScalarFunctionIgnoreZero(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<TA, TB, TC, OP, ZWRAPPER>(input.data[0], input.data[1], result, input.size());
}

template <class OP>
static scalar_function_t GetBinaryFunctionIgnoreZero(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
		return BinaryScalarFunctionIgnoreZero<int8_t, int8_t, int8_t, OP, BinaryNumericDivideWrapper>;
	case PhysicalType::INT16:
		return BinaryScalarFunctionIgnoreZero<int16_t, int16_t, int16_t, OP, BinaryNumericDivideWrapper>;
	case PhysicalType::INT32:
		return BinaryScalarFunctionIgnoreZero<int32_t, int32_t, int32_t, OP, BinaryNumericDivideWrapper>;
	case PhysicalType::INT64:
		return BinaryScalarFunctionIgnoreZero<int64_t, int64_t, int64_t, OP, BinaryNumericDivideWrapper>;
	case PhysicalType::UINT8:
		return BinaryScalarFunctionIgnoreZero<uint8_t, uint8_t, uint8_t, OP>;
	case PhysicalType::UINT16:
		return BinaryScalarFunctionIgnoreZero<uint16_t, uint16_t, uint16_t, OP>;
	case PhysicalType::UINT32:
		return BinaryScalarFunctionIgnoreZero<uint32_t, uint32_t, uint32_t, OP>;
	case PhysicalType::UINT64:
		return BinaryScalarFunctionIgnoreZero<uint64_t, uint64_t, uint64_t, OP>;
	case PhysicalType::INT128:
		return BinaryScalarFunctionIgnoreZero<hugeint_t, hugeint_t, hugeint_t, OP, BinaryNumericDivideHugeintWrapper>;
	case PhysicalType::UINT128:
		return BinaryScalarFunctionIgnoreZero<uhugeint_t, uhugeint_t, uhugeint_t, OP>;
	case PhysicalType::FLOAT:
		return BinaryScalarFunctionIgnoreZero<float, float, float, OP>;
	case PhysicalType::DOUBLE:
		return BinaryScalarFunctionIgnoreZero<double, double, double, OP>;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarUnaryFunction");
	}
}

template <class OP>
unique_ptr<FunctionData> BindBinaryFloatingPoint(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	auto &config = ClientConfig::GetConfig(context);
	if (config.ieee_floating_point_ops) {
		bound_function.function = GetScalarBinaryFunction<OP>(bound_function.return_type.InternalType());
	} else {
		bound_function.function = GetBinaryFunctionIgnoreZero<OP>(bound_function.return_type.InternalType());
	}
	return nullptr;
}

void DivideFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet fp_divide("/");
	fp_divide.AddFunction(ScalarFunction({LogicalType::FLOAT, LogicalType::FLOAT}, LogicalType::FLOAT, nullptr,
	                                     BindBinaryFloatingPoint<DivideOperator>));
	fp_divide.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE, nullptr,
	                                     BindBinaryFloatingPoint<DivideOperator>));
	fp_divide.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::BIGINT}, LogicalType::INTERVAL,
	                   BinaryScalarFunctionIgnoreZero<interval_t, int64_t, interval_t, DivideOperator>));
	set.AddFunction(fp_divide);

	ScalarFunctionSet full_divide("//");
	for (auto &type : LogicalType::Numeric()) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			continue;
		} else {
			full_divide.AddFunction(
			    ScalarFunction({type, type}, type, GetBinaryFunctionIgnoreZero<DivideOperator>(type.InternalType())));
		}
	}
	set.AddFunction(full_divide);

	full_divide.name = "divide";
	set.AddFunction(full_divide);
}

//===--------------------------------------------------------------------===//
// % [modulo]
//===--------------------------------------------------------------------===//
template <class OP>
unique_ptr<FunctionData> BindDecimalModulo(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindDecimalArithmetic<true>(context, bound_function, arguments);
	// now select the physical function to execute
	if (bind_data->check_overflow) {
		// fallback to DOUBLE if the decimal type is not guaranteed to fit within the max decimal width
		for (auto &arg : bound_function.arguments) {
			arg = LogicalType::DOUBLE;
		}
		bound_function.return_type = LogicalType::DOUBLE;
	}
	auto &result_type = bound_function.return_type;
	bound_function.function = GetBinaryFunctionIgnoreZero<OP>(result_type.InternalType());
	return std::move(bind_data);
}

template <>
float ModuloOperator::Operation(float left, float right) {
	auto result = std::fmod(left, right);
	return result;
}

template <>
double ModuloOperator::Operation(double left, double right) {
	auto result = std::fmod(left, right);
	return result;
}

template <>
hugeint_t ModuloOperator::Operation(hugeint_t left, hugeint_t right) {
	if (right.lower == 0 && right.upper == 0) {
		throw InternalException("Hugeint division by zero!");
	}
	return left % right;
}

void ModFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("%");
	for (auto &type : LogicalType::Numeric()) {
		if (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE) {
			functions.AddFunction(ScalarFunction({type, type}, type, nullptr, BindBinaryFloatingPoint<ModuloOperator>));
		} else if (type.id() == LogicalTypeId::DECIMAL) {
			functions.AddFunction(ScalarFunction({type, type}, type, nullptr, BindDecimalModulo<ModuloOperator>));
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, GetBinaryFunctionIgnoreZero<ModuloOperator>(type.InternalType())));
		}
	}
	set.AddFunction(functions);
	functions.name = "mod";
	set.AddFunction(functions);
}

} // namespace duckdb
