#include "duckdb/function/scalar/operator_functions.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/abs.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

static unique_ptr<BaseStatistics> PropagateAbsStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 1);
	auto &lstats = child_stats[0];
	Value new_min, new_max;
	bool potential_overflow = true;
	if (NumericStats::HasMinMax(lstats)) {
		switch (expr.GetReturnType().InternalType()) {
		case PhysicalType::FLOAT:
		case PhysicalType::DOUBLE:
			potential_overflow = false;
			break;
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
		new_min = Value(expr.GetReturnType());
		new_max = Value(expr.GetReturnType());
	} else {
		switch (expr.GetReturnType().InternalType()) {
		case PhysicalType::FLOAT:
		case PhysicalType::DOUBLE: {
			auto current_min = NumericStats::Min(lstats).GetValue<double>();
			auto current_max = NumericStats::Max(lstats).GetValue<double>();
			if (Value::IsNan(current_min) || Value::IsNan(current_max)) {
				return nullptr;
			}

			double min_val, max_val;
			if (current_min == 0 || current_max == 0) {
				min_val = AbsOperator::Operation<double, double>(current_min);
				max_val = AbsOperator::Operation<double, double>(current_max);
			} else if (current_min < 0 && current_max < 0) {
				min_val = AbsOperator::Operation<double, double>(current_max);
				max_val = AbsOperator::Operation<double, double>(current_min);
			} else if (current_min < 0) {
				D_ASSERT(current_max >= 0);
				min_val = 0;
				max_val = MaxValue(AbsOperator::Operation<double, double>(current_min), current_max);
			} else {
				*input.expr_ptr = std::move(input.expr.GetChildrenMutable()[0]);
				return child_stats[0].ToUnique();
			}
			new_min = expr.GetReturnType().InternalType() == PhysicalType::FLOAT
			              ? Value::FLOAT(static_cast<float>(min_val))
			              : Value::DOUBLE(min_val);
			new_max = expr.GetReturnType().InternalType() == PhysicalType::FLOAT
			              ? Value::FLOAT(static_cast<float>(max_val))
			              : Value::DOUBLE(max_val);
			break;
		}
		default: {
			auto current_min = NumericStats::Min(lstats).GetValue<int64_t>();
			auto current_max = NumericStats::Max(lstats).GetValue<int64_t>();

			int64_t min_val, max_val;
			if (current_min < 0 && current_max < 0) {
				min_val = AbsValue(current_max);
				max_val = AbsValue(current_min);
			} else if (current_min < 0) {
				D_ASSERT(current_max >= 0);
				min_val = 0;
				max_val = MaxValue(AbsValue(current_min), current_max);
			} else {
				*input.expr_ptr = std::move(input.expr.GetChildrenMutable()[0]);
				return child_stats[0].ToUnique();
			}
			new_min = Value::Numeric(expr.GetReturnType(), min_val);
			new_max = Value::Numeric(expr.GetReturnType(), max_val);
			break;
		}
		}
		expr.FunctionMutable().SetFunctionCallback(
		    ScalarFunction::GetScalarUnaryFunction<AbsOperator>(expr.GetReturnType()));
	}
	auto stats = NumericStats::CreateEmpty(expr.GetReturnType());
	NumericStats::SetMin(stats, new_min);
	NumericStats::SetMax(stats, new_max);
	stats.CopyValidity(lstats);
	return stats.ToUnique();
}

template <class OP>
static unique_ptr<FunctionData> DecimalUnaryOpBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto decimal_type = arguments[0]->GetReturnType();
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		bound_function.SetFunctionCallback(ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::SMALLINT));
		break;
	case PhysicalType::INT32:
		bound_function.SetFunctionCallback(ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::INTEGER));
		break;
	case PhysicalType::INT64:
		bound_function.SetFunctionCallback(ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::BIGINT));
		break;
	default:
		bound_function.SetFunctionCallback(ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::HUGEINT));
		break;
	}
	bound_function.GetArguments()[0] = decimal_type;
	bound_function.SetReturnType(decimal_type);
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
			function.SetStatisticsCallback(PropagateAbsStats);
			abs.AddFunction(function);
			break;
		}
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE: {
			ScalarFunction function({type}, type, ScalarFunction::GetScalarUnaryFunction<AbsOperator>(type));
			function.SetStatisticsCallback(PropagateAbsStats);
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
	abs.SetFallible();
	return abs;
}

} // namespace duckdb
