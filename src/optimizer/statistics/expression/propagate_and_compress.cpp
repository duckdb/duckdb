#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/common/operator/subtract.hpp"

namespace duckdb {

template <class T>
bool GetCastType(T signed_range, LogicalType &cast_type) {
	auto range = static_cast<typename std::make_unsigned<decltype(signed_range)>::type>(signed_range);

	// Check if this range fits in a smaller type
	if (range < NumericLimits<uint8_t>::Maximum()) {
		cast_type = LogicalType::UTINYINT;
	} else if (sizeof(T) > sizeof(uint16_t) && range < NumericLimits<uint16_t>::Maximum()) {
		cast_type = LogicalType::USMALLINT;
	} else if (sizeof(T) > sizeof(uint32_t) && range < NumericLimits<uint32_t>::Maximum()) {
		cast_type = LogicalType::UINTEGER;
	} else {
		return false;
	}
	return true;
}

template <>
bool GetCastType(hugeint_t range, LogicalType &cast_type) {
	if (range < NumericLimits<uint8_t>().Maximum()) {
		cast_type = LogicalType::UTINYINT;
	} else if (range < NumericLimits<uint16_t>().Maximum()) {
		cast_type = LogicalType::USMALLINT;
	} else if (range < NumericLimits<uint32_t>().Maximum()) {
		cast_type = LogicalType::UINTEGER;
	} else if (range < NumericLimits<uint64_t>().Maximum()) {
		cast_type = LogicalType::UBIGINT;
	} else {
		return false;
	}
	return true;
}

template <class T>
unique_ptr<Expression> TemplatedCastToSmallestType(unique_ptr<Expression> expr, NumericStatistics &num_stats) {
	// Compute range
	if (num_stats.min.IsNull() || num_stats.max.IsNull()) {
		return expr;
	}

	auto signed_min_val = num_stats.min.GetValue<T>();
	auto signed_max_val = num_stats.max.GetValue<T>();
	if (signed_max_val < signed_min_val) {
		return expr;
	}

	// Compute range, cast to unsigned to prevent comparing signed with unsigned
	T signed_range;
	if (!TrySubtractOperator::Operation(signed_max_val, signed_min_val, signed_range)) {
		// overflow in subtraction: cannot do any simplification
		return expr;
	}

	// Check if this range fits in a smaller type
	LogicalType cast_type;
	if (!GetCastType(signed_range, cast_type)) {
		return expr;
	}

	// Create expression to map to a smaller range
	auto input_type = expr->return_type;
	auto minimum_expr = make_unique<BoundConstantExpression>(Value::CreateValue(signed_min_val));
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(expr));
	arguments.push_back(std::move(minimum_expr));
	auto minus_expr = make_unique<BoundFunctionExpression>(input_type, SubtractFun::GetFunction(input_type, input_type),
	                                                       std::move(arguments), nullptr, true);

	// Cast to smaller type
	return BoundCastExpression::AddDefaultCastToType(std::move(minus_expr), cast_type);
}

unique_ptr<Expression> CastToSmallestType(unique_ptr<Expression> expr, NumericStatistics &num_stats) {
	auto physical_type = expr->return_type.InternalType();
	switch (physical_type) {
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
		return expr;
	case PhysicalType::UINT16:
		return TemplatedCastToSmallestType<uint16_t>(std::move(expr), num_stats);
	case PhysicalType::INT16:
		return TemplatedCastToSmallestType<int16_t>(std::move(expr), num_stats);
	case PhysicalType::UINT32:
		return TemplatedCastToSmallestType<uint32_t>(std::move(expr), num_stats);
	case PhysicalType::INT32:
		return TemplatedCastToSmallestType<int32_t>(std::move(expr), num_stats);
	case PhysicalType::UINT64:
		return TemplatedCastToSmallestType<uint64_t>(std::move(expr), num_stats);
	case PhysicalType::INT64:
		return TemplatedCastToSmallestType<int64_t>(std::move(expr), num_stats);
	case PhysicalType::INT128:
		return TemplatedCastToSmallestType<hugeint_t>(std::move(expr), num_stats);
	default:
		throw NotImplementedException("Unknown integer type!");
	}
}

void StatisticsPropagator::PropagateAndCompress(unique_ptr<Expression> &expr, unique_ptr<BaseStatistics> &stats) {
	stats = PropagateExpression(expr);
	if (stats) {
		if (expr->return_type.IsIntegral()) {
			expr = CastToSmallestType(std::move(expr), (NumericStatistics &)*stats);
		}
	}
}

} // namespace duckdb
