#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

unique_ptr<Expression> CastHugeintToSmallestType(unique_ptr<Expression> expr, NumericStatistics &num_stats) {
	// Compute range
	if (num_stats.min.IsNull() || num_stats.max.IsNull()) {
		return expr;
	}

	auto min_val = num_stats.min.GetValue<hugeint_t>();
	auto max_val = num_stats.max.GetValue<hugeint_t>();
	if (max_val < min_val) {
		return expr;
	}

	// Prevent overflow
	if (min_val < NumericLimits<int64_t>().Minimum() && max_val > NumericLimits<int64_t>().Maximum()) {
		return expr;
	}

	// Compute range
	auto range = max_val - min_val;

	// Check if this range fits in a smaller type
	LogicalType cast_type;
	if (range < NumericLimits<uint8_t>().Maximum()) {
		cast_type = LogicalType::UTINYINT;
	} else if (range < NumericLimits<uint16_t>().Maximum()) {
		cast_type = LogicalType::USMALLINT;
	} else if (range < NumericLimits<uint32_t>().Maximum()) {
		cast_type = LogicalType::UINTEGER;
	} else if (range < NumericLimits<uint64_t>().Maximum()) {
		cast_type = LogicalTypeId::UBIGINT;
	} else {
		return expr;
	}

	// Create expression to map to a smaller range
	auto input_type = expr->return_type;
	auto minimum_expr = make_unique<BoundConstantExpression>(Value::CreateValue(min_val));
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(move(expr));
	arguments.push_back(move(minimum_expr));
	auto minus_expr = make_unique<BoundFunctionExpression>(input_type, SubtractFun::GetFunction(input_type, input_type),
	                                                       move(arguments), nullptr, true);

	// Cast to smaller type
	return make_unique<BoundCastExpression>(move(minus_expr), cast_type);
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

	// Prevent signed integer overflow - we can't range map these
	if (std::is_signed<T>() && signed_min_val < -((T)1 << (sizeof(T) * 8 - 2)) &&
	    signed_max_val > ((T)1 << (sizeof(T) * 8 - 2))) {
		return expr;
	}

	// Compute range, cast to unsigned to prevent comparing signed with unsigned
	auto signed_range = signed_max_val - signed_min_val;
	auto range = static_cast<typename std::make_unsigned<decltype(signed_range)>::type>(signed_range);

	// Check if this range fits in a smaller type
	LogicalType cast_type;
	if (range < NumericLimits<uint8_t>().Maximum()) {
		cast_type = LogicalType::UTINYINT;
	} else if (sizeof(T) > sizeof(uint16_t) && range < NumericLimits<uint16_t>().Maximum()) {
		cast_type = LogicalType::USMALLINT;
	} else if (sizeof(T) > sizeof(uint32_t) && range < NumericLimits<uint32_t>().Maximum()) {
		cast_type = LogicalType::UINTEGER;
	} else {
		return expr;
	}

	// Create expression to map to a smaller range
	auto input_type = expr->return_type;
	auto minimum_expr = make_unique<BoundConstantExpression>(Value::CreateValue(signed_min_val));
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(move(expr));
	arguments.push_back(move(minimum_expr));
	auto minus_expr = make_unique<BoundFunctionExpression>(input_type, SubtractFun::GetFunction(input_type, input_type),
	                                                       move(arguments), nullptr, true);

	// Cast to smaller type
	return make_unique<BoundCastExpression>(move(minus_expr), cast_type);
}

unique_ptr<Expression> CastToSmallestType(unique_ptr<Expression> expr, NumericStatistics &num_stats) {
	auto physical_type = expr->return_type.InternalType();
	switch (physical_type) {
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
		return expr;
	case PhysicalType::UINT16:
		return TemplatedCastToSmallestType<uint16_t>(move(expr), num_stats);
	case PhysicalType::INT16:
		return TemplatedCastToSmallestType<int16_t>(move(expr), num_stats);
	case PhysicalType::UINT32:
		return TemplatedCastToSmallestType<uint32_t>(move(expr), num_stats);
	case PhysicalType::INT32:
		return TemplatedCastToSmallestType<int32_t>(move(expr), num_stats);
	case PhysicalType::UINT64:
		return TemplatedCastToSmallestType<uint64_t>(move(expr), num_stats);
	case PhysicalType::INT64:
		return TemplatedCastToSmallestType<int64_t>(move(expr), num_stats);
	case PhysicalType::INT128:
		return CastHugeintToSmallestType(move(expr), num_stats);
	default:
		throw NotImplementedException("Unknown integer type!");
	}
}

void StatisticsPropagator::PropagateAndCompress(unique_ptr<Expression> &expr, unique_ptr<BaseStatistics> &stats) {
	stats = PropagateExpression(expr);
	if (stats) {
		if (expr->return_type.IsIntegral()) {
			expr = CastToSmallestType(move(expr), (NumericStatistics &)*stats);
		}
	}
}

} // namespace duckdb
