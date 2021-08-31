#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

static void CastHugeintToSmallestType(unique_ptr<Expression> &expr) {
	// Compute range
	auto &num_stats = (NumericStatistics &)*expr->stats;
	if (num_stats.min.is_null || num_stats.max.is_null) {
		return;
	}
	auto range = num_stats.max.GetValue<hugeint_t>() - num_stats.min.GetValue<hugeint_t>();

	// Check if this range fits in a smaller type
	if (range < NumericLimits<uint8_t>().Maximum()) {

	} else if (sizeof(hugeint_t) > sizeof(uint16_t) && range < NumericLimits<uint16_t>().Maximum()) {

	} else if (sizeof(hugeint_t) > sizeof(uint32_t) && range < NumericLimits<uint32_t>().Maximum()) {

	} else if (sizeof(hugeint_t) > sizeof(uint64_t) && range < NumericLimits<uint64_t>().Maximum()) {
	}
}

template <class T>
static void TemplatedCastToSmallestType(unique_ptr<Expression> &expr) {
	// Compute range
	auto &num_stats = (NumericStatistics &)*expr->stats;
	if (num_stats.min.is_null || num_stats.max.is_null) {
		return;
	}
	auto signed_range = num_stats.max.GetValue<T>() - num_stats.min.GetValue<T>();
	auto range = static_cast<typename std::make_unsigned<decltype(signed_range)>::type>(signed_range);

	// Check if this range fits in a smaller type
	if (range < NumericLimits<uint8_t>().Maximum()) {
		//		auto minus_expr = make_unique<BoundFunctionExpression>(LogicalType::UTINYINT, );
	} else if (sizeof(T) > sizeof(uint16_t) && range < NumericLimits<uint16_t>().Maximum()) {

	} else if (sizeof(T) > sizeof(uint32_t) && range < NumericLimits<uint32_t>().Maximum()) {
	}
}

static void CastToSmallestType(unique_ptr<Expression> &expr) {
	auto physical_type = expr->return_type.InternalType();
	switch (physical_type) {
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
		return;
	case PhysicalType::UINT16:
		return TemplatedCastToSmallestType<uint16_t>(expr);
	case PhysicalType::INT16:
		return TemplatedCastToSmallestType<int16_t>(expr);
	case PhysicalType::UINT32:
		return TemplatedCastToSmallestType<uint32_t>(expr);
	case PhysicalType::INT32:
		return TemplatedCastToSmallestType<int32_t>(expr);
	case PhysicalType::UINT64:
		return TemplatedCastToSmallestType<uint64_t>(expr);
	case PhysicalType::INT64:
		return TemplatedCastToSmallestType<int16_t>(expr);
	case PhysicalType::INT128:
		return CastHugeintToSmallestType(expr);
	default:
		throw NotImplementedException("Unknown integer type!");
	}
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalOrder &order,
                                                                     unique_ptr<LogicalOperator> *node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(order.children[0]);

	// then propagate to each of the order expressions
	for (auto &bound_order : order.orders) {
		auto &expr = bound_order.expression;
		PropagateExpression(expr);
		if (expr->stats) {
			if (expr->return_type.IsIntegral()) {
				CastToSmallestType(bound_order.expression);
			}
			bound_order.stats = expr->stats->Copy();
		} else {
			bound_order.stats = nullptr;
		}
	}
	return move(node_stats);
}

} // namespace duckdb
