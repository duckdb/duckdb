#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"

namespace duckdb {

static unique_ptr<BaseStatistics> StatisticsOperationsNumericNumericCast(const BaseStatistics &input,
                                                                         const LogicalType &target) {
	// Bail out if the stats are not numeric
	if (input.GetStatsType() != StatisticsType::NUMERIC_STATS) {
		return nullptr;
	}
	if (!NumericStats::HasMinMax(input)) {
		return nullptr;
	}
	Value min = NumericStats::Min(input);
	Value max = NumericStats::Max(input);
	if (!min.DefaultTryCastAs(target) || !max.DefaultTryCastAs(target)) {
		// overflow in cast: bailout
		return nullptr;
	}
	auto result = NumericStats::CreateEmpty(target);
	result.CopyBase(input);
	NumericStats::SetMin(result, min);
	NumericStats::SetMax(result, max);
	return result.ToUnique();
}

bool StatisticsPropagator::CanPropagateCast(const LogicalType &source, const LogicalType &target) {
	if (source == target) {
		return true;
	}
	// we can only propagate numeric -> numeric
	switch (source.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		break;
	default:
		return false;
	}
	switch (target.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		break;
	default:
		return false;
	}
	// for time/timestamps/dates - there are various limitations on what we can propagate
	//	Downcasting timestamps to times is not a truncation operation
	switch (target.id()) {
	case LogicalTypeId::TIME: {
		switch (source.id()) {
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_TZ:
			return false;
		default:
			break;
		}
		break;
	}
	// FIXME: perform actual stats propagation for these casts
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		const bool to_timestamp = target.id() == LogicalTypeId::TIMESTAMP;
		const bool to_timestamp_tz = target.id() == LogicalTypeId::TIMESTAMP_TZ;
		//  Casting to timestamp[_tz] (us) from a different unit can not re-use stats
		switch (source.id()) {
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_SEC:
			return false;
		case LogicalTypeId::TIMESTAMP: {
			if (to_timestamp_tz) {
				// Both use INT64 physical type, but should not be treated equal
				return false;
			}
			break;
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			if (to_timestamp) {
				// Both use INT64 physical type, but should not be treated equal
				return false;
			}
			break;
		}
		default:
			break;
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		// Same as above ^
		switch (source.id()) {
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_SEC:
			return false;
		default:
			break;
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP_MS: {
		// Same as above ^
		switch (source.id()) {
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_SEC:
			return false;
		default:
			break;
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP_SEC: {
		// Same as above ^
		switch (source.id()) {
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_MS:
			return false;
		default:
			break;
		}
		break;
	}
	default:
		break;
	}
	// we can propagate!
	return true;
}

static unique_ptr<BaseStatistics> StatisticsPropagateVariant(const BaseStatistics &input, const LogicalType &target) {
	if (target.IsNested() || target.id() == LogicalTypeId::VARIANT) {
		// only try this for non-nested
		return nullptr;
	}
	if (!VariantStats::IsShredded(input)) {
		// not shredded
		return nullptr;
	}
	auto structured_type = VariantStats::GetShreddedStructuredType(input);
	auto &shredded_stats = VariantStats::GetShreddedStats(input);
	if (!VariantShreddedStats::IsFullyShredded(shredded_stats)) {
		// this field might be partially shredded - skip stats propagation
		return nullptr;
	}
	// extract the typed stats
	auto &typed_stats = VariantStats::GetTypedStats(shredded_stats);
	if (structured_type == target) {
		// type matches - return stats directly
		return typed_stats.ToUnique();
	}
	// typed stats don't match - try to cast
	return StatisticsPropagator::TryPropagateCast(typed_stats, structured_type, target);
}

unique_ptr<BaseStatistics> StatisticsPropagator::TryPropagateCast(const BaseStatistics &stats,
                                                                  const LogicalType &source,
                                                                  const LogicalType &target) {
	if (source.id() == LogicalTypeId::VARIANT) {
		return StatisticsPropagateVariant(stats, target);
	}
	if (!CanPropagateCast(source, target)) {
		return nullptr;
	}
	return StatisticsOperationsNumericNumericCast(stats, target);
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundCastExpression &cast,
                                                                     unique_ptr<Expression> &expr_ptr) {
	auto child_stats = PropagateExpression(cast.child);
	if (!child_stats) {
		return nullptr;
	}
	auto result_stats = TryPropagateCast(*child_stats, cast.child->return_type, cast.return_type);
	if (cast.try_cast && result_stats) {
		result_stats->Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	}
	return result_stats;
}

} // namespace duckdb
