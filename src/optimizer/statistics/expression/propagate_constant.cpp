#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/struct_statistics.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::StatisticsFromValue(const Value &input) {
	unique_ptr<BaseStatistics> result;
	switch (input.type().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE: {
		auto stats = make_unique<NumericStatistics>(input.type(), input, input);
		stats->Set(input.IsNull() ? StatsInfo::CAN_HAVE_NULL_VALUES : StatsInfo::CANNOT_HAVE_NULL_VALUES);
		stats->SetDistinctCount(1);
		result = std::move(stats);
		break;
	}
	case PhysicalType::VARCHAR: {
		auto stats = make_unique<StringStatistics>(input.type());
		stats->SetDistinctCount(1);
		if (!input.IsNull()) {
			auto &string_value = StringValue::Get(input);
			stats->Update(string_t(string_value));
		}
		result = std::move(stats);
		break;
	}
	case PhysicalType::STRUCT: {
		auto stats = make_unique<StructStatistics>(input.type());
		if (input.IsNull()) {
			for (auto &child_stat : stats->child_stats) {
				child_stat.reset();
			}
		} else {
			auto &struct_children = StructValue::GetChildren(input);
			D_ASSERT(stats->child_stats.size() == struct_children.size());
			for (idx_t i = 0; i < stats->child_stats.size(); i++) {
				stats->child_stats[i] = StatisticsFromValue(struct_children[i]);
			}
		}
		result = std::move(stats);
		break;
	}
	case PhysicalType::LIST: {
		auto stats = make_unique<ListStatistics>(input.type());
		if (input.IsNull()) {
			stats->child_stats.reset();
		} else {
			auto &list_children = ListValue::GetChildren(input);
			for (auto &child_element : list_children) {
				auto child_element_stats = StatisticsFromValue(child_element);
				if (child_element_stats) {
					stats->child_stats->Merge(*child_element_stats);
				} else {
					stats->child_stats.reset();
				}
			}
		}
		result = std::move(stats);
		break;
	}
	default:
		return nullptr;
	}
	if (input.IsNull()) {
		result->Set(StatsInfo::CAN_HAVE_NULL_VALUES);
		result->Set(StatsInfo::CANNOT_HAVE_VALID_VALUES);
	} else {
		result->Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		result->Set(StatsInfo::CAN_HAVE_VALID_VALUES);
	}
	return result;
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundConstantExpression &constant,
                                                                     unique_ptr<Expression> *expr_ptr) {
	return StatisticsFromValue(constant.value);
}

} // namespace duckdb
