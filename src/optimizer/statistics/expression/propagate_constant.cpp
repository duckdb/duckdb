#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/struct_statistics.hpp"

namespace duckdb {

void UpdateDistinctStats(BaseStatistics &distinct_stats, const Value &input) {
	Vector v(input);
	auto &d_stats = (DistinctStatistics &)distinct_stats;
	d_stats.Update(v, 1);
}

unique_ptr<BaseStatistics> StatisticsPropagator::StatisticsFromValue(const Value &input) {
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
		auto result = make_unique<NumericStatistics>(input.type(), input, input, StatisticsType::GLOBAL_STATS);
		result->validity_stats = make_unique<ValidityStatistics>(input.IsNull(), !input.IsNull());
		UpdateDistinctStats(*result->distinct_stats, input);
		return move(result);
	}
	case PhysicalType::VARCHAR: {
		auto result = make_unique<StringStatistics>(input.type(), StatisticsType::GLOBAL_STATS);
		result->validity_stats = make_unique<ValidityStatistics>(input.IsNull(), !input.IsNull());
		UpdateDistinctStats(*result->distinct_stats, input);
		if (!input.IsNull()) {
			auto &string_value = StringValue::Get(input);
			result->Update(string_t(string_value));
		}
		return move(result);
	}
	case PhysicalType::STRUCT: {
		auto result = make_unique<StructStatistics>(input.type());
		result->validity_stats = make_unique<ValidityStatistics>(input.IsNull(), !input.IsNull());
		if (input.IsNull()) {
			for (auto &child_stat : result->child_stats) {
				child_stat.reset();
			}
		} else {
			auto &struct_children = StructValue::GetChildren(input);
			D_ASSERT(result->child_stats.size() == struct_children.size());
			for (idx_t i = 0; i < result->child_stats.size(); i++) {
				result->child_stats[i] = StatisticsFromValue(struct_children[i]);
			}
		}
		return move(result);
	}
	case PhysicalType::LIST: {
		auto result = make_unique<ListStatistics>(input.type());
		result->validity_stats = make_unique<ValidityStatistics>(input.IsNull(), !input.IsNull());
		if (input.IsNull()) {
			result->child_stats.reset();
		} else {
			auto &list_children = ListValue::GetChildren(input);
			for (auto &child_element : list_children) {
				auto child_element_stats = StatisticsFromValue(child_element);
				if (child_element_stats) {
					result->child_stats->Merge(*child_element_stats);
				} else {
					result->child_stats.reset();
				}
			}
		}
		return move(result);
	}
	default:
		return nullptr;
	}
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundConstantExpression &constant,
                                                                     unique_ptr<Expression> *expr_ptr) {
	return StatisticsFromValue(constant.value);
}

} // namespace duckdb
