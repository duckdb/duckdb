#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/struct_statistics.hpp"
#include "duckdb/storage/statistics/list_statistics.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::StatisticsFromValue(const Value &input) {
	switch (input.type().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE: {
		auto result = make_unique<NumericStatistics>(input.type(), input, input);
		result->validity_stats = make_unique<ValidityStatistics>(input.is_null, !input.is_null);
		return move(result);
	}
	case PhysicalType::VARCHAR: {
		auto result = make_unique<StringStatistics>(input.type());
		result->validity_stats = make_unique<ValidityStatistics>(input.is_null, !input.is_null);
		string_t str(input.str_value.c_str(), input.str_value.size());
		result->Update(str);
		return move(result);
	}
	case PhysicalType::STRUCT: {
		auto result = make_unique<StructStatistics>(input.type());
		result->validity_stats = make_unique<ValidityStatistics>(input.is_null, !input.is_null);
		if (input.is_null) {
			for (auto &child_stat : result->child_stats) {
				child_stat.reset();
			}
		} else {
			D_ASSERT(result->child_stats.size() == input.struct_value.size());
			for (idx_t i = 0; i < result->child_stats.size(); i++) {
				result->child_stats[i] = StatisticsFromValue(input.struct_value[i]);
			}
		}
		return move(result);
	}
	case PhysicalType::LIST: {
		auto result = make_unique<ListStatistics>(input.type());
		result->validity_stats = make_unique<ValidityStatistics>(input.is_null, !input.is_null);
		if (input.is_null) {
			result->child_stats.reset();
		} else {
			for (auto &child_element : input.list_value) {
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
