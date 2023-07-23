#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

static uint32_t RequiredBitsForValue(uint32_t n) {
	idx_t required_bits = 0;
	while (n > 0) {
		n >>= 1;
		required_bits++;
	}
	return required_bits;
}

template <class T>
hugeint_t GetRangeHugeint(const BaseStatistics &nstats) {
	return Hugeint::Convert(NumericStats::GetMax<T>(nstats)) - Hugeint::Convert(NumericStats::GetMin<T>(nstats));
}

static bool CanUsePerfectHashAggregate(ClientContext &context, LogicalAggregate &op, vector<idx_t> &bits_per_group) {
	if (op.grouping_sets.size() > 1 || !op.grouping_functions.empty()) {
		return false;
	}
	idx_t perfect_hash_bits = 0;
	if (op.group_stats.empty()) {
		op.group_stats.resize(op.groups.size());
	}
	for (idx_t group_idx = 0; group_idx < op.groups.size(); group_idx++) {
		auto &group = op.groups[group_idx];
		auto &stats = op.group_stats[group_idx];

		switch (group->return_type.InternalType()) {
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
			break;
		default:
			// we only support simple integer types for perfect hashing
			return false;
		}
		// check if the group has stats available
		auto &group_type = group->return_type;
		if (!stats) {
			// no stats, but we might still be able to use perfect hashing if the type is small enough
			// for small types we can just set the stats to [type_min, type_max]
			switch (group_type.InternalType()) {
			case PhysicalType::INT8:
			case PhysicalType::INT16:
			case PhysicalType::UINT8:
			case PhysicalType::UINT16:
				break;
			default:
				// type is too large and there are no stats: skip perfect hashing
				return false;
			}
			// construct stats with the min and max value of the type
			stats = NumericStats::CreateUnknown(group_type).ToUnique();
			NumericStats::SetMin(*stats, Value::MinimumValue(group_type));
			NumericStats::SetMax(*stats, Value::MaximumValue(group_type));
		}
		auto &nstats = *stats;

		if (!NumericStats::HasMinMax(nstats)) {
			return false;
		}

		if (NumericStats::Max(*stats) < NumericStats::Min(*stats)) {
			// May result in underflow
			return false;
		}

		// we have a min and a max value for the stats: use that to figure out how many bits we have
		// we add two here, one for the NULL value, and one to make the computation one-indexed
		// (e.g. if min and max are the same, we still need one entry in total)
		hugeint_t range_h;
		switch (group_type.InternalType()) {
		case PhysicalType::INT8:
			range_h = GetRangeHugeint<int8_t>(nstats);
			break;
		case PhysicalType::INT16:
			range_h = GetRangeHugeint<int16_t>(nstats);
			break;
		case PhysicalType::INT32:
			range_h = GetRangeHugeint<int32_t>(nstats);
			break;
		case PhysicalType::INT64:
			range_h = GetRangeHugeint<int64_t>(nstats);
			break;
		case PhysicalType::UINT8:
			range_h = GetRangeHugeint<uint8_t>(nstats);
			break;
		case PhysicalType::UINT16:
			range_h = GetRangeHugeint<uint16_t>(nstats);
			break;
		case PhysicalType::UINT32:
			range_h = GetRangeHugeint<uint32_t>(nstats);
			break;
		case PhysicalType::UINT64:
			range_h = GetRangeHugeint<uint64_t>(nstats);
			break;
		default:
			throw InternalException("Unsupported type for perfect hash (should be caught before)");
		}

		uint64_t range;
		if (!Hugeint::TryCast(range_h, range)) {
			return false;
		}

		// bail out on any range bigger than 2^32
		if (range >= NumericLimits<int32_t>::Maximum()) {
			return false;
		}

		range += 2;
		// figure out how many bits we need
		idx_t required_bits = RequiredBitsForValue(range);
		bits_per_group.push_back(required_bits);
		perfect_hash_bits += required_bits;
		// check if we have exceeded the bits for the hash
		if (perfect_hash_bits > ClientConfig::GetConfig(context).perfect_ht_threshold) {
			// too many bits for perfect hash
			return false;
		}
	}
	for (auto &expression : op.expressions) {
		auto &aggregate = expression->Cast<BoundAggregateExpression>();
		if (aggregate.IsDistinct() || !aggregate.function.combine) {
			// distinct aggregates are not supported in perfect hash aggregates
			return false;
		}
	}
	return true;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAggregate &op) {
	unique_ptr<PhysicalOperator> groupby;
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	plan = ExtractAggregateExpressions(std::move(plan), op.expressions, op.groups);

	if (op.groups.empty() && op.grouping_sets.size() <= 1) {
		// no groups, check if we can use a simple aggregation
		// special case: aggregate entire columns together
		bool use_simple_aggregation = true;
		for (auto &expression : op.expressions) {
			auto &aggregate = expression->Cast<BoundAggregateExpression>();
			if (!aggregate.function.simple_update) {
				// unsupported aggregate for simple aggregation: use hash aggregation
				use_simple_aggregation = false;
				break;
			}
		}
		if (use_simple_aggregation) {
			groupby = make_uniq_base<PhysicalOperator, PhysicalUngroupedAggregate>(op.types, std::move(op.expressions),
			                                                                       op.estimated_cardinality);
		} else {
			groupby = make_uniq_base<PhysicalOperator, PhysicalHashAggregate>(
			    context, op.types, std::move(op.expressions), op.estimated_cardinality);
		}
	} else {
		// groups! create a GROUP BY aggregator
		// use a perfect hash aggregate if possible
		vector<idx_t> required_bits;
		if (CanUsePerfectHashAggregate(context, op, required_bits)) {
			groupby = make_uniq_base<PhysicalOperator, PhysicalPerfectHashAggregate>(
			    context, op.types, std::move(op.expressions), std::move(op.groups), std::move(op.group_stats),
			    std::move(required_bits), op.estimated_cardinality);
		} else {
			groupby = make_uniq_base<PhysicalOperator, PhysicalHashAggregate>(
			    context, op.types, std::move(op.expressions), std::move(op.groups), std::move(op.grouping_sets),
			    std::move(op.grouping_functions), op.estimated_cardinality);
		}
	}
	groupby->children.push_back(std::move(plan));
	return groupby;
}

unique_ptr<PhysicalOperator>
PhysicalPlanGenerator::ExtractAggregateExpressions(unique_ptr<PhysicalOperator> child,
                                                   vector<unique_ptr<Expression>> &aggregates,
                                                   vector<unique_ptr<Expression>> &groups) {
	vector<unique_ptr<Expression>> expressions;
	vector<LogicalType> types;

	// bind sorted aggregates
	for (auto &aggr : aggregates) {
		auto &bound_aggr = aggr->Cast<BoundAggregateExpression>();
		if (bound_aggr.order_bys) {
			// sorted aggregate!
			FunctionBinder::BindSortedAggregate(context, bound_aggr, groups);
		}
	}
	for (auto &group : groups) {
		auto ref = make_uniq<BoundReferenceExpression>(group->return_type, expressions.size());
		types.push_back(group->return_type);
		expressions.push_back(std::move(group));
		group = std::move(ref);
	}
	for (auto &aggr : aggregates) {
		auto &bound_aggr = aggr->Cast<BoundAggregateExpression>();
		for (auto &child : bound_aggr.children) {
			auto ref = make_uniq<BoundReferenceExpression>(child->return_type, expressions.size());
			types.push_back(child->return_type);
			expressions.push_back(std::move(child));
			child = std::move(ref);
		}
		if (bound_aggr.filter) {
			auto &filter = bound_aggr.filter;
			auto ref = make_uniq<BoundReferenceExpression>(filter->return_type, expressions.size());
			types.push_back(filter->return_type);
			expressions.push_back(std::move(filter));
			bound_aggr.filter = std::move(ref);
		}
	}
	if (expressions.empty()) {
		return child;
	}
	auto projection =
	    make_uniq<PhysicalProjection>(std::move(types), std::move(expressions), child->estimated_cardinality);
	projection->children.push_back(std::move(child));
	return std::move(projection);
}

} // namespace duckdb
