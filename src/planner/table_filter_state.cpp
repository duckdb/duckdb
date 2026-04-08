#include "duckdb/planner/table_filter_state.hpp"

#include <utility>
#include <vector>

#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/perfect_hash_join_filter.hpp"
#include "duckdb/planner/filter/prefix_range_filter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"

namespace duckdb {
class ClientContext;
class Expression;

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression) : executor(context) {
	executor.AddExpression(expression);
}

JoinFilterTableFilterState::JoinFilterTableFilterState(const LogicalType &key_logical_type)
    : current_capacity(0), hashes_v(LogicalType::HASH, current_capacity),
      keys_sliced_v(key_logical_type, current_capacity), probe_sel(current_capacity) {
}

void JoinFilterTableFilterState::PrepareSlicedKeys(Vector &keys_v, SelectionVector &sel,
                                                   const idx_t approved_tuple_count) {
	if (current_capacity < approved_tuple_count) {
		hashes_v.Initialize(false, approved_tuple_count);
		keys_sliced_v.Initialize(false, approved_tuple_count);
		probe_sel.Initialize(approved_tuple_count);
		current_capacity = approved_tuple_count;
	}

	if (keys_sliced_v.GetType() == keys_v.GetType()) {
		keys_sliced_v.Slice(keys_v, sel, approved_tuple_count);
	} else {
		// Apply sel first (into keys_v space), then cast to key_type
		Vector sliced_src(keys_v.GetType());
		sliced_src.Slice(keys_v, sel, approved_tuple_count);
		sliced_src.Flatten(approved_tuple_count);
		VectorOperations::DefaultCast(sliced_src, keys_sliced_v, approved_tuple_count);
	}
}

const LogicalType &GetTableFilterKeyType(const TableFilter &filter) {
	if (filter.filter_type == TableFilterType::BLOOM_FILTER) {
		return filter.Cast<BFTableFilter>().GetKeyType();
	}
	if (filter.filter_type == TableFilterType::PERFECT_HASH_JOIN_FILTER) {
		return filter.Cast<PerfectHashJoinFilter>().GetKeyType();
	}
	if (filter.filter_type == TableFilterType::PREFIX_RANGE_FILTER) {
		return filter.Cast<PrefixRangeTableFilter>().GetKeyType();
	}
	throw NotImplementedException("Unknown filter type");
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::BLOOM_FILTER:
	case TableFilterType::PERFECT_HASH_JOIN_FILTER:
	case TableFilterType::PREFIX_RANGE_FILTER: {
		return make_uniq<JoinFilterTableFilterState>(GetTableFilterKeyType(filter));
	}
	case TableFilterType::OPTIONAL_FILTER: {
		// the optional filter may be executed if it is a SelectivityOptionalFilter
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return optional_filter.InitializeState(context);
	}

	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		return Initialize(context, *struct_filter.child_filter);
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conj_filter = filter.Cast<ConjunctionOrFilter>();
		auto result = make_uniq<ConjunctionOrFilterState>();
		for (auto &child_filter : conj_filter.child_filters) {
			result->child_states.push_back(Initialize(context, *child_filter));
		}
		return std::move(result);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conj_filter = filter.Cast<ConjunctionAndFilter>();
		auto result = make_uniq<ConjunctionAndFilterState>();
		for (auto &child_filter : conj_filter.child_filters) {
			result->child_states.push_back(Initialize(context, *child_filter));
		}
		return std::move(result);
	}
	case TableFilterType::EXPRESSION_FILTER: {
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		return make_uniq<ExpressionFilterState>(context, *expr_filter.expr);
	}
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
		// root nodes - create an empty filter state
		return make_uniq<TableFilterState>();
	default:
		throw InternalException("Unsupported filter type for TableFilterState::Initialize");
	}
}

} // namespace duckdb
