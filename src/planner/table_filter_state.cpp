#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"

namespace duckdb {

CachedSelectionVector::CachedSelectionVector() {
}

sel_t *CachedSelectionVector::Get(idx_t count) {
	if (count > capacity) {
		sel_data = make_uniq_array<sel_t>(count);
		capacity = count;
	}
	return sel_data.get();
}

LeafFilterState::LeafFilterState() {}

unique_ptr<TableFilterState> TableFilterState::Initialize(const TableFilter &filter) {
	switch(filter.filter_type) {
	case TableFilterType::OPTIONAL_FILTER:
		// optional filter is not executed - create an empty filter state
		return make_uniq<TableFilterState>();
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		return Initialize(*struct_filter.child_filter);
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conj_filter = filter.Cast<ConjunctionOrFilter>();
		auto result = make_uniq<ConjunctionOrFilterState>();
		for(auto &child_filter : conj_filter.child_filters) {
			result->child_states.push_back(Initialize(*child_filter));
		}
		return std::move(result);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conj_filter = filter.Cast<ConjunctionAndFilter>();
		auto result = make_uniq<ConjunctionAndFilterState>();
		for(auto &child_filter : conj_filter.child_filters) {
			result->child_states.push_back(Initialize(*child_filter));
		}
		return std::move(result);
	}
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
		return make_uniq<LeafFilterState>();
	default:
		throw InternalException("Unsupported filter type for TableFilterState::Initialize");
	}
}

}
