#include "duckdb/optimizer/relation_statistics/relation_statistics.hpp"

namespace duckdb {

DistinctCount::DistinctCount(idx_t distinct_count, DistinctCountSource source)
    : distinct_count(distinct_count), source(source) {
}

RelationColumnStats::RelationColumnStats(ColumnBinding binding, DistinctCount distinct_count, Identifier name)
    : binding(binding), distinct_count(distinct_count), name(std::move(name)) {
}

RelationStats::RelationStats() : cardinality(1), filter_strength(1), stats_initialized(false) {
}

optional_idx RelationStats::FindColumn(ColumnBinding binding) const {
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		if (columns[column_idx].binding == binding) {
			return column_idx;
		}
	}
	return {};
}

optional_ptr<const RelationColumnStats> RelationStats::GetColumnStats(ColumnBinding binding) const {
	auto column_idx = FindColumn(binding);
	if (!column_idx.IsValid()) {
		return nullptr;
	}
	return columns[column_idx.GetIndex()];
}

bool RelationStats::MatchesBindings(const vector<ColumnBinding> &bindings) const {
	if (columns.size() != bindings.size()) {
		return false;
	}
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		if (columns[column_idx].binding != bindings[column_idx]) {
			return false;
		}
	}
	return true;
}

void RelationStats::Verify(const vector<ColumnBinding> &bindings) const {
	D_ASSERT(!stats_initialized || MatchesBindings(bindings));
}

} // namespace duckdb
