#include "duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp"

#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

void MultiFileAdaptiveFilterCache::InitializeAdaptiveFilter(const TableFilterSet &filters,
                                                            const vector<MultiFileGlobalIndex> &filter_global_indices,
                                                            Logger &logger, const string &file_path) {
	vector<idx_t> identities;
	for (const auto &global_index : filter_global_indices) {
		identities.push_back(global_index.GetIndex());
	}
	AdaptiveFilterSource source = AdaptiveFilterSource::INITIAL;
	if (filter && filter->Remap(filters, identities)) {
		source = AdaptiveFilterSource::SEEDED;
	} else {
		filter = make_uniq<AdaptiveFilter>(filters, identities);
	}
	filter->SetLogger(logger, file_path, source, identities);
}

} // namespace duckdb
