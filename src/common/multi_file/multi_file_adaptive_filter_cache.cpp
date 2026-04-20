#include "duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp"

#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

unique_ptr<AdaptiveFilterConfiguration>
MultiFileAdaptiveFilterCache::GetConfiguration(idx_t expected_filter_count) const {
	lock_guard<mutex> guard(lock);
	if (!populated || adaptive_state.permutation.size() != expected_filter_count) {
		return nullptr;
	}
	return make_uniq<AdaptiveFilterConfiguration>(adaptive_state);
}

void MultiFileAdaptiveFilterCache::StoreConfiguration(AdaptiveFilterConfiguration adaptive_state_p) {
	lock_guard<mutex> guard(lock);
	adaptive_state = std::move(adaptive_state_p);
	populated = true;
}

unique_ptr<AdaptiveFilter> CreateMultiFileAdaptiveFilter(optional_ptr<MultiFileAdaptiveFilterCache> cache,
                                                         const TableFilterSet &filters, Logger &logger,
                                                         const string &file_path) {
	unique_ptr<AdaptiveFilterConfiguration> seed;
	if (cache) {
		seed = cache->GetConfiguration(filters.FilterCount());
	}
	unique_ptr<AdaptiveFilter> result;
	AdaptiveFilterSource source;
	if (seed) {
		result = make_uniq<AdaptiveFilter>(filters, std::move(*seed));
		source = AdaptiveFilterSource::SEEDED;
	} else {
		result = make_uniq<AdaptiveFilter>(filters);
		source = AdaptiveFilterSource::INITIAL;
	}
	result->SetLogger(logger, file_path, source);
	return result;
}

void StoreMultiFileAdaptiveFilter(optional_ptr<MultiFileAdaptiveFilterCache> cache, const AdaptiveFilter &filter) {
	if (!cache) {
		return;
	}
	cache->StoreConfiguration(filter.GetConfiguration());
}

} // namespace duckdb
