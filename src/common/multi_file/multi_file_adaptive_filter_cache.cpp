#include "duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp"

namespace duckdb {

unique_ptr<AdaptiveFilterConfiguration>
MultiFileAdaptiveFilterCache::GetConfiguration(idx_t expected_filter_count) {
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

} // namespace duckdb
