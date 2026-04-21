#include "duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp"

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

struct FilterPosition {
	idx_t position;
	TableFilterType filter_type;
};

unique_ptr<AdaptiveFilter> CreateMultiFileAdaptiveFilter(optional_ptr<MultiFileAdaptiveFilterCache> cache,
                                                         const TableFilterSet &filters,
                                                         const vector<MultiFileGlobalIndex> &filter_global_indices,
                                                         Logger &logger, const string &file_path) {
	unique_ptr<AdaptiveFilterConfiguration> seed;
	if (cache) {
		const auto &ordering = cache->GetOrdering();
		if (!ordering.empty() && ordering.size() == filters.FilterCount()) {
			unordered_map<MultiFileGlobalIndex, FilterPosition> by_global;
			// Lets first get the fitered column -> [permutation_position|filter_type]
			idx_t permutation_pos = 0;
			for (auto &entry : filters) {
				by_global.emplace(filter_global_indices[permutation_pos],
				                  FilterPosition {permutation_pos, entry.Filter().filter_type});
				permutation_pos++;
			}
			vector<idx_t> permutation;
			vector<idx_t> swap_likeliness;
			bool compatible = true;
			// for the cached order, we now get the permutation and swap_likeness (based on global column index)
			for (idx_t i = 0; i < ordering.size(); i++) {
				auto it = by_global.find(ordering[i].global_index);
				if (it == by_global.end() || it->second.filter_type != ordering[i].filter_type) {
					compatible = false;
					break;
				}
				permutation.push_back(it->second.position);
				if (i + 1 < ordering.size()) {
					swap_likeliness.push_back(ordering[i].swap_likeliness);
				}
			}
			if (compatible) {
				// if they are compatible, all filters in the right place, we create the config seed
				seed = make_uniq<AdaptiveFilterConfiguration>();
				seed->permutation = std::move(permutation);
				seed->swap_likeliness = std::move(swap_likeliness);
			}
		}
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
	// extract the global ids, for the logger
	vector<idx_t> global_ids;
	for (const auto &global_index : filter_global_indices) {
		global_ids.push_back(global_index.GetIndex());
	}
	result->SetLogger(logger, file_path, source, global_ids);
	return result;
}

void StoreMultiFileAdaptiveFilter(optional_ptr<MultiFileAdaptiveFilterCache> cache, const AdaptiveFilter &filter,
                                  const TableFilterSet &filters,
                                  const vector<MultiFileGlobalIndex> &filter_global_indices) {
	if (!cache) {
		return;
	}
	vector<TableFilterType> types_by_pos;
	for (auto &entry : filters) {
		types_by_pos.push_back(entry.Filter().filter_type);
	}
	const auto &config = filter.GetConfiguration();
	vector<AdaptiveFilterOrderEntry> ordering;
	for (idx_t i = 0; i < config.permutation.size(); i++) {
		auto permutation = config.permutation[i];
		idx_t swap_likeness = i < config.swap_likeliness.size() ? config.swap_likeliness[i] : 0;
		ordering.emplace_back(filter_global_indices[permutation], types_by_pos[permutation], swap_likeness);
	}
	cache->StoreOrdering(std::move(ordering));
}

} // namespace duckdb
