#include "duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp"

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

static vector<GlobalPosition> BuildSignature(const TableFilterSet &filters,
                                             const vector<MultiFileGlobalIndex> &filter_global_indices) {
	vector<GlobalPosition> signature;
	idx_t pos = 0;
	for (auto &entry : filters) {
		signature.push_back({filter_global_indices[pos], entry.Filter().filter_type});
		pos++;
	}
	return signature;
}

struct FilterPosition {
	idx_t position;
	TableFilterType filter_type;
};

static unique_ptr<AdaptiveFilterConfiguration> RemapConfig(const AdaptiveFilterConfiguration &cached_config,
                                                           const vector<GlobalPosition> &cached_signature,
                                                           const TableFilterSet &filters,
                                                           const vector<MultiFileGlobalIndex> &filter_global_indices) {
	if (cached_signature.size() != filters.FilterCount()) {
		return nullptr;
	}
	unordered_map<MultiFileGlobalIndex, FilterPosition> by_global;
	idx_t pos = 0;
	for (auto &entry : filters) {
		by_global.emplace(filter_global_indices[pos], FilterPosition {pos, entry.Filter().filter_type});
		pos++;
	}
	vector<idx_t> permutation;
	vector<idx_t> swap_likeliness;
	for (idx_t i = 0; i < cached_config.permutation.size(); i++) {
		auto cached_pos = cached_config.permutation[i];
		auto &cached_entry = cached_signature[cached_pos];
		auto it = by_global.find(cached_entry.global_index);
		if (it == by_global.end() || it->second.filter_type != cached_entry.filter_type) {
			return nullptr;
		}
		permutation.push_back(it->second.position);
		if (i + 1 < cached_config.permutation.size()) {
			swap_likeliness.push_back(cached_config.swap_likeliness[i]);
		}
	}
	auto seed = make_uniq<AdaptiveFilterConfiguration>();
	seed->permutation = std::move(permutation);
	seed->swap_likeliness = std::move(swap_likeliness);
	return seed;
}

void MultiFileAdaptiveFilterCache::InitializeAdaptiveFilter(const TableFilterSet &filters,
                                                            const vector<MultiFileGlobalIndex> &filter_global_indices,
                                                            Logger &logger, const string &file_path) {
	auto current_signature = BuildSignature(filters, filter_global_indices);
	AdaptiveFilterSource source = AdaptiveFilterSource::INITIAL;
	if (filter) {
		if (positions == current_signature) {
			// Same filter order
			source = AdaptiveFilterSource::SEEDED;
		} else {
			// Different order, we gotta remap
			auto seed = RemapConfig(filter->GetConfiguration(), positions, filters, filter_global_indices);
			if (seed) {
				filter = make_uniq<AdaptiveFilter>(filters, std::move(*seed));
				source = AdaptiveFilterSource::SEEDED;
			} else {
				filter = make_uniq<AdaptiveFilter>(filters);
			}
		}
	} else {
		filter = make_uniq<AdaptiveFilter>(filters);
	}
	positions = std::move(current_signature);

	vector<idx_t> global_ids;
	for (const auto &global_index : filter_global_indices) {
		global_ids.push_back(global_index.GetIndex());
	}
	filter->SetLogger(logger, file_path, source, global_ids);
}

} // namespace duckdb
