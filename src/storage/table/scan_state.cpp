#include "duckdb/storage/table/scan_state.hpp"

#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

namespace {

struct RowGroupMapEntry {
	reference<SegmentNode<RowGroup>> row_group;
	unique_ptr<BaseStatistics> stats;
};

bool CompareValues(const Value &v1, const Value &v2, const OrderByStatistics order) {
	return (order == OrderByStatistics::MAX && v1 < v2) || (order == OrderByStatistics::MIN && v1 > v2);
}

idx_t GetQualifyingTupleCount(RowGroup &row_group, BaseStatistics &stats, const OrderByColumnType type) {
	if (!stats.CanHaveNull()) {
		return row_group.count;
	}

	if (type == OrderByColumnType::NUMERIC) {
		if (!NumericStats::HasMinMax(stats)) {
			return 0;
		}
		if (NumericStats::IsConstant(stats)) {
			return 1;
		}
		return 2;
	}
	// We cannot check if the min/max for StringStats have actually been set. As the strings may be truncated, we
	// also cannot assume that min and max are the same
	return 0;
}

void EraseFromMultiMap(multimap<Value, RowGroupMapEntry> &row_group_map,
                       multimap<Value, RowGroupMapEntry>::iterator &it) {
	row_group_map.erase(it);
}

void EraseFromMultiMap(multimap<Value, RowGroupMapEntry> &row_group_map,
                       multimap<Value, RowGroupMapEntry>::reverse_iterator &it) {
	row_group_map.erase(std::next(it).base());
}

void ResetIterator(multimap<Value, RowGroupMapEntry> &row_group_map, multimap<Value, RowGroupMapEntry>::iterator &it) {
	it = row_group_map.begin();
}

void ResetIterator(multimap<Value, RowGroupMapEntry> &row_group_map,
                   multimap<Value, RowGroupMapEntry>::reverse_iterator &it) {
	it = row_group_map.rbegin();
}

template <typename It, typename End>
void AddRowGroups(multimap<Value, RowGroupMapEntry> &row_group_map, It it, End end,
                  vector<reference<SegmentNode<RowGroup>>> &ordered_row_groups, const idx_t row_limit,
                  const idx_t row_offset, const OrderByColumnType column_type, const OrderByStatistics stat_type) {
	const auto opposite_stat_type =
	    stat_type == OrderByStatistics::MAX ? OrderByStatistics::MIN : OrderByStatistics::MAX;

	auto last_unresolved_entry = it;
	auto last_unresolved_boundary = RowGroupReorderer::RetrieveStat(*it->second.stats, opposite_stat_type, column_type);

	// Try to find row groups that can be excluded with offset
	if (row_offset > 0) {
		idx_t seen_tuples = 0;
		idx_t new_row_offset = row_offset;
		auto offset_it = it;
		vector<It> delete_row_group_its;

		for (; offset_it != end; ++offset_it) {
			auto &current_key = offset_it->first;
			auto &row_group = *offset_it->second.row_group.get().node;
			seen_tuples += row_group.count;

			while (last_unresolved_entry != offset_it) {
				if (!CompareValues(current_key, last_unresolved_boundary, stat_type)) {
					break;
				}
				// Row groups do not overlap
				auto &current_stats = offset_it->second.stats;
				if (!current_stats->CanHaveNull()) {
					// This row group has exactly row_group.count valid values. We can exclude those
					delete_row_group_its.push_back(last_unresolved_entry);
					new_row_offset -= row_group.count;
				}

				++last_unresolved_entry;
				auto &upcoming_stats = *last_unresolved_entry->second.stats;
				last_unresolved_boundary =
				    RowGroupReorderer::RetrieveStat(upcoming_stats, opposite_stat_type, column_type);
			}

			if (seen_tuples > row_offset) {
				break;
			}
		}

		for (auto &delete_it : delete_row_group_its) {
			EraseFromMultiMap(row_group_map, delete_it);
		}
		ResetIterator(row_group_map, it);
	}

	last_unresolved_entry = it;
	auto &last_stats = it->second.stats;
	last_unresolved_boundary = RowGroupReorderer::RetrieveStat(*last_stats, opposite_stat_type, column_type);

	// Try to find row groups that can be excluded with limit
	idx_t qualifying_tuples = 0;
	idx_t qualify_later = 0;

	idx_t last_unresolved_row_group_sum =
	    GetQualifyingTupleCount(*it->second.row_group.get().node, *last_stats, column_type);
	for (; it != end; ++it) {
		auto &current_key = it->first;
		auto &row_group = it->second.row_group;

		while (last_unresolved_entry != it) {
			if (!CompareValues(current_key, last_unresolved_boundary, stat_type)) {
				if (current_key != std::prev(it)->first) {
					// Row groups overlap: we can only guarantee one additional qualifying tuple
					qualifying_tuples += qualify_later;
					qualify_later = 0;
					qualifying_tuples++;
				} else {
					// Row groups have the same order value, we can only guarantee a qualifying tuple later
					qualify_later++;
				}

				break;
			}
			// Row groups do not overlap: we can guarantee that the tuples qualify
			qualifying_tuples = last_unresolved_row_group_sum;
			++last_unresolved_entry;
			auto &upcoming_row_group = *last_unresolved_entry->second.row_group.get().node;
			auto &upcoming_stats = *last_unresolved_entry->second.stats;

			last_unresolved_row_group_sum += GetQualifyingTupleCount(upcoming_row_group, upcoming_stats, column_type);
			last_unresolved_boundary = RowGroupReorderer::RetrieveStat(upcoming_stats, opposite_stat_type, column_type);
		}
		if (qualifying_tuples >= row_limit) {
			return;
		}
		ordered_row_groups.emplace_back(row_group);
	}
}

void SetRowGroupVectorWithLimit(multimap<Value, RowGroupMapEntry> &row_group_map, const optional_idx row_limit,
                                const optional_idx row_offset, const RowGroupOrderType order_type,
                                const OrderByColumnType column_type,
                                vector<reference<SegmentNode<RowGroup>>> &ordered_row_groups) {
	D_ASSERT(row_limit.IsValid());

	const auto stat_type = order_type == RowGroupOrderType::ASC ? OrderByStatistics::MIN : OrderByStatistics::MAX;
	ordered_row_groups.reserve(row_group_map.size());

	Value previous_key;
	if (order_type == RowGroupOrderType::ASC) {
		auto it = row_group_map.begin();
		auto end = row_group_map.end();
		AddRowGroups(row_group_map, it, end, ordered_row_groups, row_limit.GetIndex(),
		             row_offset.IsValid() ? row_offset.GetIndex() : 0, column_type, stat_type);
	} else {
		auto it = row_group_map.rbegin();
		auto end = row_group_map.rend();
		AddRowGroups(row_group_map, it, end, ordered_row_groups, row_limit.GetIndex(),
		             row_offset.IsValid() ? row_offset.GetIndex() : 0, column_type, stat_type);
	}
}

} // namespace

TableScanState::TableScanState() : table_state(*this), local_state(*this) {
}

TableScanState::~TableScanState() {
}

void TableScanState::Initialize(vector<StorageIndex> column_ids_p, optional_ptr<ClientContext> context,
                                optional_ptr<TableFilterSet> table_filters,
                                optional_ptr<SampleOptions> table_sampling) {
	this->column_ids = std::move(column_ids_p);
	if (table_filters) {
		filters.Initialize(*context, *table_filters, column_ids);
	}
	if (table_sampling) {
		sampling_info.do_system_sample = table_sampling->method == SampleMethod::SYSTEM_SAMPLE;
		sampling_info.sample_rate = table_sampling->sample_size.GetValue<double>() / 100.0;
		if (table_sampling->seed.IsValid()) {
			table_state.random.SetSeed(table_sampling->seed.GetIndex());
		}
	}
}

const vector<StorageIndex> &TableScanState::GetColumnIds() {
	D_ASSERT(!column_ids.empty());
	return column_ids;
}

ScanFilterInfo::~ScanFilterInfo() {
}

ScanFilterInfo &TableScanState::GetFilterInfo() {
	return filters;
}

ScanSamplingInfo &TableScanState::GetSamplingInfo() {
	return sampling_info;
}

ScanFilter::ScanFilter(ClientContext &context, idx_t index, const vector<StorageIndex> &column_ids, TableFilter &filter)
    : scan_column_index(index), table_column_index(column_ids[index].GetPrimaryIndex()), filter(filter),
      always_true(false) {
	filter_state = TableFilterState::Initialize(context, filter);
}

void ScanFilterInfo::Initialize(ClientContext &context, TableFilterSet &filters,
                                const vector<StorageIndex> &column_ids) {
	D_ASSERT(!filters.filters.empty());
	table_filters = &filters;
	adaptive_filter = make_uniq<AdaptiveFilter>(filters);
	filter_list.reserve(filters.filters.size());
	for (auto &entry : filters.filters) {
		filter_list.emplace_back(context, entry.first, column_ids, *entry.second);
	}
	column_has_filter.reserve(column_ids.size());
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		bool has_filter = table_filters->filters.find(col_idx) != table_filters->filters.end();
		column_has_filter.push_back(has_filter);
	}
	base_column_has_filter = column_has_filter;
}

bool ScanFilterInfo::ColumnHasFilters(idx_t column_idx) {
	if (column_idx < column_has_filter.size()) {
		return column_has_filter[column_idx];
	} else {
		return false;
	}
}

bool ScanFilterInfo::HasFilters() const {
	if (!table_filters) {
		// no filters
		return false;
	}
	// if we have filters - check if we need to check any of them
	return always_true_filters < filter_list.size();
}

void ScanFilterInfo::CheckAllFilters() {
	always_true_filters = 0;
	// reset the "column_has_filter" bitmask to the original
	for (idx_t col_idx = 0; col_idx < column_has_filter.size(); col_idx++) {
		column_has_filter[col_idx] = base_column_has_filter[col_idx];
	}
	// set "always_true" in the individual filters to false
	for (auto &filter : filter_list) {
		filter.always_true = false;
	}
}

void ScanFilterInfo::SetFilterAlwaysTrue(idx_t filter_idx) {
	auto &filter = filter_list[filter_idx];
	if (filter.always_true) {
		return;
	}
	filter.always_true = true;
	column_has_filter[filter.scan_column_index] = false;
	always_true_filters++;
}

RowGroupReorderer::RowGroupReorderer(const RowGroupOrderOptions &options)
    : column_idx(options.column_idx), order_by(options.order_by), order_type(options.order_type),
      column_type(options.column_type), row_limit(options.row_limit), row_offset(options.row_offset), offset(0),
      initialized(false) {
}

optional_ptr<SegmentNode<RowGroup>> RowGroupReorderer::GetNextRowGroup(SegmentNode<RowGroup> &row_group) {
	D_ASSERT(RefersToSameObject(ordered_row_groups[offset].get(), row_group));
	if (offset >= ordered_row_groups.size() - 1) {
		return nullptr;
	}
	return ordered_row_groups[++offset].get();
}

Value RowGroupReorderer::RetrieveStat(const BaseStatistics &stats, OrderByStatistics order_by,
                                      OrderByColumnType column_type) {
	switch (order_by) {
	case OrderByStatistics::MIN:
		return column_type == OrderByColumnType::NUMERIC ? NumericStats::Min(stats) : StringStats::Min(stats);
	case OrderByStatistics::MAX:
		return column_type == OrderByColumnType::NUMERIC ? NumericStats::Max(stats) : StringStats::Max(stats);
	}
	return Value();
}

struct RowGroupStatsPair {
	reference<RowGroup> row_group;
	unique_ptr<BaseStatistics> stats;
};

template <typename It, typename End>
idx_t GetNewOffset(It it, End end, const RowGroupOrderOptions &options) {
	const auto opposite_stat_type =
	    options.order_by == OrderByStatistics::MAX ? OrderByStatistics::MIN : OrderByStatistics::MAX;

	auto last_unresolved_entry = it;
	auto last_unresolved_boundary =
	    RowGroupReorderer::RetrieveStat(*it->second.stats, opposite_stat_type, options.column_type);

	// Try to find row groups that can be excluded with offset
	idx_t seen_tuples = 0;
	idx_t new_row_offset = options.row_offset.GetIndex();
	auto offset_it = it;
	vector<It> delete_row_group_its;

	for (; offset_it != end; ++offset_it) {
		auto &current_key = offset_it->first;
		auto &row_group = offset_it->second.row_group.get();
		seen_tuples += row_group.count;

		while (last_unresolved_entry != offset_it) {
			if (!CompareValues(current_key, last_unresolved_boundary, options.order_by)) {
				break;
			}
			// Row groups do not overlap
			auto &current_stats = offset_it->second.stats;
			if (!current_stats->CanHaveNull()) {
				// This row group has exactly row_group.count valid values. We can exclude those
				delete_row_group_its.push_back(last_unresolved_entry);
				new_row_offset -= row_group.count;
			}

			++last_unresolved_entry;
			auto &upcoming_stats = *last_unresolved_entry->second.stats;
			last_unresolved_boundary =
			    RowGroupReorderer::RetrieveStat(upcoming_stats, opposite_stat_type, options.column_type);
		}

		if (seen_tuples > options.row_offset.GetIndex()) {
			break;
		}
	}

	return new_row_offset;
}

idx_t RowGroupReorderer::GetOffsetAfterPruning(const RowGroupOrderOptions &options,
                                               vector<PartitionStatistics> &stats) {
	multimap<Value, RowGroupStatsPair> ordered_row_groups;

	for (auto &partition_stats : stats) {
		if (partition_stats.count_type == CountType::COUNT_APPROXIMATE) {
			return options.row_offset.GetIndex();
		}

		auto column_stats = partition_stats.GetColumnStatistics(options.column_idx);
		Value comparison_value = RetrieveStat(*column_stats, options.order_by, options.column_type);
		auto entry = RowGroupStatsPair {*partition_stats.row_group, std::move(column_stats)};
		ordered_row_groups.emplace(comparison_value, std::move(entry));
	}

	if (options.order_type == RowGroupOrderType::ASC) {
		return GetNewOffset(ordered_row_groups.begin(), ordered_row_groups.end(), options);
	}
	return GetNewOffset(ordered_row_groups.rbegin(), ordered_row_groups.rend(), options);
}

optional_ptr<SegmentNode<RowGroup>> RowGroupReorderer::GetRootSegment(RowGroupSegmentTree &row_groups) {
	if (initialized) {
		if (ordered_row_groups.empty()) {
			return nullptr;
		}
		return ordered_row_groups[0].get();
	}

	initialized = true;

	multimap<Value, RowGroupMapEntry> row_group_map;
	for (auto &row_group : row_groups.SegmentNodes()) {
		auto stats = row_group.node->GetStatistics(column_idx);
		Value comparison_value = RetrieveStat(*stats, order_by, column_type);
		auto entry = RowGroupMapEntry {row_group, std::move(stats)};
		row_group_map.emplace(comparison_value, std::move(entry));
	}

	if (row_group_map.empty()) {
		return nullptr;
	}

	if (row_limit.IsValid()) {
		SetRowGroupVectorWithLimit(row_group_map, row_limit, row_offset, order_type, column_type, ordered_row_groups);
	} else {
		ordered_row_groups.reserve(row_group_map.size());
		if (order_type == RowGroupOrderType::ASC) {
			for (auto &row_group : row_group_map) {
				ordered_row_groups.emplace_back(row_group.second.row_group);
			}
		} else {
			for (auto it = row_group_map.rbegin(); it != row_group_map.rend(); ++it) {
				ordered_row_groups.emplace_back(it->second.row_group);
			}
		}
	}

	return ordered_row_groups[0].get();
}

optional_ptr<AdaptiveFilter> ScanFilterInfo::GetAdaptiveFilter() {
	return adaptive_filter.get();
}

AdaptiveFilterState ScanFilterInfo::BeginFilter() const {
	if (!adaptive_filter) {
		return AdaptiveFilterState();
	}
	return adaptive_filter->BeginFilter();
}

void ScanFilterInfo::EndFilter(AdaptiveFilterState state) {
	if (!adaptive_filter) {
		return;
	}
	adaptive_filter->EndFilter(state);
}

void ColumnScanState::NextInternal(idx_t count) {
	if (!current) {
		//! There is no column segment
		return;
	}
	row_index += count;
	while (row_index >= current->node->start + current->node->count) {
		current = segment_tree->GetNextSegment(*current);
		initialized = false;
		segment_checked = false;
		if (!current) {
			break;
		}
	}
	D_ASSERT(!current ||
	         (row_index >= current->node->start && row_index < current->node->start + current->node->count));
}

void ColumnScanState::Next(idx_t count) {
	NextInternal(count);
	for (auto &child_state : child_states) {
		child_state.Next(count);
	}
}

const vector<StorageIndex> &CollectionScanState::GetColumnIds() {
	return parent.GetColumnIds();
}

TableFilterSet &GetFilters();

ScanFilterInfo &CollectionScanState::GetFilterInfo() {
	return parent.GetFilterInfo();
}

ScanSamplingInfo &CollectionScanState::GetSamplingInfo() {
	return parent.GetSamplingInfo();
}

TableScanOptions &CollectionScanState::GetOptions() {
	return parent.options;
}

ParallelCollectionScanState::ParallelCollectionScanState()
    : collection(nullptr), current_row_group(nullptr), processed_rows(0) {
}

optional_ptr<SegmentNode<RowGroup>> ParallelCollectionScanState::GetRootSegment(RowGroupSegmentTree &row_groups) const {
	if (reorderer) {
		return reorderer->GetRootSegment(row_groups);
	}
	return row_groups.GetRootSegment();
}

optional_ptr<SegmentNode<RowGroup>>
ParallelCollectionScanState::GetNextRowGroup(RowGroupSegmentTree &row_groups, SegmentNode<RowGroup> &row_group) const {
	if (reorderer) {
		return reorderer->GetNextRowGroup(row_group);
	}
	return row_groups.GetNextSegment(row_group);
}

CollectionScanState::CollectionScanState(TableScanState &parent_p)
    : row_group(nullptr), vector_index(0), max_row_group_row(0), row_groups(nullptr), max_row(0), batch_index(0),
      valid_sel(STANDARD_VECTOR_SIZE), random(-1), parent(parent_p) {
}

optional_ptr<SegmentNode<RowGroup>> CollectionScanState::GetNextRowGroup(SegmentNode<RowGroup> &row_group) const {
	if (reorderer) {
		return reorderer->GetNextRowGroup(row_group);
	}
	return row_groups->GetNextSegment(row_group);
}

optional_ptr<SegmentNode<RowGroup>> CollectionScanState::GetNextRowGroup(SegmentLock &l,
                                                                         SegmentNode<RowGroup> &row_group) const {
	D_ASSERT(!reorderer);
	return row_groups->GetNextSegment(l, row_group);
}

optional_ptr<SegmentNode<RowGroup>> CollectionScanState::GetRootSegment() const {
	if (reorderer) {
		return reorderer->GetRootSegment(*row_groups);
	}
	return row_groups->GetRootSegment();
}

bool CollectionScanState::Scan(DuckTransaction &transaction, DataChunk &result) {
	while (row_group) {
		row_group->node->Scan(transaction, *this, result);
		if (result.size() > 0) {
			return true;
		} else if (max_row <= row_group->node->start + row_group->node->count) {
			row_group = nullptr;
			return false;
		} else {
			do {
				row_group = GetNextRowGroup(*row_group).get();
				if (row_group) {
					if (row_group->node->start >= max_row) {
						row_group = nullptr;
						break;
					}
					bool scan_row_group = row_group->node->InitializeScan(*this, *row_group);
					if (scan_row_group) {
						// scan this row group
						break;
					}
				}
			} while (row_group);
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, SegmentLock &l, TableScanType type) {
	while (row_group) {
		row_group->node->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = GetNextRowGroup(l, *row_group).get();
			if (row_group) {
				row_group->node->InitializeScan(*this, *row_group);
			}
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, TableScanType type) {
	while (row_group) {
		row_group->node->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		}

		row_group = GetNextRowGroup(*row_group).get();
		if (row_group) {
			row_group->node->InitializeScan(*this, *row_group);
		}
	}
	return false;
}

PrefetchState::~PrefetchState() {
}

void PrefetchState::AddBlock(shared_ptr<BlockHandle> block) {
	blocks.push_back(std::move(block));
}

} // namespace duckdb
