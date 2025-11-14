#include "duckdb/storage/table/row_group_reorderer.hpp"

namespace duckdb {

namespace {

struct RowGroupSegmentNodeEntry {
	reference<SegmentNode<RowGroup>> row_group;
	unique_ptr<BaseStatistics> stats;
};

struct RowGroupEntry {
	reference<RowGroup> row_group;
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

template <typename It, typename End>
void AddRowGroups(multimap<Value, RowGroupSegmentNodeEntry> &row_group_map, It it, End end,
                  vector<reference<SegmentNode<RowGroup>>> &ordered_row_groups, const idx_t row_limit,
                  const OrderByColumnType column_type, const OrderByStatistics stat_type) {
	const auto opposite_stat_type =
	    stat_type == OrderByStatistics::MAX ? OrderByStatistics::MIN : OrderByStatistics::MAX;

	auto last_unresolved_entry = it;
	auto &last_stats = it->second.stats;
	auto last_unresolved_boundary = RowGroupReorderer::RetrieveStat(*last_stats, opposite_stat_type, column_type);

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

template <typename It>
It SkipOffsetPrunedRowGroups(It it, const idx_t row_group_offset) {
	for (idx_t i = 0; i < row_group_offset; i++) {
		++it;
	}
	return it;
}

template <typename It, typename End>
void InsertAllRowGroups(It it, End end, vector<reference<SegmentNode<RowGroup>>> &ordered_row_groups) {
	for (; it != end; ++it) {
		ordered_row_groups.push_back(it->second.row_group);
	}
}

void SetRowGroupVector(multimap<Value, RowGroupSegmentNodeEntry> &row_group_map, const optional_idx row_limit,
                       const idx_t row_group_offset, const RowGroupOrderType order_type,
                       const OrderByColumnType column_type,
                       vector<reference<SegmentNode<RowGroup>>> &ordered_row_groups) {
	const auto stat_type = order_type == RowGroupOrderType::ASC ? OrderByStatistics::MIN : OrderByStatistics::MAX;
	ordered_row_groups.reserve(row_group_map.size());

	Value previous_key;
	if (order_type == RowGroupOrderType::ASC) {
		auto it = SkipOffsetPrunedRowGroups(row_group_map.begin(), row_group_offset);
		auto end = row_group_map.end();
		if (row_limit.IsValid()) {
			AddRowGroups(row_group_map, it, end, ordered_row_groups, row_limit.GetIndex(), column_type, stat_type);
		} else {
			InsertAllRowGroups(it, end, ordered_row_groups);
		}
	} else {
		auto it = SkipOffsetPrunedRowGroups(row_group_map.rbegin(), row_group_offset);
		auto end = row_group_map.rend();
		if (row_limit.IsValid()) {
			AddRowGroups(row_group_map, it, end, ordered_row_groups, row_limit.GetIndex(), column_type, stat_type);
		} else {
			InsertAllRowGroups(it, end, ordered_row_groups);
		}
	}
}


template <typename It, typename End>
OffsetPruningResult FindOffsetPrunableChunks(It it, End end, const OrderByStatistics order_by, const OrderByColumnType column_type,
                                 const idx_t row_offset) {
	const auto opposite_stat_type =
	    order_by == OrderByStatistics::MAX ? OrderByStatistics::MIN : OrderByStatistics::MAX;

	auto last_unresolved_entry = it;
	auto last_unresolved_boundary = RowGroupReorderer::RetrieveStat(*it->second.stats, opposite_stat_type, column_type);

	// Try to find row groups that can be excluded with offset
	idx_t seen_tuples = 0;
	idx_t new_row_offset = row_offset;
	idx_t pruned_row_group_count = 0;

	for (; it != end; ++it) {
		auto &current_key = it->first;
		auto &row_group = it->second.row_group.get();
		seen_tuples += row_group.count;

		while (last_unresolved_entry != it) {
			if (!CompareValues(current_key, last_unresolved_boundary, order_by)) {
				break;
			}
			// Row groups do not overlap
			auto &current_stats = it->second.stats;
			if (!current_stats->CanHaveNull()) {
				// This row group has exactly row_group.count valid values. We can exclude those
				pruned_row_group_count++;
				new_row_offset -= row_group.count;
			}

			++last_unresolved_entry;
			auto &upcoming_stats = *last_unresolved_entry->second.stats;
			last_unresolved_boundary = RowGroupReorderer::RetrieveStat(upcoming_stats, opposite_stat_type, column_type);
		}

		if (seen_tuples > row_offset) {
			break;
		}
	}

	return {new_row_offset, pruned_row_group_count};
}

} // namespace

RowGroupReorderer::RowGroupReorderer(const RowGroupOrderOptions &options_p)
    : options(options_p), offset(0), initialized(false) {
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

OffsetPruningResult RowGroupReorderer::GetOffsetAfterPruning(const OrderByStatistics order_by,
                                                             const OrderByColumnType column_type,
                                                             const RowGroupOrderType order_type,
                                                             const column_t column_idx, const idx_t row_offset,
                                                             vector<PartitionStatistics> &stats) {
	multimap<Value, RowGroupEntry> ordered_row_groups;

	for (auto &partition_stats : stats) {
		if (partition_stats.count_type == CountType::COUNT_APPROXIMATE) {
			return {row_offset, 0};
		}

		auto column_stats = partition_stats.GetColumnStatistics(column_idx);
		Value comparison_value = RetrieveStat(*column_stats, order_by, column_type);
		auto entry = RowGroupEntry {*partition_stats.row_group, std::move(column_stats)};
		ordered_row_groups.emplace(comparison_value, std::move(entry));
	}

	if (order_type == RowGroupOrderType::ASC) {
		return FindOffsetPrunableChunks(ordered_row_groups.begin(), ordered_row_groups.end(), order_by, column_type, row_offset);
	}
	return FindOffsetPrunableChunks(ordered_row_groups.rbegin(), ordered_row_groups.rend(), order_by, column_type, row_offset);
}

optional_ptr<SegmentNode<RowGroup>> RowGroupReorderer::GetRootSegment(RowGroupSegmentTree &row_groups) {
	if (initialized) {
		if (ordered_row_groups.empty()) {
			return nullptr;
		}
		return ordered_row_groups[0].get();
	}

	initialized = true;

	multimap<Value, RowGroupSegmentNodeEntry> row_group_map;
	for (auto &row_group : row_groups.SegmentNodes()) {
		auto stats = row_group.node->GetStatistics(options.column_idx);
		Value comparison_value = RetrieveStat(*stats, options.order_by, options.column_type);
		auto entry = RowGroupSegmentNodeEntry {row_group, std::move(stats)};
		row_group_map.emplace(comparison_value, std::move(entry));
	}


	if (row_group_map.empty()) {
		return nullptr;
	}

	D_ASSERT(row_group_map.size() > options.row_group_offset);
	SetRowGroupVector(row_group_map, options.row_limit, options.row_group_offset, options.order_type,
	                  options.column_type, ordered_row_groups);

	return ordered_row_groups[0].get();
}

} // namespace duckdb