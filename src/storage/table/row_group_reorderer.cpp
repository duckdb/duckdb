#include "duckdb/storage/table/row_group_reorderer.hpp"

namespace duckdb {

namespace {

struct RowGroupSegmentNodeEntry {
	reference<SegmentNode<RowGroup>> row_group;
	unique_ptr<BaseStatistics> stats;
};

struct RowGroupOffsetEntry {
	idx_t count;
	unique_ptr<BaseStatistics> stats;
};

bool IsNullOnly(const BaseStatistics &stats) {
	return !stats.CanHaveNoNull();
}

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
	    GetQualifyingTupleCount(it->second.row_group.get().GetNode(), *last_stats, column_type);
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
			auto &upcoming_row_group = last_unresolved_entry->second.row_group.get().GetNode();
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

template <typename It, typename End>
It SkipOffsetPrunedRowGroups(It it, End end, idx_t row_group_offset) {
	while (row_group_offset > 0 && it != end) {
		++it;
		row_group_offset--;
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
                       const idx_t row_group_offset, const OrderType order_type, const OrderByColumnType column_type,
                       vector<reference<SegmentNode<RowGroup>>> &ordered_row_groups) {
	const auto stat_type = order_type == OrderType::ASCENDING ? OrderByStatistics::MIN : OrderByStatistics::MAX;
	if (order_type == OrderType::ASCENDING) {
		auto end = row_group_map.end();
		auto it = SkipOffsetPrunedRowGroups(row_group_map.begin(), end, row_group_offset);
		if (it == end) {
			return;
		}
		if (row_limit.IsValid()) {
			AddRowGroups(row_group_map, it, end, ordered_row_groups, row_limit.GetIndex(), column_type, stat_type);
		} else {
			InsertAllRowGroups(it, end, ordered_row_groups);
		}
	} else {
		auto end = row_group_map.rend();
		auto it = SkipOffsetPrunedRowGroups(row_group_map.rbegin(), end, row_group_offset);
		if (it == end) {
			return;
		}
		if (row_limit.IsValid()) {
			AddRowGroups(row_group_map, it, end, ordered_row_groups, row_limit.GetIndex(), column_type, stat_type);
		} else {
			InsertAllRowGroups(it, end, ordered_row_groups);
		}
	}
}

void AppendRowGroups(const vector<reference<SegmentNode<RowGroup>>> &source, idx_t offset,
                     vector<reference<SegmentNode<RowGroup>>> &target) {
	for (idx_t i = offset; i < source.size(); i++) {
		target.push_back(source[i]);
	}
}

template <typename It, typename End>
OffsetPruningResult FindOffsetPrunableChunks(It it, End end, const OrderByStatistics order_by,
                                             const OrderByColumnType column_type, const idx_t row_offset) {
	if (it == end) {
		return {row_offset, 0, 0};
	}
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
		auto tuple_count = it->second.count;
		seen_tuples += tuple_count;

		while (last_unresolved_entry != it) {
			if (!CompareValues(current_key, last_unresolved_boundary, order_by)) {
				break;
			}
			// Row groups do not overlap
			auto &pruned_stats = last_unresolved_entry->second.stats;
			if (pruned_stats->CanHaveNull()) {
				// Offset pruning can only remove a contiguous prefix of row groups. Once the first unresolved
				// row group might still contribute NULLs to the ordered prefix, we cannot prune past it.
				return {new_row_offset, pruned_row_group_count, 0};
			}
			// This row group has exactly row_group.count valid values. We can exclude those.
			pruned_row_group_count++;
			new_row_offset -= last_unresolved_entry->second.count;

			++last_unresolved_entry;
			if (last_unresolved_entry == end) {
				return {new_row_offset, pruned_row_group_count, 0};
			}
			auto &upcoming_stats = *last_unresolved_entry->second.stats;
			last_unresolved_boundary = RowGroupReorderer::RetrieveStat(upcoming_stats, opposite_stat_type, column_type);
		}

		if (seen_tuples > row_offset) {
			break;
		}
	}

	return {new_row_offset, pruned_row_group_count, 0};
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
	if (column_type == OrderByColumnType::NUMERIC) {
		if (!NumericStats::HasMinMax(stats)) {
			return Value();
		}
		switch (order_by) {
		case OrderByStatistics::MIN:
			return NumericStats::Min(stats);
		case OrderByStatistics::MAX:
			return NumericStats::Max(stats);
		default:
			throw InternalException("Unsupported OrderByStatistics for numeric");
		}
	}
	if (column_type == OrderByColumnType::STRING) {
		if (!StringStats::HasMinMax(stats)) {
			// Row group is all nulls or has incomplete stats - stats are meaningless for ordering
			return Value();
		}
		switch (order_by) {
		case OrderByStatistics::MIN:
			return StringStats::Min(stats);
		case OrderByStatistics::MAX:
			return StringStats::Max(stats);
		default:
			throw InternalException("Unsupported OrderByStatistics for string");
		}
	}
	throw InternalException("Unsupported OrderByColumnType");
}

OffsetPruningResult RowGroupReorderer::GetOffsetAfterPruning(const OrderByStatistics order_by,
                                                             const OrderByColumnType column_type,
                                                             const OrderType order_type,
                                                             const OrderByNullType null_order,
                                                             const StorageIndex &storage_index, const idx_t row_offset,
                                                             vector<PartitionStatistics> &stats) {
	multimap<Value, RowGroupOffsetEntry> ordered_row_groups;
	idx_t new_row_offset = row_offset;
	idx_t leading_null_group_offset = 0;
	bool encountered_maybe_null_group = false;

	for (auto &partition_stats : stats) {
		if (partition_stats.count_type == CountType::COUNT_APPROXIMATE || !partition_stats.partition_row_group) {
			return {row_offset, 0, 0};
		}

		auto column_stats = partition_stats.partition_row_group->GetColumnStatistics(storage_index);
		if (null_order == OrderByNullType::NULLS_FIRST && IsNullOnly(*column_stats)) {
			if (new_row_offset < partition_stats.count) {
				return {new_row_offset, 0, leading_null_group_offset};
			}
			new_row_offset -= partition_stats.count;
			leading_null_group_offset++;
			continue;
		}
		Value comparison_value = RetrieveStat(*column_stats, order_by, column_type);
		if (comparison_value.IsNull()) {
			if (null_order == OrderByNullType::NULLS_LAST && IsNullOnly(*column_stats)) {
				// With NULLS_LAST, null-stats row groups are scanned after all non-null row groups.
				// They fall outside the offset range being pruned here, so skip them.
				continue;
			}
			// The row group might still contribute ordered values, but we cannot position it safely.
			if (null_order == OrderByNullType::NULLS_FIRST) {
				encountered_maybe_null_group = true;
				continue;
			}
			return {new_row_offset, 0, leading_null_group_offset};
		}
		if (null_order == OrderByNullType::NULLS_FIRST && column_stats->CanHaveNull()) {
			// Groups that might contain NULLs are scanned before definitely non-null groups with NULLS_FIRST.
			// Without exact NULL counts, they block further offset pruning into the non-null region.
			encountered_maybe_null_group = true;
			continue;
		}
		auto entry = RowGroupOffsetEntry {partition_stats.count, std::move(column_stats)};
		ordered_row_groups.emplace(comparison_value, std::move(entry));
	}
	if (null_order == OrderByNullType::NULLS_FIRST && encountered_maybe_null_group) {
		return {new_row_offset, 0, leading_null_group_offset};
	}

	switch (order_type) {
	case OrderType::ASCENDING: {
		auto result = FindOffsetPrunableChunks(ordered_row_groups.begin(), ordered_row_groups.end(), order_by,
		                                       column_type, new_row_offset);
		result.leading_null_group_offset = leading_null_group_offset;
		return result;
	}
	case OrderType::DESCENDING: {
		auto result = FindOffsetPrunableChunks(ordered_row_groups.rbegin(), ordered_row_groups.rend(), order_by,
		                                       column_type, new_row_offset);
		result.leading_null_group_offset = leading_null_group_offset;
		return result;
	}
	default:
		throw InternalException("Unsupported order type in GetOffsetAfterPruning");
	}
}

optional_ptr<SegmentNode<RowGroup>> RowGroupReorderer::GetRootSegment(RowGroupSegmentTree &row_groups) {
	if (initialized) {
		if (ordered_row_groups.empty()) {
			return nullptr;
		}
		return ordered_row_groups[0].get();
	}

	initialized = true;

	vector<reference<SegmentNode<RowGroup>>> null_only_groups;
	vector<reference<SegmentNode<RowGroup>>> ambiguous_groups;
	multimap<Value, RowGroupSegmentNodeEntry> row_group_map;
	for (auto &row_group : row_groups.SegmentNodes()) {
		auto stats = row_group.GetNode().GetStatistics(options.column_idx);
		if (IsNullOnly(*stats)) {
			null_only_groups.push_back(row_group);
			continue;
		}
		Value comparison_value = RetrieveStat(*stats, options.order_by, options.column_type);
		if (comparison_value.IsNull() || (options.null_order == OrderByNullType::NULLS_FIRST && stats->CanHaveNull())) {
			// No trustworthy ordering statistics, or the row group might still contribute NULLs that must be scanned
			// before definitely non-null groups with NULLS_FIRST.
			ambiguous_groups.push_back(row_group);
			continue;
		}
		auto entry = RowGroupSegmentNodeEntry {row_group, std::move(stats)};
		row_group_map.emplace(comparison_value, std::move(entry));
	}

	if (options.null_order == OrderByNullType::NULLS_FIRST) {
		AppendRowGroups(null_only_groups, options.leading_null_group_offset, ordered_row_groups);
		AppendRowGroups(ambiguous_groups, 0, ordered_row_groups);
		SetRowGroupVector(row_group_map, options.row_limit, options.row_group_offset, options.order_type,
		                  options.column_type, ordered_row_groups);
	} else {
		SetRowGroupVector(row_group_map, options.row_limit, options.row_group_offset, options.order_type,
		                  options.column_type, ordered_row_groups);
		AppendRowGroups(ambiguous_groups, 0, ordered_row_groups);
		AppendRowGroups(null_only_groups, 0, ordered_row_groups);
	}

	if (ordered_row_groups.empty()) {
		return nullptr;
	}

	return ordered_row_groups[0].get();
}

} // namespace duckdb
