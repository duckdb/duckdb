#include "duckdb/storage/table/scan_state.hpp"

#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

//! Try to extract a min or max Value from statistics.
//! Returns true and sets |result| on success. Returns false if stats are unavailable or inexact.
static bool TryGetStatValue(const BaseStatistics &stats, bool is_min, Value &result) {
	if (stats.GetStatsType() == StatisticsType::NUMERIC_STATS) {
		if (is_min) {
			if (!NumericStats::HasMin(stats)) {
				return false;
			}
			result = NumericStats::Min(stats);
		} else {
			if (!NumericStats::HasMax(stats)) {
				return false;
			}
			result = NumericStats::Max(stats);
		}
		return true;
	} else if (stats.GetStatsType() == StatisticsType::STRING_STATS) {
		if (!StringStats::HasMaxStringLength(stats)) {
			return false;
		}
		// String min/max stats are stored in a fixed 8-byte buffer (MAX_STRING_MINMAX_SIZE).
		auto max_len = StringStats::MaxStringLength(stats);
		if (max_len > StringStatsData::MAX_STRING_MINMAX_SIZE) {
			return false;
		}
		auto s_min = StringStats::Min(stats);
		auto s_max = StringStats::Max(stats);
		if (s_min > s_max) {
			return false;
		}
		result = Value(is_min ? s_min : s_max);
		return true;
	}
	return false;
}

void ScanAggregateInfo::Initialize(AggregatePushdownInfo &agg_info_p) {
	aggregate_info = &agg_info_p;
	idx_t n = agg_info_p.aggregates.size();
	counts.assign(n, 0);

	aggregate_states.resize(n);
	bool needs_arena = false;
	for (idx_t i = 0; i < n; i++) {
		auto &agg = agg_info_p.aggregates[i];
		if (agg.aggregate_function) {
			auto &func = *agg.aggregate_function;
			auto state_size = func.state_size(func);
			aggregate_states[i] = make_unsafe_uniq_array_uninitialized<data_t>(state_size);
			func.initialize(func, aggregate_states[i].get());
			needs_arena = true;
		}
	}
	if (needs_arena) {
		arena = make_uniq<ArenaAllocator>(Allocator::DefaultAllocator());
	}
}

ScanAggregateInfo::~ScanAggregateInfo() {
	if (!aggregate_info) {
		return;
	}

	for (idx_t i = 0; i < aggregate_states.size(); i++) {
		if (!aggregate_states[i]) {
			continue;
		}
		auto &aggregate = aggregate_info->aggregates[i];
		if (aggregate.aggregate_function && aggregate.aggregate_function->destructor) {
			Vector state_vector(Value::POINTER(CastPointerToValue(aggregate_states[i].get())));
			AggregateInputData aggr_input_data(aggregate.bind_data.get(), *arena);
			aggregate.aggregate_function->destructor(state_vector, aggr_input_data, 1);
		}
	}
}

void ScanAggregateInfo::AccumulateStatValue(const PushedAggregateInfo &aggregate, const Value &stat_val) {
	auto idx = aggregate.output_idx;
	// Feed the stat value into the native aggregate state via simple_update.
	Vector input(stat_val);
	input.Flatten(1);
	AggregateInputData aggr_input_data(aggregate.bind_data.get(), *arena);
	aggregate.aggregate_function->simple_update(&input, aggr_input_data, 1, aggregate_states[idx].get(), 1);
}

bool ScanAggregateInfo::AccumulateRowGroupStats(RowGroup &rg) {
	D_ASSERT(aggregate_info);
	// First, check if ALL aggregates can be resolved from stats.
	// We must not partially accumulate — if any aggregate fails, we scan the entire RG.
	for (auto &agg : aggregate_info->aggregates) {
		if (agg.type == PushedAggregateType::COUNT_COL) {
			auto stats = rg.GetStatistics(agg.col_idx);
			if (!stats || stats->CanHaveNull()) {
				return false;
			}
		}
		if (agg.type == PushedAggregateType::MIN || agg.type == PushedAggregateType::MAX) {
			auto stats = rg.GetStatistics(agg.col_idx);
			if (!stats) {
				return false;
			}
			Value stat_val;
			if (!TryGetStatValue(*stats, agg.type == PushedAggregateType::MIN, stat_val)) {
				return false;
			}
		}
	}
	// Second, all checks passed, safe to accumulate.
	for (auto &aggr : aggregate_info->aggregates) {
		switch (aggr.type) {
		case PushedAggregateType::COUNT_STAR: {
			AggregateInputData aggr_input_data(aggr.bind_data.get(), *arena);
			aggr.aggregate_function->simple_update(nullptr, aggr_input_data, 0, aggregate_states[aggr.output_idx].get(),
			                                       rg.count.load());
			break;
		}
		case PushedAggregateType::COUNT_COL:
			counts[aggr.output_idx] += NumericCast<int64_t>(rg.count.load());
			break;
		case PushedAggregateType::MIN:
		case PushedAggregateType::MAX: {
			auto stats = rg.GetStatistics(aggr.col_idx);
			Value stat_val;
			TryGetStatValue(*stats, aggr.type == PushedAggregateType::MIN, stat_val);
			AccumulateStatValue(aggr, stat_val);
			break;
		}
		}
	}
	return true;
}

bool ScanAggregateInfo::AccumulateSegmentStats(idx_t count, const unsafe_vector<ColumnScanState> &col_scans) {
	D_ASSERT(aggregate_info);
	// First, check if ALL aggregates can be resolved from segment stats.
	for (auto &aggregate : aggregate_info->aggregates) {
		if (aggregate.type == PushedAggregateType::COUNT_COL) {
			if (aggregate.scan_col_position >= col_scans.size()) {
				return false;
			}
			auto &col_scan = col_scans[aggregate.scan_col_position];
			if (!col_scan.current) {
				return false;
			}
			// NULLs are tracked in the validity child segment (child_states[0]),
			// not in the data segment's stats.
			if (col_scan.child_states.empty() || !col_scan.child_states[0].current) {
				return false;
			}
			auto &validity_stats = col_scan.child_states[0].current->GetNode().stats.statistics;
			if (validity_stats.CanHaveNull()) {
				return false;
			}
		}
		if (aggregate.type == PushedAggregateType::MIN || aggregate.type == PushedAggregateType::MAX) {
			if (aggregate.scan_col_position >= col_scans.size()) {
				return false;
			}
			auto &col_scan = col_scans[aggregate.scan_col_position];
			if (!col_scan.current) {
				return false;
			}
			auto &seg_stats = col_scan.current->GetNode().stats.statistics;
			Value stat_val;
			if (!TryGetStatValue(seg_stats, aggregate.type == PushedAggregateType::MIN, stat_val)) {
				return false;
			}
		}
	}
	// Second, all checks passed, safe to accumulate.
	for (auto &aggregate : aggregate_info->aggregates) {
		switch (aggregate.type) {
		case PushedAggregateType::COUNT_STAR: {
			AggregateInputData aggr_input_data(aggregate.bind_data.get(), *arena);
			aggregate.aggregate_function->simple_update(nullptr, aggr_input_data, 0,
			                                            aggregate_states[aggregate.output_idx].get(), count);
			break;
		}
		case PushedAggregateType::COUNT_COL:
			counts[aggregate.output_idx] += NumericCast<int64_t>(count);
			break;
		case PushedAggregateType::MIN:
		case PushedAggregateType::MAX: {
			auto &col_scan = col_scans[aggregate.scan_col_position];
			auto &seg_stats = col_scan.current->GetNode().stats.statistics;
			Value stat_val;
			TryGetStatValue(seg_stats, aggregate.type == PushedAggregateType::MIN, stat_val);
			AccumulateStatValue(aggregate, stat_val);
			break;
		}
		}
	}
	return true;
}

void ScanAggregateInfo::AccumulateChunk(DataChunk &chunk) {
	D_ASSERT(aggregate_info);
	if (chunk.size() == 0) {
		return;
	}
	for (auto &agg : aggregate_info->aggregates) {
		switch (agg.type) {
		case PushedAggregateType::COUNT_STAR: {
			AggregateInputData aggr_input_data(agg.bind_data.get(), *arena);
			agg.aggregate_function->simple_update(nullptr, aggr_input_data, 0, aggregate_states[agg.output_idx].get(),
			                                      chunk.size());
			break;
		}
		case PushedAggregateType::COUNT_COL:
		case PushedAggregateType::MIN:
		case PushedAggregateType::MAX: {
			AggregateInputData aggr_input_data(agg.bind_data.get(), *arena);
			agg.aggregate_function->simple_update(&chunk.data[agg.scan_col_position], aggr_input_data, 1,
			                                      aggregate_states[agg.output_idx].get(), chunk.size());
			break;
		}
		}
	}
}

void ScanAggregateInfo::Finalize(DataChunk &output) const {
	D_ASSERT(aggregate_info);
	output.Reset();
	output.SetCardinality(1);
	for (auto &agg : aggregate_info->aggregates) {
		switch (agg.type) {
		case PushedAggregateType::COUNT_STAR:
		case PushedAggregateType::MIN:
		case PushedAggregateType::MAX: {
			Vector state_vector(Value::POINTER(CastPointerToValue(aggregate_states[agg.output_idx].get())));
			AggregateInputData aggr_input_data(agg.bind_data.get(), *arena);
			agg.aggregate_function->GetStateFinalizeCallback()(state_vector, aggr_input_data,
			                                                   output.data[agg.output_idx], 1, 0);
			break;
		}
		case PushedAggregateType::COUNT_COL: {
			Vector state_vector(Value::POINTER(CastPointerToValue(aggregate_states[agg.output_idx].get())));
			AggregateInputData aggr_input_data(agg.bind_data.get(), *arena);
			agg.aggregate_function->GetStateFinalizeCallback()(state_vector, aggr_input_data,
			                                                   output.data[agg.output_idx], 1, 0);
			int64_t chunk_count = output.data[agg.output_idx].GetValue(0).GetValue<int64_t>();
			output.data[agg.output_idx].SetValue(0, Value::BIGINT(counts[agg.output_idx] + chunk_count));
			break;
		}
		}
	}
}

TableScanState::TableScanState() : table_state(*this), local_state(*this) {
}

TableScanState::~TableScanState() {
}

void TableScanState::Initialize(vector<StorageIndex> column_ids_p, optional_ptr<ClientContext> context,
                                optional_ptr<TableFilterSet> table_filters, optional_ptr<SampleOptions> table_sampling,
                                optional_ptr<AggregatePushdownInfo> table_aggregates) {
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
	if (table_aggregates) {
		aggregates.Initialize(*table_aggregates);
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

ScanAggregateInfo &TableScanState::GetAggregateInfo() {
	return aggregates;
}

ScanSamplingInfo &TableScanState::GetSamplingInfo() {
	return sampling_info;
}

ScanFilter::ScanFilter(ClientContext &context, ProjectionIndex index, const vector<StorageIndex> &column_ids,
                       TableFilter &filter)
    : scan_column_index(index), table_column_index(column_ids[index]), filter(filter), always_true(false) {
	filter_state = TableFilterState::Initialize(context, filter);
}

void ScanFilterInfo::Initialize(ClientContext &context, TableFilterSet &filters,
                                const vector<StorageIndex> &column_ids) {
	D_ASSERT(filters.HasFilters());
	table_filters = &filters;
	adaptive_filter = make_uniq<AdaptiveFilter>(filters);
	filter_list.reserve(filters.FilterCount());
	for (auto &entry : filters) {
		filter_list.emplace_back(context, entry.GetIndex(), column_ids, entry.Filter());
	}
	column_has_filter.reserve(column_ids.size());
	for (auto col_idx : ProjectionIndex::GetIndexes(column_ids.size())) {
		bool has_filter = table_filters->HasFilter(col_idx);
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
	offset_in_column += count;
	while (offset_in_column >= current->GetRowStart() + current->GetNode().count) {
		current = segment_tree->GetNextSegment(*current);
		initialized = false;
		segment_checked = false;
		if (!current) {
			break;
		}
	}
	D_ASSERT(!current || (offset_in_column >= current->GetRowStart() &&
	                      offset_in_column < current->GetRowStart() + current->GetNode().count));
}

idx_t ColumnScanState::GetPositionInSegment() const {
	return offset_in_column - (current ? current->GetRowStart() : 0);
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

ScanAggregateInfo &CollectionScanState::GetAggregateInfo() {
	return parent.GetAggregateInfo();
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

void CollectionScanState::AdvanceRowGroup() {
	if (max_row <= row_group->GetRowStart() + row_group->GetNode().count) {
		row_group = nullptr;
		return;
	}
	do {
		row_group = GetNextRowGroup(*row_group).get();
		if (!row_group || row_group->GetRowStart() >= max_row) {
			row_group = nullptr;
			return;
		}
		if (row_group->GetNode().InitializeScan(*this, *row_group)) {
			return;
		}
	} while (row_group);
}

bool CollectionScanState::Scan(DuckTransaction &transaction, DataChunk &result) {
	auto &agg_info = GetAggregateInfo();
	auto &filter_info = GetFilterInfo();
	while (row_group) {
		if (agg_info.IsActive() && filter_info.AllFiltersAlwaysTrue()) {
			auto &rg = row_group->GetNode();
			if (!rg.StatsAreStale()) {
				// Entire row group satisfies the filter, try to accumulate from stats, skip scanning.
				if (agg_info.AccumulateRowGroupStats(rg)) {
					AdvanceRowGroup();
					continue;
				}
				// Fall through to normal scanning for this row group.
			}
		}

		row_group->GetNode().Scan(TransactionData(transaction), *this, result);
		if (result.size() > 0) {
			if (agg_info.IsActive()) {
				// Accumulate the scanned chunk into the aggregate state instead of returning it.
				agg_info.AccumulateChunk(result);
				result.Reset();
				continue;
			}
			return true;
		}
		AdvanceRowGroup();
	}
	return false;
}

bool CollectionScanState::Scan(DataChunk &result, TableScanType type, optional_ptr<SegmentLock> l) {
	while (row_group) {
		row_group->GetNode().Scan(*this, result, type);
		if (result.size() > 0) {
			return true;
		}
		// move to the next row group
		if (l) {
			row_group = GetNextRowGroup(*l, *row_group).get();
		} else {
			row_group = GetNextRowGroup(*row_group).get();
		}
		if (row_group) {
			row_group->GetNode().InitializeScan(*this, *row_group);
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
