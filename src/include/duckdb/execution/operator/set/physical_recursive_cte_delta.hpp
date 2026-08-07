#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

namespace duckdb {

struct RecursiveCTEKeyDeltaState {
	RecursiveCTEKeyDeltaState(ClientContext &context, const PhysicalRecursiveCTE &op, bool track_touched_keys);

	void Reset();

	unique_ptr<GroupedAggregateHashTable> touched_keys;
	ColumnDataCollection previous_rows;
	ColumnDataCollection new_keys;
	ColumnDataAppendState previous_append_state;
	ColumnDataAppendState new_key_append_state;
	DataChunk first_touch_keys;
	DataChunk selected_keys;
	DataChunk payload_rows;
	DataChunk result_rows;
	DataChunk changed_rows;
	DataChunk previous_scan_rows;
	DataChunk key_scan_rows;
	DataChunk comparison_rows;
	Vector touched_addresses;
	Vector matched_addresses;
	SelectionVector first_touches;
	SelectionVector found_groups;
	SelectionVector missing_groups;
	SelectionVector changed_groups;
	SelectionVector changed_column_groups;
	SelectionVector equal_groups_a;
	SelectionVector equal_groups_b;
	AggregateHTLookupState lookup_state;
	ArenaAllocator arena;
	RowOperationsState row_state;
	idx_t touched_count = 0;
	idx_t new_count = 0;
	idx_t changed_count = 0;
};

} // namespace duckdb
