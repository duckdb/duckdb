#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

namespace duckdb {

class RecursiveCTEGroupAddressSet {
public:
	void Reset();
	void Reserve(idx_t count);
	bool Insert(data_ptr_t address);
	bool Contains(data_ptr_t address) const;

private:
	void Resize(idx_t capacity);

	vector<data_ptr_t> entries;
	vector<idx_t> occupied_slots;
};

struct RecursiveCTEKeyDeltaState {
	RecursiveCTEKeyDeltaState(ClientContext &context, const PhysicalRecursiveCTE &op);

	void Reset();
	void PrepareCollections();

	ColumnDataCollection previous_rows;
	ColumnDataCollection new_keys;
	ColumnDataAppendState previous_append_state;
	ColumnDataAppendState new_key_append_state;
	DataChunk selected_keys;
	DataChunk aggregate_rows;
	DataChunk updated_aggregate_rows;
	DataChunk result_rows;
	DataChunk previous_state_rows;
	DataChunk changed_rows;
	DataChunk previous_scan_rows;
	DataChunk key_scan_rows;
	DataChunk comparison_rows;
	Vector matched_addresses;
	Vector finalize_addresses;
	SelectionVector found_groups;
	SelectionVector missing_groups;
	SelectionVector changed_groups;
	SelectionVector changed_column_groups;
	SelectionVector equal_groups_a;
	SelectionVector equal_groups_b;
	AggregateHTLookupState lookup_state;
	ArenaAllocator arena;
	RowOperationsState row_state;
	unsafe_vector<data_ptr_t> new_group_addresses;
	RecursiveCTEGroupAddressSet touched_addresses;
	RecursiveCTEGroupAddressSet new_group_address_set;
	bool new_group_address_set_built = false;
	bool collections_initialized = false;
	bool deferred_previous_rows = false;
	bool deferred_candidate_reuse = false;
	idx_t deferred_count = 0;
	idx_t touched_count = 0;
	idx_t new_count = 0;
	idx_t changed_count = 0;
};

} // namespace duckdb
