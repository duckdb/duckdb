//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/perfect_aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/clustered_aggregate.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"

namespace duckdb {

class ColumnDataCollection;
class GroupedAggregateHashTable;
struct AggregateStateSpillPlan;

class PerfectAggregateHashTable : public BaseAggregateHashTable {
public:
	PerfectAggregateHashTable(ClientContext &context, Allocator &allocator, const vector<LogicalType> &group_types,
	                          vector<LogicalType> payload_types_p, vector<AggregateObject> aggregate_objects,
	                          vector<Value> group_minima, vector<idx_t> required_bits);
	~PerfectAggregateHashTable() override;

public:
	//! Add the given data to the HT
	void AddChunk(DataChunk &groups, DataChunk &payload);

	//! Combines the target perfect aggregate HT into this one
	void Combine(PerfectAggregateHashTable &other);

	bool HasSingleArena() const {
		return stored_allocators.empty();
	}
	void MoveToRadix(ClientContext &context, const vector<LogicalType> &group_types, GroupedAggregateHashTable &target);

	//! Export dense state in bounded batches before releasing its backing arenas.
	void Export(ClientContext &context, const vector<LogicalType> &group_types, const AggregateStateSpillPlan &plan,
	            vector<unique_ptr<ColumnDataCollection>> &exported, idx_t radix_bits);

	idx_t ArenaAllocationSize() const;
	//! Deduct native string payloads from the list budget if they fit in one spill block.
	bool ReserveStringResults(const AggregateStateSpillPlan &plan, idx_t block_size, idx_t &available) const;
	bool SingleGroupExceeds(const AggregateStateSpillPlan &plan, idx_t available, idx_t list_width, idx_t input_rows);
	bool ListResultExceeds(const AggregateStateSpillPlan &plan, idx_t available,
	                       idx_t max_groups = STANDARD_VECTOR_SIZE, optional_ptr<idx_t> largest_list = nullptr) const;

	const TupleDataLayout &GetLayout() const {
		return *layout_ptr;
	}
	ArenaAllocator &GetArena() {
		return *aggregate_allocator;
	}
	const ArenaAllocator &GetArena() const {
		return *aggregate_allocator;
	}

	idx_t Capacity() const {
		return total_groups;
	}

	//! Scan the HT starting from the scan_position
	void Scan(idx_t &scan_position, DataChunk &result, idx_t max_groups = STANDARD_VECTOR_SIZE);

protected:
	Vector addresses;
	//! The required bits per group
	vector<idx_t> required_bits;
	//! The total required bits for the HT (this determines the max capacity)
	idx_t total_required_bits;
	//! The total amount of groups
	idx_t total_groups;
	//! The tuple size
	idx_t tuple_size;
	//! The number of grouping columns
	idx_t grouping_columns;

	// The actual pointer to the data
	data_ptr_t data;
	//! The owned data of the HT
	unsafe_unique_array<data_t> owned_data;
	//! Information on whether or not a specific group has any entries
	unsafe_unique_array<bool> group_is_set;

	//! The minimum values for each of the group columns
	vector<Value> group_minima;

	//! Reused selection vector
	SelectionVector sel;

	//! The active arena allocator used by the aggregates for their internal state
	unique_ptr<ArenaAllocator> aggregate_allocator;
	//! Owning arena allocators that this HT has data from
	vector<unique_ptr<ArenaAllocator>> stored_allocators;

	ClusteredAggrState clustered_state;
	//! The finalize state of the scan, kept across Scan calls like the finalize local states it holds
	unique_ptr<RowOperationsState> scan_row_state;

private:
	idx_t measured_rows = 0;
	idx_t largest_measured_list = 0;

	//! Try adding a chunk using the clustered aggregation path. Returns false if not applicable.
	bool AddChunkClustered(uintptr_t *address_data, DataChunk &payload);
	idx_t ScanGroups(idx_t &scan_position, DataChunk &result, idx_t max_groups = STANDARD_VECTOR_SIZE);
	//! Destroy the perfect aggregate HT (called automatically by the destructor)
	void Destroy();
};

} // namespace duckdb
