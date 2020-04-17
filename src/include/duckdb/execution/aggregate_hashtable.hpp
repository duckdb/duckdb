//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {
class BoundAggregateExpression;

struct AggregateObject {
	AggregateObject(AggregateFunction function, idx_t child_count, idx_t payload_size, bool distinct,
	                TypeId return_type)
	    : function(move(function)), child_count(child_count), payload_size(payload_size), distinct(distinct),
	      return_type(return_type) {
	}

	AggregateFunction function;
	idx_t child_count;
	idx_t payload_size;
	bool distinct;
	TypeId return_type;

	static vector<AggregateObject> CreateAggregateObjects(vector<BoundAggregateExpression *> bindings);
};

//! SuperLargeHashTable is a linear probing HT that is used for computing
//! aggregates
/*!
    SuperLargeHashTable is a HT that is used for computing aggregates. It takes
   as input the set of groups and the types of the aggregates to compute and
   stores them in the HT. It uses linear probing for collision resolution, and
   supports both parallel and sequential modes.
*/
class SuperLargeHashTable {
public:
	SuperLargeHashTable(idx_t initial_capacity, vector<TypeId> group_types, vector<TypeId> payload_types,
	                    vector<BoundAggregateExpression *> aggregates, bool parallel = false);
	SuperLargeHashTable(idx_t initial_capacity, vector<TypeId> group_types, vector<TypeId> payload_types,
	                    vector<AggregateObject> aggregates, bool parallel = false);
	~SuperLargeHashTable();

	//! Resize the HT to the specified size. Must be larger than the current
	//! size.
	void Resize(idx_t size);
	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	void AddChunk(DataChunk &groups, DataChunk &payload);
	//! Scan the HT starting from the scan_position until the result and group
	//! chunks are filled. scan_position will be updated by this function.
	//! Returns the amount of elements found.
	idx_t Scan(idx_t &scan_position, DataChunk &group, DataChunk &result);

	//! Fetch the aggregates for specific groups from the HT and place them in the result
	void FetchAggregates(DataChunk &groups, DataChunk &result);

	//! Finds or creates groups in the hashtable using the specified group keys. The addresses vector will be filled
	//! with pointers to the groups in the hash table, and the new_groups selection vector will point to the newly
	//! created groups. The return value is the amount of newly created groups.
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &addresses, SelectionVector &new_groups);
	void FindOrCreateGroups(DataChunk &groups, Vector &addresses);

	//! The stringheap of the AggregateHashTable
	StringHeap string_heap;

private:
	void HashGroups(DataChunk &groups, Vector &addresses);

	//! The aggregates to be computed
	vector<AggregateObject> aggregates;
	//! The types of the group columns stored in the hashtable
	vector<TypeId> group_types;
	//! The types of the payload columns stored in the hashtable
	vector<TypeId> payload_types;
	//! The size of the groups in bytes
	idx_t group_width;
	//! The size of the payload (aggregations) in bytes
	idx_t payload_width;
	//! The total tuple size
	idx_t tuple_size;
	//! The capacity of the HT. This can be increased using
	//! SuperLargeHashTable::Resize
	idx_t capacity;
	//! The amount of entries stored in the HT currently
	idx_t entries;
	//! The data of the HT
	data_ptr_t data;
	//! The endptr of the hashtable
	data_ptr_t endptr;
	//! Whether or not the HT has to support parallel insertion operations
	bool parallel = false;
	//! The empty payload data
	unique_ptr<data_t[]> empty_payload_data;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	uint64_t bitmask;

	vector<unique_ptr<SuperLargeHashTable>> distinct_hashes;

	//! The size of the initial flag for each cell
	static constexpr int FLAG_SIZE = sizeof(uint8_t);
	//! Flag indicating a cell is empty
	static constexpr int EMPTY_CELL = 0x00;
	//! Flag indicating a cell is full
	static constexpr int FULL_CELL = 0xFF;

	SuperLargeHashTable(const SuperLargeHashTable &) = delete;

	//! unique_ptr to indicate the ownership
	unique_ptr<data_t[]> owned_data;

private:
	void Destroy();
	void CallDestructors(Vector &state_vector, idx_t count);
	void ScatterGroups(DataChunk &groups, unique_ptr<VectorData[]> &group_data, Vector &addresses,
	                   const SelectionVector &sel, idx_t count);
};

} // namespace duckdb
