//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/enums/expression_type.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "common/types/vector.hpp"

namespace duckdb {

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
	SuperLargeHashTable(index_t initial_capacity, vector<TypeId> group_types, vector<TypeId> payload_types,
	                    vector<ExpressionType> aggregate_types, bool parallel = false);
	~SuperLargeHashTable();

	//! Resize the HT to the specified size. Must be larger than the current
	//! size.
	void Resize(index_t size);
	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	void AddChunk(DataChunk &groups, DataChunk &payload);
	//! Scan the HT starting from the scan_position until the result and group
	//! chunks are filled. scan_position will be updated by this function.
	//! Returns the amount of elements found.
	index_t Scan(index_t &scan_position, DataChunk &group, DataChunk &result);

	//! Fetch the aggregates for specific groups from the HT and place them in the result
	void FetchAggregates(DataChunk &groups, DataChunk &result);

	void FindOrCreateGroups(DataChunk &groups, Vector &addresses, Vector &new_group);

	//! The stringheap of the AggregateHashTable
	StringHeap string_heap;

private:
	TupleSerializer group_serializer;

	//! The aggregate types to be computed
	vector<ExpressionType> aggregate_types;

	//! The types of the group columns stored in the hashtable
	vector<TypeId> group_types;
	//! The types of the payload columns stored in the hashtable
	vector<TypeId> payload_types;
	//! The size of the payload (aggregations) in bytes
	index_t payload_width;
	//! The total tuple size
	index_t tuple_size;
	//! The capacity of the HT. This can be increased using
	//! SuperLargeHashTable::Resize
	index_t capacity;
	//! The amount of entries stored in the HT currently
	index_t entries;
	//! The data of the HT
	data_ptr_t data;
	//! The maximum size of the chain
	index_t max_chain;
	//! Whether or not the HT has to support parallel insertion operations
	bool parallel = false;
	//! The empty payload data
	unique_ptr<data_t[]> empty_payload_data;

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
};

} // namespace duckdb
