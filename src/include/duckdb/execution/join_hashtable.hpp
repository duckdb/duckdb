//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/join_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row_data_collection.hpp"
#include "duckdb/common/types/row_layout.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {
class BufferManager;
class BufferHandle;
class Pipeline;
class Event;

struct JoinHTScanState {
	JoinHTScanState() : position(0), block_position(0) {
	}

	idx_t position;
	idx_t block_position;
	mutex lock;
};

//! JoinHashTable is a linear probing HT that is used for computing joins
/*!
   The JoinHashTable concatenates incoming chunks inside a linked list of
   data ptrs. The storage looks like this internally.
   [SERIALIZED ROW][NEXT POINTER]
   [SERIALIZED ROW][NEXT POINTER]
   There is a separate hash map of pointers that point into this table.
   This is what is used to resolve the hashes.
   [POINTER]
   [POINTER]
   [POINTER]
   The pointers are either NULL
*/
class JoinHashTable {
public:
	using ValidityBytes = TemplatedValidityMask<uint8_t>;

	//! Scan structure that can be used to resume scans, as a single probe can
	//! return 1024*N values (where N is the size of the HT). This is
	//! returned by the JoinHashTable::Scan function and can be used to resume a
	//! probe.
	struct ScanStructure {
		unique_ptr<VectorData[]> key_data;
		Vector pointers;
		idx_t count;
		SelectionVector sel_vector;
		// whether or not the given tuple has found a match
		unique_ptr<bool[]> found_match;
		JoinHashTable &ht;
		bool finished;

		explicit ScanStructure(JoinHashTable &ht);
		//! Get the next batch of data from the scan structure
		void Next(DataChunk &keys, DataChunk &left, DataChunk &result);

	private:
		//! Next operator for the inner join
		void NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the semi join
		void NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the anti join
		void NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the left outer join
		void NextLeftJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the mark join
		void NextMarkJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the single join
		void NextSingleJoin(DataChunk &keys, DataChunk &left, DataChunk &result);

		//! Scan the hashtable for matches of the specified keys, setting the found_match[] array to true or false
		//! for every tuple
		void ScanKeyMatches(DataChunk &keys);
		template <bool MATCH>
		void NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result);

		void ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &child, DataChunk &result);

		idx_t ScanInnerJoin(DataChunk &keys, SelectionVector &result_vector);

		idx_t ResolvePredicates(DataChunk &keys, SelectionVector &match_sel);
		idx_t ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector &no_match_sel);

	public:
		void AdvancePointers();
		void AdvancePointers(const SelectionVector &sel, idx_t sel_count);
		void GatherResult(Vector &result, const SelectionVector &result_vector, const SelectionVector &sel_vector,
		                  const idx_t count, const idx_t col_idx);
		void GatherResult(Vector &result, const SelectionVector &sel_vector, const idx_t count, const idx_t col_idx);
		idx_t ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel);
	};

public:
	JoinHashTable(BufferManager &buffer_manager, const vector<JoinCondition> &conditions,
	              vector<LogicalType> build_types, JoinType type);
	~JoinHashTable();

	//! Initialize an empty HT for the same join
	unique_ptr<JoinHashTable> CopyEmpty() const;
	//! Add the given data to the HT
	void Build(DataChunk &keys, DataChunk &input);
	//! Merge another HT into this one
	void Merge(JoinHashTable &other);
	//! Finalize the build of the HT, constructing the actual hash table and making the HT ready for probing.
	//! Finalize must be called before any call to Probe, and after Finalize is called Build should no longer be
	//! ever called.
	void Finalize();
	//! Probe the HT with the given input chunk, resulting in the given result
	unique_ptr<ScanStructure> Probe(DataChunk &keys);
	//! Scan the HT to construct the final full outer join result after
	void ScanFullOuter(DataChunk &result, JoinHTScanState &state);
	//! Fill the pointer with all the addresses from the hashtable for full scan
	idx_t FillWithHTOffsets(data_ptr_t *key_locations, JoinHTScanState &state);

	idx_t Count() {
		return block_collection->count;
	}

	//! BufferManager
	BufferManager &buffer_manager;
	//! The join conditions
	const vector<JoinCondition> &conditions;
	//! The types of the keys used in equality comparison
	vector<LogicalType> equality_types;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! The comparison predicates
	vector<ExpressionType> predicates;
	//! Data column layout
	RowLayout layout;
	//! The size of an entry as stored in the HashTable
	idx_t entry_size;
	//! The total tuple size
	idx_t tuple_size;
	//! Next pointer offset in tuple
	idx_t pointer_offset;
	//! A constant false column for initialising right outer joins
	Vector vfound;
	//! The join type of the HT
	JoinType join_type;
	//! Whether or not the HT has been finalized
	bool finalized;
	//! Whether or not any of the key elements contain NULL
	bool has_null;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	uint64_t bitmask;

	struct {
		mutex mj_lock;
		//! The types of the duplicate eliminated columns, only used in correlated MARK JOIN for flattening
		//! ANY()/ALL() expressions
		vector<LogicalType> correlated_types;
		//! The aggregate expression nodes used by the HT
		vector<unique_ptr<Expression>> correlated_aggregates;
		//! The HT that holds the group counts for every correlated column
		unique_ptr<GroupedAggregateHashTable> correlated_counts;
		//! Group chunk used for aggregating into correlated_counts
		DataChunk group_chunk;
		//! Payload chunk used for aggregating into correlated_counts
		DataChunk correlated_payload;
		//! Result chunk used for aggregating into correlated_counts
		DataChunk result_chunk;
	} correlated_mark_join_info;

private:
	void Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes);

	//! Apply a bitmask to the hashes
	void ApplyBitmask(Vector &hashes, idx_t count);
	void ApplyBitmask(Vector &hashes, const SelectionVector &sel, idx_t count, Vector &pointers);

private:
	//! Insert the given set of locations into the HT with the given set of
	//! hashes. Caller should hold lock in parallel HT.
	void InsertHashes(Vector &hashes, idx_t count, data_ptr_t key_locations[]);

	idx_t PrepareKeys(DataChunk &keys, unique_ptr<VectorData[]> &key_data, const SelectionVector *&current_sel,
	                  SelectionVector &sel, bool build_side);

	//! The RowDataCollection holding the main data of the hash table
	unique_ptr<RowDataCollection> block_collection;
	//! The stringheap of the JoinHashTable
	unique_ptr<RowDataCollection> string_heap;
	//! Pinned handles, these are pinned during finalization only
	vector<unique_ptr<BufferHandle>> pinned_handles;
	//! The hash map of the HT, created after finalization
	unique_ptr<BufferHandle> hash_map;
	//! Whether or not NULL values are considered equal in each of the comparisons
	vector<bool> null_values_are_equal;

	//! Copying not allowed
	JoinHashTable(const JoinHashTable &) = delete;

	//! Out of core stuff down here
public:
	//! TODO
	idx_t SizeInBytes();
	//! Swizzles all blocks in this HT
	void SwizzleCollectedBlocks();
	//! Unswizzle blocks in the 'swizzled_...' RowDataCollections
	void UnswizzleBlocks();
	//! Similar to Finalize() but for an external join
	void SchedulePartitionTasks(Pipeline &pipeline, Event &event, vector<unique_ptr<JoinHashTable>> local_hts);
	//! Partition this HT
	void Partition(JoinHashTable &global_ht);
	//! TODO
	void PinPartitions();
	//! TODO
	void FinalizeExternal();
	//! Probe whatever we can, sink the rest into a thread-local HT
	unique_ptr<ScanStructure> ProbeAndBuild(DataChunk &keys, DataChunk &payload, JoinHashTable &local_ht,
	                                        DataChunk &sink_keys, DataChunk &sink_payload);

private:
	//! Merges histogram into this one
	void MergeHistogram(JoinHashTable &other);
	//! Reduces histogram on bit at a time
	void ReduceHistogram(idx_t avg_string_size);
	//! Checks whether the current histogram
	bool PartitionsFitInMemory(idx_t histogram[], idx_t average_row_size);

private:
	//! The number of radix bits used to build the histogram
	static constexpr const idx_t INITIAL_RADIX_BITS = 10;
	//! The current number of radix bits
	idx_t current_radix_bits;

	//! The RowDataCollection holding the swizzled main data of the hash table
	unique_ptr<RowDataCollection> swizzled_block_collection;
	//! The stringheap accompanying the swizzled main data
	unique_ptr<RowDataCollection> swizzled_string_heap;

	//! TODO
	mutex finalize_lock;

	//! Histogram lock
	mutex histogram_lock;
	//! Histogram of inserted values
	unique_ptr<idx_t[]> histogram_ptr;

	//! Partitioned data lock
	mutex partition_lock;
	//! Partitioned data
	vector<unique_ptr<RowDataCollection>> partition_block_collections;
	vector<unique_ptr<RowDataCollection>> partition_string_heaps;
	//! TODO
	idx_t partition_cutoff;
};

} // namespace duckdb
