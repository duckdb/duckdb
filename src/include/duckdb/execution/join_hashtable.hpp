//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/join_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/ht_entry.hpp"

namespace duckdb {

class BufferManager;
class BufferHandle;
class ColumnDataCollection;
struct ColumnDataAppendState;
struct ClientConfig;

struct JoinHTScanState {
public:
	JoinHTScanState(TupleDataCollection &collection, idx_t chunk_idx_from, idx_t chunk_idx_to,
	                TupleDataPinProperties properties = TupleDataPinProperties::ALREADY_PINNED)
	    : iterator(collection, properties, chunk_idx_from, chunk_idx_to, false), offset_in_chunk(0) {
	}

	TupleDataChunkIterator iterator;
	idx_t offset_in_chunk;

private:
	//! Implicit copying is not allowed
	JoinHTScanState(const JoinHTScanState &) = delete;
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

	//! only compare salts with the ht entries if the capacity is larger than 8192 so
	//! that it does not fit into the CPU cache
	static constexpr const idx_t USE_SALT_THRESHOLD = 8192;

	//! Scan structure that can be used to resume scans, as a single probe can
	//! return 1024*N values (where N is the size of the HT). This is
	//! returned by the JoinHashTable::Scan function and can be used to resume a
	//! probe.
	struct ScanStructure {
		TupleDataChunkState &key_state;
		//! Directly point to the entry in the hash table
		Vector pointers;
		idx_t count;
		SelectionVector sel_vector;
		SelectionVector chain_match_sel_vector;
		SelectionVector chain_no_match_sel_vector;

		// whether or not the given tuple has found a match
		unsafe_unique_array<bool> found_match;
		JoinHashTable &ht;
		bool finished;
		bool is_null;

		// it records the RHS pointers for the result chunk
		Vector rhs_pointers;
		// it records the LHS sel vector for the result chunk
		SelectionVector lhs_sel_vector;
		// these two variable records the last match results
		idx_t last_match_count;
		SelectionVector last_sel_vector;

		explicit ScanStructure(JoinHashTable &ht, TupleDataChunkState &key_state);
		//! Get the next batch of data from the scan structure
		void Next(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Are pointer chains all pointing to NULL?
		bool PointersExhausted() const;

	private:
		//! Next operator for the inner join
		void NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the semi join
		void NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the anti join
		void NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result);
		//! Next operator for the RIGHT semi and anti join
		void NextRightSemiOrAntiJoin(DataChunk &keys);
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

		//! Update the data chunk compaction buffer
		void UpdateCompactionBuffer(idx_t base_count, SelectionVector &result_vector, idx_t result_count);

	public:
		void AdvancePointers();
		void AdvancePointers(const SelectionVector &sel, idx_t sel_count);
		void GatherResult(Vector &result, const SelectionVector &result_vector, const SelectionVector &sel_vector,
		                  const idx_t count, const idx_t col_idx);
		void GatherResult(Vector &result, const SelectionVector &sel_vector, const idx_t count, const idx_t col_idx);
		void GatherResult(Vector &result, const idx_t count, const idx_t col_idx);
		idx_t ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel);
	};

public:
	struct SharedState {
		SharedState();

		// The ptrs to the row to which a key should be inserted into during building
		// or matched against during probing
		Vector rhs_row_locations;
		Vector salt_v;

		SelectionVector salt_match_sel;
		SelectionVector key_no_match_sel;
	};

	struct ProbeState : SharedState {
		ProbeState();

		Vector ht_offsets_v;
		Vector ht_offsets_dense_v;

		SelectionVector non_empty_sel;
	};

	struct InsertState : SharedState {
		explicit InsertState(const JoinHashTable &ht);
		/// Because of the index hick up
		SelectionVector remaining_sel;
		SelectionVector key_match_sel;

		DataChunk lhs_data;
		TupleDataChunkState chunk_state;
	};

	JoinHashTable(ClientContext &context, const vector<JoinCondition> &conditions, vector<LogicalType> build_types,
	              JoinType type, const vector<idx_t> &output_columns);
	~JoinHashTable();

	//! Add the given data to the HT
	void Build(PartitionedTupleDataAppendState &append_state, DataChunk &keys, DataChunk &input);
	//! Merge another HT into this one
	void Merge(JoinHashTable &other);
	//! Combines the partitions in sink_collection into data_collection, as if it were not partitioned
	void Unpartition();
	//! Allocate the pointer table for the probe
	void AllocatePointerTable();
	//! Initialize the pointer table for the probe
	void InitializePointerTable(idx_t entry_idx_from, idx_t entry_idx_to);
	//! Finalize the build of the HT, constructing the actual hash table and making the HT ready for probing.
	//! Finalize must be called before any call to Probe, and after Finalize is called Build should no longer be
	//! ever called.
	void Finalize(idx_t chunk_idx_from, idx_t chunk_idx_to, bool parallel);
	//! Probe the HT with the given input chunk, resulting in the given result
	void Probe(ScanStructure &scan_structure, DataChunk &keys, TupleDataChunkState &key_state, ProbeState &probe_state,
	           optional_ptr<Vector> precomputed_hashes = nullptr);
	//! Scan the HT to construct the full outer join result
	void ScanFullOuter(JoinHTScanState &state, Vector &addresses, DataChunk &result) const;

	//! Fill the pointer with all the addresses from the hashtable for full scan
	static idx_t FillWithHTOffsets(JoinHTScanState &state, Vector &addresses);

	idx_t Count() const {
		return data_collection->Count();
	}
	idx_t SizeInBytes() const {
		return data_collection->SizeInBytes();
	}

	PartitionedTupleData &GetSinkCollection() {
		return *sink_collection;
	}

	TupleDataCollection &GetDataCollection() {
		return *data_collection;
	}
	bool NullValuesAreEqual(idx_t col_idx) const {
		return null_values_are_equal[col_idx];
	}

	ClientContext &context;
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
	//! Positions of the columns that need to output
	const vector<idx_t> &output_columns;
	//! The comparison predicates that only contain equality predicates
	vector<ExpressionType> equality_predicates;
	//! The comparison predicates that contain non-equality predicates
	vector<ExpressionType> non_equality_predicates;

	//! The column indices of the equality predicates to be used to compare the rows
	vector<column_t> equality_predicate_columns;
	//! The column indices of the non-equality predicates to be used to compare the rows
	vector<column_t> non_equality_predicate_columns;
	//! Data column layout
	TupleDataLayout layout;
	//! Matches the equal condition rows during the build phase of the hash join to prevent
	//! duplicates in a list because of hash-collisions
	RowMatcher row_matcher_build;
	//! Efficiently matches the non-equi rows during the probing phase, only there if non_equality_predicates is not
	//! empty
	unique_ptr<RowMatcher> row_matcher_probe;
	//! Matches the same rows as the row_matcher, but also returns a vector for no matches
	unique_ptr<RowMatcher> row_matcher_probe_no_match_sel;
	//! Is true if there are predicates that are not equality predicates and we need to use the matchers during probing
	bool needs_chain_matcher;

	//! If there is more than one element in the chain, we need to scan the next elements of the chain
	bool chains_longer_than_one;

	//! The capacity of the HT. Is the same as hash_map.GetSize() / sizeof(ht_entry_t)
	idx_t capacity = DConstants::INVALID_INDEX;
	//! The size of an entry as stored in the HashTable
	idx_t entry_size;
	//! The total tuple size
	idx_t tuple_size;
	//! Next pointer offset in tuple, also used for the position of the hash, which then gets overwritten by the pointer
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
	uint64_t bitmask = DConstants::INVALID_INDEX;
	//! Whether or not we error on multiple rows found per match in a SINGLE join
	bool single_join_error_on_multiple_rows = true;

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
	void InitializeScanStructure(ScanStructure &scan_structure, DataChunk &keys, TupleDataChunkState &key_state,
	                             const SelectionVector *&current_sel);
	void Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes);

	bool UseSalt() const;

	//! Gets a pointer to the entry in the HT for each of the hashes_v using linear probing. Will update the
	//! key_match_sel vector and the count argument to the number and position of the matches
	void GetRowPointers(DataChunk &keys, TupleDataChunkState &key_state, ProbeState &state, Vector &hashes_v,
	                    const SelectionVector &sel, idx_t &count, Vector &pointers_result_v,
	                    SelectionVector &match_sel);

private:
	//! Insert the given set of locations into the HT with the given set of hashes_v
	void InsertHashes(Vector &hashes_v, idx_t count, TupleDataChunkState &chunk_state, InsertState &insert_statebool,
	                  bool parallel);
	//! Prepares keys by filtering NULLs
	idx_t PrepareKeys(DataChunk &keys, vector<TupleDataVectorFormat> &vector_data, const SelectionVector *&current_sel,
	                  SelectionVector &sel, bool build_side);

	//! Lock for combining data_collection when merging HTs
	mutex data_lock;
	//! Partitioned data collection that the data is sunk into when building
	unique_ptr<PartitionedTupleData> sink_collection;
	//! The DataCollection holding the main data of the hash table
	unique_ptr<TupleDataCollection> data_collection;

	//! The hash map of the HT, created after finalization
	AllocatedData hash_map;
	ht_entry_t *entries = nullptr;
	//! Whether or not NULL values are considered equal in each of the comparisons
	vector<bool> null_values_are_equal;
	//! An empty tuple that's a "dead end", can be used to stop chains early
	unsafe_unique_array<data_t> dead_end;

	//! Copying not allowed
	JoinHashTable(const JoinHashTable &) = delete;

public:
	//===--------------------------------------------------------------------===//
	// External Join
	//===--------------------------------------------------------------------===//
	static constexpr const idx_t INITIAL_RADIX_BITS = 4;

	struct ProbeSpillLocalAppendState {
		ProbeSpillLocalAppendState() {
		}
		//! Local partition and append state (if partitioned)
		optional_ptr<PartitionedColumnData> local_partition;
		optional_ptr<PartitionedColumnDataAppendState> local_partition_append_state;
	};
	//! ProbeSpill represents materialized probe-side data that could not be probed during PhysicalHashJoin::Execute
	//! because the HashTable did not fit in memory. The ProbeSpill is not partitioned if the remaining data can be
	//! dealt with in just 1 more round of probing, otherwise it is radix partitioned in the same way as the HashTable
	struct ProbeSpill {
	public:
		ProbeSpill(JoinHashTable &ht, ClientContext &context, const vector<LogicalType> &probe_types);

	public:
		//! Create a state for a new thread
		ProbeSpillLocalAppendState RegisterThread();
		//! Append a chunk to this ProbeSpill
		void Append(DataChunk &chunk, ProbeSpillLocalAppendState &local_state);
		//! Finalize by merging the thread-local accumulated data
		void Finalize();

	public:
		//! Prepare the next probe round
		void PrepareNextProbe();
		//! Scans and consumes the ColumnDataCollection
		unique_ptr<ColumnDataConsumer> consumer;

	private:
		JoinHashTable &ht;
		mutex lock;
		ClientContext &context;

		//! The types of the probe DataChunks
		const vector<LogicalType> &probe_types;
		//! The column ids
		vector<column_t> column_ids;

		//! The partitioned probe data and append states
		unique_ptr<PartitionedColumnData> global_partitions;
		vector<unique_ptr<PartitionedColumnData>> local_partitions;
		vector<unique_ptr<PartitionedColumnDataAppendState>> local_partition_append_states;

		//! The active probe data
		unique_ptr<ColumnDataCollection> global_spill_collection;
	};

	idx_t GetRadixBits() const {
		return radix_bits;
	}

	//! For a LOAD_FACTOR of 2.0, the HT is between 25% and 50% full
	static constexpr double DEFAULT_LOAD_FACTOR = 2.0;
	//! For a LOAD_FACTOR of 1.5, the HT is between 33% and 67% full
	static constexpr double EXTERNAL_LOAD_FACTOR = 1.5;

	double load_factor = DEFAULT_LOAD_FACTOR;

	//! Capacity of the pointer table given the ht count
	idx_t PointerTableCapacity(idx_t count) const {
		static constexpr idx_t MINIMUM_CAPACITY = 16384;

		const auto capacity = NextPowerOfTwo(LossyNumericCast<idx_t>(static_cast<double>(count) * load_factor));
		return MaxValue<idx_t>(capacity, MINIMUM_CAPACITY);
	}
	//! Size of the pointer table (in bytes)
	idx_t PointerTableSize(idx_t count) const {
		return PointerTableCapacity(count) * sizeof(data_ptr_t);
	}

	//! Get total size of HT if all partitions would be built
	idx_t GetTotalSize(const vector<unique_ptr<JoinHashTable>> &local_hts, idx_t &max_partition_size,
	                   idx_t &max_partition_count) const;
	idx_t GetTotalSize(const vector<idx_t> &partition_sizes, const vector<idx_t> &partition_counts,
	                   idx_t &max_partition_size, idx_t &max_partition_count) const;
	//! Get the remaining size of the unbuilt partitions
	idx_t GetRemainingSize() const;
	//! Sets number of radix bits according to the max ht size
	void SetRepartitionRadixBits(const idx_t max_ht_size, const idx_t max_partition_size,
	                             const idx_t max_partition_count);
	//! Initialized "current_partitions" and "completed_partitions"
	void InitializePartitionMasks();
	//! How many partitions are currently active
	idx_t CurrentPartitionCount() const;
	//! How many partitions are fully done
	idx_t FinishedPartitionCount() const;
	//! Partition this HT
	void Repartition(JoinHashTable &global_ht);

	//! Delete blocks that belong to the current partitioned HT
	void Reset();
	//! Build HT for the next partitioned probe round
	bool PrepareExternalFinalize(const idx_t max_ht_size);
	//! Probe whatever we can, sink the rest into a thread-local HT
	void ProbeAndSpill(ScanStructure &scan_structure, DataChunk &probe_keys, TupleDataChunkState &key_state,
	                   ProbeState &probe_state, DataChunk &probe_chunk, ProbeSpill &probe_spill,
	                   ProbeSpillLocalAppendState &spill_state, DataChunk &spill_chunk);

private:
	//! The current number of radix bits used to partition
	idx_t radix_bits;

	//! Bits set to 1 for currently active partitions
	ValidityMask current_partitions;
	//! Bits set to 1 for completed partitions
	ValidityMask completed_partitions;
};

} // namespace duckdb
