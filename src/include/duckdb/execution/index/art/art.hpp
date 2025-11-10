//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/common/array.hpp"

namespace duckdb {

enum class VerifyExistenceType : uint8_t { APPEND = 0, APPEND_FK = 1, DELETE_FK = 2 };
enum class ARTConflictType : uint8_t { NO_CONFLICT = 0, CONSTRAINT = 1, TRANSACTION = 2 };
enum class ARTHandlingResult : uint8_t { CONTINUE = 0, SKIP = 1, YIELD = 2, NONE = 3 };

class ConflictManager;
class ARTKey;
class ARTKeySection;
class FixedSizeAllocator;

struct ARTIndexScanState;

class ART : public BoundIndex {
public:
	friend class Leaf;

public:
	//! Index type name for the ART.
	static constexpr const char *TYPE_NAME = "ART";
	//! FixedSizeAllocator count of the ART.
	static constexpr uint8_t ALLOCATOR_COUNT = 9;
	//! FixedSizeAllocator count of deprecated ARTs.
	static constexpr uint8_t DEPRECATED_ALLOCATOR_COUNT = ALLOCATOR_COUNT - 3;
	//! Keys must not exceed MAX_KEY_LEN * prefix_count.
	static constexpr idx_t MAX_KEY_LEN = 8192;

public:
	ART(const string &name, const IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
	    TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
	    AttachedDatabase &db,
	    const shared_ptr<array<unsafe_unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr = nullptr,
	    const IndexStorageInfo &info = IndexStorageInfo());

	//! Create a index instance of this type.
	static unique_ptr<BoundIndex> Create(CreateIndexInput &input) {
		auto art = make_uniq<ART>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                          input.unbound_expressions, input.db, nullptr, input.storage_info);
		return std::move(art);
	}

	//! Plan index construction.
	static PhysicalOperator &CreatePlan(PlanIndexInput &input);

	//! Root of the tree.
	Node tree = Node();
	//! Fixed-size allocators holding the ART nodes.
	shared_ptr<array<unsafe_unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> allocators;
	//! True, if the ART owns its data.
	bool owns_data;
	//! True, if keys need a key length verification pass.
	bool verify_max_key_len;
	//! The number of bytes fitting in the prefix.
	uint8_t prefix_count;

public:
	//! Try to initialize a scan on the ART with the given expression and filter.
	unique_ptr<IndexScanState> TryInitializeScan(const Expression &expr, const Expression &filter_expr);
	//! Perform a lookup on the ART, fetching up to max_count row IDs.
	//! If all row IDs were fetched, it return true, else false.
	bool Scan(IndexScanState &state, idx_t max_count, set<row_t> &row_ids);

	//! Appends data to the locked index.
	ErrorData Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) override;
	//! Appends data to the locked index and verifies constraint violations.
	ErrorData Append(IndexLock &l, DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info) override;

	//! Insert a chunk.
	ErrorData Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) override;
	//! Insert a chunk and verifies constraint violations.
	ErrorData Insert(IndexLock &l, DataChunk &data, Vector &row_ids, IndexAppendInfo &info) override;

	//! Verify that data can be appended to the index without a constraint violation.
	void VerifyAppend(DataChunk &chunk, IndexAppendInfo &info, optional_ptr<ConflictManager> manager) override;

	//! Delete a chunk from the ART.
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_ids) override;
	//! Drop the ART.
	void CommitDrop(IndexLock &index_lock) override;

	//! Build an ART from a vector of sorted keys and their row IDs.
	ARTConflictType Build(unsafe_vector<ARTKey> &keys, unsafe_vector<ARTKey> &row_ids, const idx_t row_count);

	//! Merge another ART into this ART. Both must be locked.
	//! FIXME: Return ARTConflictType instead of a boolean.
	bool MergeIndexes(IndexLock &state, BoundIndex &other_index) override;

	//! Vacuums the ART storage.
	void Vacuum(IndexLock &state) override;

	//! Serializes ART memory to disk and returns the ART storage information.
	IndexStorageInfo SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) override;
	//! Serializes ART memory to the WAL and returns the ART storage information.
	IndexStorageInfo SerializeToWAL(const case_insensitive_map_t<Value> &options) override;

	//! Returns the in-memory usage of the ART.
	idx_t GetInMemorySize(IndexLock &index_lock) override;

	//! ART key generation.
	template <bool IS_NOT_NULL = false>
	void GenerateKeys(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys);
	void GenerateKeyVectors(ArenaAllocator &allocator, DataChunk &input, Vector &row_ids, unsafe_vector<ARTKey> &keys,
	                        unsafe_vector<ARTKey> &row_id_keys);

	//! Verifies the nodes.
	void Verify(IndexLock &l) override;
	//! Verifies that the node allocations match the node counts.
	void VerifyAllocations(IndexLock &l) override;
	//! Verifies the index buffers.
	void VerifyBuffers(IndexLock &l) override;

	//! Returns string representation of the ART.
	string ToString(IndexLock &l, bool display_ascii = false) override;

private:
	bool SearchEqual(ARTKey &key, idx_t max_count, set<row_t> &row_ids);
	bool SearchGreater(ARTKey &key, bool equal, idx_t max_count, set<row_t> &row_ids);
	bool SearchLess(ARTKey &upper_bound, bool equal, idx_t max_count, set<row_t> &row_ids);
	bool SearchCloseRange(ARTKey &lower_bound, ARTKey &upper_bound, bool left_equal, bool right_equal, idx_t max_count,
	                      set<row_t> &row_ids);

	string GenerateErrorKeyName(DataChunk &input, idx_t row);
	string GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name);
	void VerifyLeaf(const Node &leaf, const ARTKey &key, optional_ptr<ART> delete_art, ConflictManager &manager,
	                optional_idx &conflict_idx, idx_t i);
	void VerifyConstraint(DataChunk &chunk, IndexAppendInfo &info, ConflictManager &manager) override;
	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                     DataChunk &input) override;

	void InitializeMergeUpperBounds(unsafe_vector<idx_t> &upper_bounds);
	void InitializeMerge(Node &node, unsafe_vector<idx_t> &upper_bounds);

	void InitializeVacuum(unordered_set<uint8_t> &indexes);
	void FinalizeVacuum(const unordered_set<uint8_t> &indexes);

	void InitAllocators(const IndexStorageInfo &info);
	void TransformToDeprecated();
	IndexStorageInfo PrepareSerialize(const case_insensitive_map_t<Value> &options, const bool v1_0_0_storage);
	void Deserialize(const BlockPointer &pointer);
	void WritePartialBlocks(QueryContext context, const bool v1_0_0_storage);
	void SetPrefixCount(const IndexStorageInfo &info);

	string ToStringInternal(bool display_ascii);
	void VerifyInternal();
	void VerifyAllocationsInternal();
};

template <>
void ART::GenerateKeys<>(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys);

template <>
void ART::GenerateKeys<true>(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys);

} // namespace duckdb
