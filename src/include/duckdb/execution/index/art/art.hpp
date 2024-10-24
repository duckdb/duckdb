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

enum class VerifyExistenceType : uint8_t {
	// Appends to a table.
	APPEND = 0,
	// Appends to a table that has a foreign key.
	APPEND_FK = 1,
	// Delete from a table that has a foreign key.
	DELETE_FK = 2
};
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
	static unique_ptr<PhysicalOperator> CreatePlan(PlanIndexInput &input);

	//! Root of the tree.
	Node tree = Node();
	//! Fixed-size allocators holding the ART nodes.
	shared_ptr<array<unsafe_unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> allocators;
	//! True, if the ART owns its data.
	bool owns_data;
	//! The number of bytes fitting in the prefix.
	uint8_t prefix_count;

public:
	//! Try to initialize a scan on the ART with the given expression and filter.
	unique_ptr<IndexScanState> TryInitializeScan(const Expression &expr, const Expression &filter_expr);
	//! Perform a lookup on the ART, fetching up to max_count row IDs.
	//! If all row IDs were fetched, it return true, else false.
	bool Scan(IndexScanState &state, idx_t max_count, unsafe_vector<row_t> &row_ids);

	//! Append a chunk by first executing the ART's expressions.
	ErrorData Append(IndexLock &lock, DataChunk &input, Vector &row_ids) override;
	//! Insert a chunk.
	bool Insert(Node &node, const ARTKey &key, idx_t depth, const ARTKey &row_id, const GateStatus status);
	ErrorData Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	//! Constraint verification for a chunk.
	void VerifyAppend(DataChunk &chunk) override;
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;

	//! Delete a chunk from the ART.
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_ids) override;
	//! Drop the ART.
	void CommitDrop(IndexLock &index_lock) override;

	//! Construct an ART from a vector of sorted keys and their row IDs.
	bool Construct(unsafe_vector<ARTKey> &keys, unsafe_vector<ARTKey> &row_ids, const idx_t row_count);

	//! Merge another ART into this ART. Both must be locked.
	bool MergeIndexes(IndexLock &state, BoundIndex &other_index) override;

	//! Vacuums the ART storage.
	void Vacuum(IndexLock &state) override;

	//! Returns ART storage serialization information.
	IndexStorageInfo GetStorageInfo(const case_insensitive_map_t<Value> &options, const bool to_wal) override;
	//! Returns the in-memory usage of the ART.
	idx_t GetInMemorySize(IndexLock &index_lock) override;

	//! ART key generation.
	template <bool IS_NOT_NULL = false>
	static void GenerateKeys(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys);
	static void GenerateKeyVectors(ArenaAllocator &allocator, DataChunk &input, Vector &row_ids,
	                               unsafe_vector<ARTKey> &keys, unsafe_vector<ARTKey> &row_id_keys);

	//! Verifies the nodes and optionally returns a string of the ART.
	string VerifyAndToString(IndexLock &state, const bool only_verify) override;
	//! Verifies that the node allocations match the node counts.
	void VerifyAllocations(IndexLock &state) override;

private:
	bool SearchEqual(ARTKey &key, idx_t max_count, unsafe_vector<row_t> &row_ids);
	bool SearchGreater(ARTKey &key, bool equal, idx_t max_count, unsafe_vector<row_t> &row_ids);
	bool SearchLess(ARTKey &upper_bound, bool equal, idx_t max_count, unsafe_vector<row_t> &row_ids);
	bool SearchCloseRange(ARTKey &lower_bound, ARTKey &upper_bound, bool left_equal, bool right_equal, idx_t max_count,
	                      unsafe_vector<row_t> &row_ids);
	const unsafe_optional_ptr<const Node> Lookup(const Node &node, const ARTKey &key, idx_t depth);

	void InsertIntoEmpty(Node &node, const ARTKey &key, const idx_t depth, const ARTKey &row_id,
	                     const GateStatus status);
	bool InsertIntoNode(Node &node, const ARTKey &key, const idx_t depth, const ARTKey &row_id,
	                    const GateStatus status);

	string GenerateErrorKeyName(DataChunk &input, idx_t row);
	string GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name);
	void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) override;
	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                     DataChunk &input) override;

	void Erase(Node &node, reference<const ARTKey> key, idx_t depth, reference<const ARTKey> row_id, GateStatus status);

	bool ConstructInternal(const unsafe_vector<ARTKey> &keys, const unsafe_vector<ARTKey> &row_ids, Node &node,
	                       ARTKeySection &section);

	void InitializeMerge(unsafe_vector<idx_t> &upper_bounds);

	void InitializeVacuum(unordered_set<uint8_t> &indexes);
	void FinalizeVacuum(const unordered_set<uint8_t> &indexes);

	void InitAllocators(const IndexStorageInfo &info);
	void TransformToDeprecated();
	void Deserialize(const BlockPointer &pointer);
	void WritePartialBlocks(const bool v1_0_0_storage);
	void SetPrefixCount(const IndexStorageInfo &info);

	string VerifyAndToStringInternal(const bool only_verify);
	void VerifyAllocationsInternal();
};

template <>
void ART::GenerateKeys<>(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys);

template <>
void ART::GenerateKeys<true>(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys);

} // namespace duckdb
