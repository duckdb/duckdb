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

// classes
enum class VerifyExistenceType : uint8_t {
	APPEND = 0,    // appends to a table
	APPEND_FK = 1, // appends to a table that has a foreign key
	DELETE_FK = 2  // delete from a table that has a foreign key
};
class ConflictManager;
class ARTKey;
class FixedSizeAllocator;

// structs
struct ARTIndexScanState;
struct ARTFlags {
	vector<bool> vacuum_flags;
	vector<idx_t> merge_buffer_counts;
};

class ART : public BoundIndex {
public:
	// Index type name for the ART
	static constexpr const char *TYPE_NAME = "ART";
	//! FixedSizeAllocator count of the ART
	static constexpr uint8_t ALLOCATOR_COUNT = 6;

public:
	//! Constructs an ART
	ART(const string &name, const IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
	    TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
	    AttachedDatabase &db,
	    const shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr = nullptr,
	    const IndexStorageInfo &info = IndexStorageInfo());

	//! Root of the tree
	Node tree = Node();
	//! Fixed-size allocators holding the ART nodes
	shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> allocators;
	//! True, if the ART owns its data
	bool owns_data;

	//! Try to initialize a scan on the index with the given expression and filter
	unique_ptr<IndexScanState> TryInitializeScan(const Transaction &transaction, const Expression &index_expr,
	                                             const Expression &filter_expr);

	//! Performs a lookup on the index, fetching up to max_count result IDs. Returns true if all row IDs were fetched,
	//! and false otherwise
	bool Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, idx_t max_count,
	          vector<row_t> &result_ids);

public:
	//! Create a index instance of this type
	static unique_ptr<BoundIndex> Create(CreateIndexInput &input) {
		auto art = make_uniq<ART>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                          input.unbound_expressions, input.db, nullptr, input.storage_info);
		return std::move(art);
	}

	//! Called when data is appended to the index. The lock obtained from InitializeLock must be held
	ErrorData Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Verify that data can be appended to the index without a constraint violation
	void VerifyAppend(DataChunk &chunk) override;
	//! Verify that data can be appended to the index without a constraint violation using the conflict manager
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;
	//! Deletes all data from the index. The lock obtained from InitializeLock must be held
	void CommitDrop(IndexLock &index_lock) override;
	//! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert a chunk of entries into the index
	ErrorData Insert(IndexLock &lock, DataChunk &data, Vector &row_identifiers) override;

	//! Construct an ART from a vector of sorted keys
	bool ConstructFromSorted(idx_t count, vector<ARTKey> &keys, Vector &row_identifiers);

	//! Search equal values and fetches the row IDs
	bool SearchEqual(ARTKey &key, idx_t max_count, vector<row_t> &result_ids);

	//! Returns all ART storage information for serialization
	IndexStorageInfo GetStorageInfo(const bool get_buffers) override;

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	bool MergeIndexes(IndexLock &state, BoundIndex &other_index) override;

	//! Traverses an ART and vacuums the qualifying nodes. The lock obtained from InitializeLock must be held
	void Vacuum(IndexLock &state) override;

	//! Returns the in-memory usage of the index. The lock obtained from InitializeLock must be held
	idx_t GetInMemorySize(IndexLock &index_lock) override;

	//! Generate ART keys for an input chunk
	template <bool IS_NOT_NULL = false>
	static void GenerateKeys(ArenaAllocator &allocator, DataChunk &input, vector<ARTKey> &keys);

	//! Generate a string containing all the expressions and their respective values that violate a constraint
	string GenerateErrorKeyName(DataChunk &input, idx_t row);
	//! Generate the matching error message for a constraint violation
	string GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name);
	//! Performs constraint checking for a chunk of input data
	void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) override;

	//! Returns the string representation of the ART, or only traverses and verifies the index
	string VerifyAndToString(IndexLock &state, const bool only_verify) override;

	//! Find the node with a matching key, or return nullptr if not found
	optional_ptr<const Node> Lookup(const Node &node, const ARTKey &key, idx_t depth);
	//! Insert a key into the tree
	bool Insert(Node &node, const ARTKey &key, idx_t depth, const row_t &row_id);

private:
	//! Insert a row ID into a leaf
	bool InsertToLeaf(Node &leaf, const row_t &row_id);
	//! Erase a key from the tree (if a leaf has more than one value) or erase the leaf itself
	void Erase(Node &node, const ARTKey &key, idx_t depth, const row_t &row_id);

	//! Returns all row IDs belonging to a key greater (or equal) than the search key
	bool SearchGreater(ARTIndexScanState &state, ARTKey &key, bool equal, idx_t max_count, vector<row_t> &result_ids);
	//! Returns all row IDs belonging to a key less (or equal) than the upper_bound
	bool SearchLess(ARTIndexScanState &state, ARTKey &upper_bound, bool equal, idx_t max_count,
	                vector<row_t> &result_ids);
	//! Returns all row IDs belonging to a key within the range of lower_bound and upper_bound
	bool SearchCloseRange(ARTIndexScanState &state, ARTKey &lower_bound, ARTKey &upper_bound, bool left_equal,
	                      bool right_equal, idx_t max_count, vector<row_t> &result_ids);

	//! Initializes a merge operation by returning a set containing the buffer count of each fixed-size allocator
	void InitializeMerge(ARTFlags &flags);

	//! Initializes a vacuum operation by calling the initialize operation of the respective
	//! node allocator, and returns a vector containing either true, if the allocator at
	//! the respective position qualifies, or false, if not
	void InitializeVacuum(ARTFlags &flags);
	//! Finalizes a vacuum operation by calling the finalize operation of all qualifying
	//! fixed size allocators
	void FinalizeVacuum(const ARTFlags &flags);

	//! Internal function to return the string representation of the ART,
	//! or only traverses and verifies the index
	string VerifyAndToStringInternal(const bool only_verify);

	//! Initialize the allocators of the ART
	void InitAllocators(const IndexStorageInfo &info);
	//! STABLE STORAGE NOTE: This is for old storage files, to deserialize the allocators of the ART
	void Deserialize(const BlockPointer &pointer);
	//! Initializes the serialization of the index by combining the allocator data onto partial blocks
	void WritePartialBlocks();

	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                     DataChunk &input) override;
};

template <>
void ART::GenerateKeys<>(ArenaAllocator &allocator, DataChunk &input, vector<ARTKey> &keys);

template <>
void ART::GenerateKeys<true>(ArenaAllocator &allocator, DataChunk &input, vector<ARTKey> &keys);

} // namespace duckdb
