//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/index.hpp"
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

class ART : public Index {
public:
	//! FixedSizeAllocator count of the ART
	static constexpr uint8_t ALLOCATOR_COUNT = 6;

public:
	//! Constructs an ART
	ART(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
	    const vector<unique_ptr<Expression>> &unbound_expressions, const IndexConstraintType constraint_type,
	    AttachedDatabase &db,
	    const shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr = nullptr,
	    const BlockPointer &block = BlockPointer());

	//! Root of the tree
	Node tree = Node();
	//! Fixed-size allocators holding the ART nodes
	shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> allocators;
	//! True, if the ART owns its data
	bool owns_data;

public:
	//! Initialize a single predicate scan on the index with the given expression and column IDs
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
	                                                         const ExpressionType expression_type) override;
	//! Initialize a two predicate scan on the index with the given expression and column IDs
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(const Transaction &transaction, const Value &low_value,
	                                                       const ExpressionType low_expression_type,
	                                                       const Value &high_value,
	                                                       const ExpressionType high_expression_type) override;
	//! Performs a lookup on the index, fetching up to max_count result IDs. Returns true if all row IDs were fetched,
	//! and false otherwise
	bool Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, const idx_t max_count,
	          vector<row_t> &result_ids) override;

	//! Called when data is appended to the index. The lock obtained from InitializeLock must be held
	PreservedError Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Verify that data can be appended to the index without a constraint violation
	void VerifyAppend(DataChunk &chunk) override;
	//! Verify that data can be appended to the index without a constraint violation using the conflict manager
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;
	//! Deletes all data from the index. The lock obtained from InitializeLock must be held
	void CommitDrop(IndexLock &index_lock) override;
	//! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert a chunk of entries into the index
	PreservedError Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	//! Construct an ART from a vector of sorted keys
	bool ConstructFromSorted(idx_t count, vector<ARTKey> &keys, Vector &row_identifiers);

	//! Search equal values and fetches the row IDs
	bool SearchEqual(ARTKey &key, idx_t max_count, vector<row_t> &result_ids);
	//! Search equal values used for joins that do not need to fetch data
	void SearchEqualJoinNoFetch(ARTKey &key, idx_t &result_size);

	//! Serializes the index and returns the pair of block_id offset positions
	BlockPointer Serialize(MetadataWriter &writer) override;

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	bool MergeIndexes(IndexLock &state, Index &other_index) override;

	//! Traverses an ART and vacuums the qualifying nodes. The lock obtained from InitializeLock must be held
	void Vacuum(IndexLock &state) override;

	//! Generate ART keys for an input chunk
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

	//! Deserialize the allocators of the ART
	void Deserialize(const BlockPointer &pointer);
};

} // namespace duckdb
