//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

class ConflictManager;
struct ARTIndexScanState;

enum class VerifyExistenceType : uint8_t {
	APPEND = 0,    // appends to a table
	APPEND_FK = 1, // appends to a table that has a foreign key
	DELETE_FK = 2  // delete from a table that has a foreign key
};

class ART : public Index {
public:
	//! Constructs an ART containing the bound expressions, which are resolved during index construction
	ART(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
	    const vector<unique_ptr<Expression>> &unbound_expressions, IndexConstraintType constraint_type,
	    AttachedDatabase &db, bool track_memory, idx_t block_id = DConstants::INVALID_INDEX,
	    idx_t block_offset = DConstants::INVALID_INDEX);
	~ART() override;

	//! Root of the tree
	Node *tree;

public:
	//! Initialize a single predicate scan on the index with the given expression and column IDs
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
	                                                         ExpressionType expression_type) override;
	//! Initialize a two predicate scan on the index with the given expression and column IDs
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, const Value &low_value,
	                                                       ExpressionType low_expression_type, const Value &high_value,
	                                                       ExpressionType high_expression_type) override;
	//! Performs a lookup on the index, fetching up to max_count result IDs. Returns true if all row IDs were fetched,
	//! and false otherwise
	bool Scan(Transaction &transaction, DataTable &table, IndexScanState &state, idx_t max_count,
	          vector<row_t> &result_ids) override;

	//! Called when data is appended to the index. The lock obtained from InitializeLock must be held
	PreservedError Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Verify that data can be appended to the index without a constraint violation
	void VerifyAppend(DataChunk &chunk) override;
	//! Verify that data can be appended to the index without a constraint violation using the conflict manager
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;
	//! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert a chunk of entries into the index
	PreservedError Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	//! Construct an ART from a vector of sorted keys
	bool ConstructFromSorted(idx_t count, vector<Key> &keys, Vector &row_identifiers);

	//! Search equal values and fetches the row IDs
	bool SearchEqual(Key &key, idx_t max_count, vector<row_t> &result_ids);
	//! Search equal values used for joins that do not need to fetch data
	void SearchEqualJoinNoFetch(Key &key, idx_t &result_size);

	//! Serializes the index and returns the pair of block_id offset positions
	BlockPointer Serialize(duckdb::MetaBlockWriter &writer) override;

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	bool MergeIndexes(IndexLock &state, Index *other_index) override;

	//! Generate ART keys for an input chunk
	static void GenerateKeys(ArenaAllocator &allocator, DataChunk &input, vector<Key> &keys);

	//! Generate a string containing all the expressions and their respective values that violate a constraint
	string GenerateErrorKeyName(DataChunk &input, idx_t row);
	//! Generate the matching error message for a constraint violation
	string GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name);
	//! Performs constraint checking for a chunk of input data
	void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) override;

	//! Returns the string representation of an ART
	string ToString() override;
	//! Verifies that the in-memory size value of the index matches its actual size
	void Verify() override;
	//! Increases the memory size by the difference between the old size and the current size
	//! and performs verifications
	void IncreaseAndVerifyMemorySize(idx_t old_memory_size) override;

private:
	//! Insert a row ID into a leaf
	bool InsertToLeaf(Leaf &leaf, row_t row_id);
	//! Insert a key into the tree
	bool Insert(Node *&node, Key &key, idx_t depth, row_t row_id);
	//! Erase a key from the tree (if a leaf has more than one value) or erase the leaf itself
	void Erase(Node *&node, Key &key, idx_t depth, row_t row_id);
	//! Find the node with a matching key, or return nullptr if not found
	Leaf *Lookup(Node *node, Key &key, idx_t depth);
	//! Returns all row IDs belonging to a key greater (or equal) than the search key
	bool SearchGreater(ARTIndexScanState *state, Key &key, bool inclusive, idx_t max_count, vector<row_t> &result_ids);
	//! Returns all row IDs belonging to a key less (or equal) than the upper_bound
	bool SearchLess(ARTIndexScanState *state, Key &upper_bound, bool inclusive, idx_t max_count,
	                vector<row_t> &result_ids);
	//! Returns all row IDs belonging to a key within the range of lower_bound and upper_bound
	bool SearchCloseRange(ARTIndexScanState *state, Key &lower_bound, Key &upper_bound, bool left_inclusive,
	                      bool right_inclusive, idx_t max_count, vector<row_t> &result_ids);
};

} // namespace duckdb
