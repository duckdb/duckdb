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

struct ARTIndexScanState : public IndexScanState {
	ARTIndexScanState() : checked(false), result_index(0) {
	}

	Value values[2];
	ExpressionType expressions[2];
	bool checked;
	vector<row_t> result_ids;
	Iterator iterator;
	//! Stores the current leaf
	Leaf *cur_leaf = nullptr;
	//! Offset to leaf
	idx_t result_index = 0;
};

enum class VerifyExistenceType : uint8_t {
	APPEND = 0,    // for purpose to append into table
	APPEND_FK = 1, // for purpose to append into table has foreign key
	DELETE_FK = 2  // for purpose to delete from table related to foreign key
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
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for a single predicate
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
	                                                         ExpressionType expression_type) override;

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for two predicates
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, const Value &low_value,
	                                                       ExpressionType low_expression_type, const Value &high_value,
	                                                       ExpressionType high_expression_type) override;

	//! Perform a lookup on the index
	bool Scan(Transaction &transaction, DataTable &table, IndexScanState &state, idx_t max_count,
	          vector<row_t> &result_ids) override;
	//! Append entries to the index
	bool Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Verify that data can be appended to the index
	void VerifyAppend(DataChunk &chunk) override;
	//! Verify that data can be appended to the index
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;
	//! Verify that data can be appended to the index for foreign key constraint
	void VerifyAppendForeignKey(DataChunk &chunk) override;
	//! Verify that data can be delete from the index for foreign key constraint
	void VerifyDeleteForeignKey(DataChunk &chunk) override;
	//! Delete entries in the index
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert data into the index
	bool Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	//! Construct an ART from a vector of sorted keys
	void ConstructFromSorted(idx_t count, vector<Key> &keys, Vector &row_identifiers);

	//! Search Equal and fetches the row IDs
	bool SearchEqual(Key &key, idx_t max_count, vector<row_t> &result_ids);
	//! Search Equal used for Joins that do not need to fetch data
	void SearchEqualJoinNoFetch(Key &key, idx_t &result_size);
	//! Serialized the ART
	BlockPointer Serialize(duckdb::MetaBlockWriter &writer) override;

	//! Merge two ARTs
	bool MergeIndexes(IndexLock &state, Index *other_index) override;
	//! Generate ART keys for an input chunk
	static void GenerateKeys(ArenaAllocator &allocator, DataChunk &input, vector<Key> &keys);

	//! Generate a string containing all the expressions and their respective values that violate a constraint
	string GenerateErrorKeyName(DataChunk &input, idx_t row);
	//! Generate the matching error message for a constraint violation
	string GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name);

	//! Returns the string representation of an ART
	string ToString() override;
	//! Verifies that the memory_size value of the ART matches its actual size
	void Verify() override;

private:
	//! Insert a row id into a leaf node
	bool InsertToLeaf(Leaf &leaf, row_t row_id);
	//! Insert the leaf value into the tree
	bool Insert(Node *&node, Key &key, idx_t depth, row_t row_id);

	//! Erase element from leaf (if leaf has more than one value) or eliminate the leaf itself
	void Erase(Node *&node, Key &key, idx_t depth, row_t row_id);

	//! Perform 'Lookup' for an entire chunk, marking which succeeded
	void LookupValues(DataChunk &input, ConflictManager &conflict_manager) final override;

	//! Find the node with a matching key, optimistic version
	Leaf *Lookup(Node *node, Key &key, idx_t depth);

	bool SearchGreater(ARTIndexScanState *state, Key &key, bool inclusive, idx_t max_count, vector<row_t> &result_ids);
	bool SearchLess(ARTIndexScanState *state, Key &upper_bound, bool inclusive, idx_t max_count,
	                vector<row_t> &result_ids);
	bool SearchCloseRange(ARTIndexScanState *state, Key &lower_bound, Key &upper_bound, bool left_inclusive,
	                      bool right_inclusive, idx_t max_count, vector<row_t> &result_ids);
};

} // namespace duckdb
