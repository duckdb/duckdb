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

enum VerifyExistenceType : uint8_t {
	APPEND = 0,    // for purpose to append into table
	APPEND_FK = 1, // for purpose to append into table has foreign key
	DELETE_FK = 2  // for purpose to delete from table related to foreign key
};

class ART : public Index {
public:
	ART(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
	    const vector<unique_ptr<Expression>> &unbound_expressions, IndexConstraintType constraint_type,
	    DatabaseInstance &db, idx_t block_id = DConstants::INVALID_INDEX,
	    idx_t block_offset = DConstants::INVALID_INDEX);
	~ART() override;

	//! Root of the tree
	Node *tree;

	DatabaseInstance &db;

public:
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for a single predicate
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, Value value,
	                                                         ExpressionType expression_type) override;

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for two predicates
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, Value low_value,
	                                                       ExpressionType low_expression_type, Value high_value,
	                                                       ExpressionType high_expression_type) override;

	//! Perform a lookup on the index
	bool Scan(Transaction &transaction, DataTable &table, IndexScanState &state, idx_t max_count,
	          vector<row_t> &result_ids) override;
	//! Append entries to the index
	bool Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Verify that data can be appended to the index
	void VerifyAppend(DataChunk &chunk) override;
	//! Verify that data can be appended to the index for foreign key constraint
	void VerifyAppendForeignKey(DataChunk &chunk, string *err_msg_ptr) override;
	//! Verify that data can be delete from the index for foreign key constraint
	void VerifyDeleteForeignKey(DataChunk &chunk, string *err_msg_ptr) override;
	//! Delete entries in the index
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert data into the index.
	bool Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	//! Construct ARTs from sorted chunks and merge them.
	void ConstructAndMerge(IndexLock &lock, PayloadScanner &scanner, Allocator &allocator) override;

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
	//! Returns the string representation of an ART
	string ToString() override;

private:
	//! Insert a row id into a leaf node
	bool InsertToLeaf(Leaf &leaf, row_t row_id);
	//! Insert the leaf value into the tree
	bool Insert(Node *&node, Key &key, idx_t depth, row_t row_id);

	//! Erase element from leaf (if leaf has more than one value) or eliminate the leaf itself
	void Erase(Node *&node, Key &key, idx_t depth, row_t row_id);

	//! Find the node with a matching key, optimistic version
	Leaf *Lookup(Node *node, Key &key, idx_t depth);

	bool SearchGreater(ARTIndexScanState *state, Key &key, bool inclusive, idx_t max_count, vector<row_t> &result_ids);
	bool SearchLess(ARTIndexScanState *state, Key &upper_bound, bool inclusive, idx_t max_count,
	                vector<row_t> &result_ids);
	bool SearchCloseRange(ARTIndexScanState *state, Key &lower_bound, Key &upper_bound, bool left_inclusive,
	                      bool right_inclusive, idx_t max_count, vector<row_t> &result_ids);

	void VerifyExistence(DataChunk &chunk, VerifyExistenceType verify_type, string *err_msg_ptr = nullptr);
};

} // namespace duckdb
