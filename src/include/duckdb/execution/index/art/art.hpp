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
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index.hpp"

#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {
struct IteratorEntry {
	IteratorEntry() {
	}
	IteratorEntry(Node *node, idx_t pos) : node(node), pos(pos) {
	}

	Node *node = nullptr;
	idx_t pos = 0;
};

struct Iterator {
	//! The current Leaf Node, valid if depth>0
	Leaf *node = nullptr;
	//! The current depth
	int32_t depth = 0;
	//! Stack, the size is determined at runtime
	vector<IteratorEntry> stack;

	bool start = false;

	void SetEntry(idx_t depth, IteratorEntry entry);
};

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
	ART(const vector<column_t> &column_ids, const vector<unique_ptr<Expression>> &unbound_expressions,
	    IndexConstraintType constraint_type);
	~ART() override;

	//! Root of the tree
	unique_ptr<Node> tree;
	//! True if machine is little endian
	bool is_little_endian;

public:
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for a single predicate
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, Value value,
	                                                         ExpressionType expressionType) override;

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

	bool SearchEqual(ARTIndexScanState *state, idx_t max_count, vector<row_t> &result_ids);
	//! Search Equal used for Joins that do not need to fetch data
	void SearchEqualJoinNoFetch(Value &equal_value, idx_t &result_size);

private:
	DataChunk expression_result;

private:
	//! Insert a row id into a leaf node
	bool InsertToLeaf(Leaf &leaf, row_t row_id);
	//! Insert the leaf value into the tree
	bool Insert(unique_ptr<Node> &node, unique_ptr<Key> key, unsigned depth, row_t row_id);

	//! Erase element from leaf (if leaf has more than one value) or eliminate the leaf itself
	void Erase(unique_ptr<Node> &node, Key &key, unsigned depth, row_t row_id);

	//! Check if the key of the leaf is equal to the searched key
	bool LeafMatches(Node *node, Key &key, unsigned depth);

	//! Find the node with a matching key, optimistic version
	Node *Lookup(unique_ptr<Node> &node, Key &key, unsigned depth);

	//! Find the first node that is bigger (or equal to) a specific key
	bool Bound(unique_ptr<Node> &node, Key &key, Iterator &iterator, bool inclusive);

	//! Gets next node for range queries
	bool IteratorNext(Iterator &iter);

	bool SearchGreater(ARTIndexScanState *state, bool inclusive, idx_t max_count, vector<row_t> &result_ids);
	bool SearchLess(ARTIndexScanState *state, bool inclusive, idx_t max_count, vector<row_t> &result_ids);
	bool SearchCloseRange(ARTIndexScanState *state, bool left_inclusive, bool right_inclusive, idx_t max_count,
	                      vector<row_t> &result_ids);

private:
	template <bool HAS_BOUND, bool INCLUSIVE>
	bool IteratorScan(ARTIndexScanState *state, Iterator *it, Key *upper_bound, idx_t max_count,
	                  vector<row_t> &result_ids);

	void GenerateKeys(DataChunk &input, vector<unique_ptr<Key>> &keys);

	void VerifyExistence(DataChunk &chunk, VerifyExistenceType verify_type, string *err_msg_ptr = NULL);
};

} // namespace duckdb
