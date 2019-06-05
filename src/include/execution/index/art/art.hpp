//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/art.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "common/types/vector.hpp"
#include "parser/parsed_expression.hpp"
#include "storage/data_table.hpp"
#include "storage/index.hpp"
#include "common/types/static_vector.hpp"
#include "art_key.hpp"
#include "leaf.hpp"
#include "node.hpp"
#include "node4.hpp"
#include "node16.hpp"
#include "node48.hpp"
#include "node256.hpp"

namespace duckdb {
struct IteratorEntry {
	Node *node;
	int pos;
};

struct Iterator {
	//! The current Leaf Node, valid if depth>0
	Leaf *node;
	//! The current depth
	int32_t depth = 0;
	//! Stack, actually the size is determined at runtime
	IteratorEntry stack[9];

	bool start = false;
};

struct ARTIndexScanState : public IndexScanState {
	ARTIndexScanState(vector<column_t> column_ids) : IndexScanState(column_ids), checked(false) {
	}

	Value values[2];
	ExpressionType expressions[2];
	bool checked;
	uint64_t pointquery_tuple = 0;
	Iterator iterator;
};

class ART : public Index {
public:
	ART(DataTable &table, vector<column_t> column_ids, vector<unique_ptr<Expression>> unbound_expressions, bool is_unique = false);
	~ART();

	//! Lock used for updating the index
	std::mutex lock;
	//! Root of the tree
	unique_ptr<Node> tree;
	//! True if machine is little endian
	bool is_little_endian;
	//! The maximum prefix length for compressed paths stored in the
	//! header, if the path is longer it is loaded from the database on demand
	uint32_t maxPrefix;
	//! Whether or not the ART is an index built to enforce a UNIQUE constraint
	bool is_unique;
public:
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for a single predicate
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, vector<column_t> column_ids,
	                                                         Value value, ExpressionType expressionType) override;

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for two predicates
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, vector<column_t> column_ids,
	                                                       Value low_value, ExpressionType low_expression_type,
	                                                       Value high_value,
	                                                       ExpressionType high_expression_type) override;

	//! Perform a lookup on the index
	void Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) override;
	//! Append entries to the index
	void Append(ClientContext &context, DataChunk &entries, uint64_t row_identifier_start) override;
	//! Update entries in the index
	void Update(ClientContext &context, vector<column_t> &column_ids, DataChunk &update_data,
	            Vector &row_identifiers) override;
	//! Delete entries in the index
	void Delete(DataChunk &entries, Vector &row_identifiers) override;

	//! Insert data into the index. Does not lock the index.
	void Insert(ClientContext &context, DataChunk &data, Vector &row_ids);
private:
	DataChunk expression_result;
private:
	//! Insert a row id into a leaf node
	void InsertToLeaf(ClientContext &context, Leaf &leaf, row_t row_id);
	//! Insert the leaf value into the tree
	void Insert(ClientContext &context, unique_ptr<Node> &node, Key &key, unsigned depth, uintptr_t value, TypeId type, row_t row_id);

	//! Erase element from leaf (if leaf has more than one value) or eliminate the leaf itself
	void Erase(unique_ptr<Node> &node, Key &key, unsigned depth, TypeId type, row_t row_id);

	//! Check if the key of the leaf is equal to the searched key
	bool LeafMatches(Node *node, Key &key, unsigned depth);

	//! Find the node with a matching key, optimistic version
	Node *Lookup(unique_ptr<Node> &node, Key &key, unsigned depth);

	//! Find the iterator position for bound queries
	bool Bound(unique_ptr<Node> &node, Key &key, Iterator &iterator, bool inclusive);

	//! Gets next node for range queries
	bool IteratorNext(Iterator &iter);

	void SearchEqual(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state);
	void SearchGreater(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool inclusive);
	void SearchLess(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool inclusive);
	void SearchCloseRange(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool left_inclusive,
	                      bool right_inclusive);
private:
	template <class T>
	void templated_insert(ClientContext &context, DataChunk &input, Vector &row_ids);
	template <class T>
	void templated_delete(DataChunk &input, Vector &row_ids);
	template <class T>
	index_t templated_lookup(TypeId type, T data, int64_t *result_ids, ARTIndexScanState *state);
	template <class T>
	index_t templated_greater_scan(TypeId type, T data, int64_t *result_ids, bool inclusive,
	                                ARTIndexScanState *state);
	template <class T>
	index_t templated_less_scan(TypeId type, T data, int64_t *result_ids, bool inclusive, ARTIndexScanState *state);
	template <class T>
	index_t templated_close_range(TypeId type, T left_query, T right_query, int64_t *result_ids, bool left_inclusive,
	                               bool right_inclusive, ARTIndexScanState *state);
};

} // namespace duckdb
