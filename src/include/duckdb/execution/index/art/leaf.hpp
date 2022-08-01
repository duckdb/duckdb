//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

class Leaf : public Node {
public:
	Leaf(unique_ptr<Key> value, row_t row_id);

	Leaf(unique_ptr<Key> value, unique_ptr<row_t[]> row_ids, idx_t num_elements);
	unique_ptr<Key> value;
	idx_t capacity;
	idx_t num_elements;

	row_t GetRowId(idx_t index) {
		return row_ids[index];
	}

public:
	void Insert(row_t row_id);
	void Remove(row_t row_id);

	//! Merge NLeaf with another Node
	static void Merge(Node *l_node, Node *r_node, idx_t depth);
	//! Merge two NLeaf nodes
	static void MergeNLeafNLeaf(Leaf *l_node, Leaf *r_node, idx_t depth);
	//! Merge NLeaf with Node4
	static void MergeNLeafNode4(Leaf *l_node, Node4 *r_node, idx_t depth);
	//! Merge NLeaf with Node16
	static void MergeNLeafNode16(Leaf *l_node, Node16 *r_node, idx_t depth);
	//! Merge NLeaf with Node48
	static void MergeNLeafNode48(Leaf *l_node, Node48 *r_node, idx_t depth);
	//! Merge NLeaf with Node256
	static void MergeNLeafNode256(Leaf *l_node, Node256 *r_node, idx_t depth);

	BlockPointer SerializeLeaf(duckdb::MetaBlockWriter &writer);

	static Leaf *Deserialize(duckdb::MetaBlockReader &reader, uint32_t value_length);

private:
	unique_ptr<row_t[]> row_ids;
};

} // namespace duckdb
