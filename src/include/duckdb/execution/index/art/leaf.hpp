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

namespace duckdb {

class Leaf : public Node {
public:
	Leaf(Key &value, uint32_t depth, row_t row_id);
	Leaf(Key &value, uint32_t depth, unique_ptr<row_t[]> row_ids, idx_t num_elements);
	Leaf(unique_ptr<row_t[]> row_ids, idx_t num_elements, Prefix &prefix);

	idx_t capacity;

	row_t GetRowId(idx_t index) {
		return row_ids[index];
	}

public:
	//! Insert a row_id into a leaf
	void Insert(row_t row_id);
	//! Remove a row_id from a leaf
	void Remove(row_t row_id);

	//! Merge two NLeaf nodes
	static void Merge(bool &has_constraint, Node *&l_node, Node *&r_node);

	//! Serialize a leaf
	BlockPointer Serialize(duckdb::MetaBlockWriter &writer);
	// Deserialize a leaf
	static Leaf *Deserialize(duckdb::MetaBlockReader &reader);

private:
	unique_ptr<row_t[]> row_ids;
};

} // namespace duckdb
