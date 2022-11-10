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
	Leaf(Key &value, uint32_t depth, row_t *row_ids, idx_t num_elements);
	Leaf(unique_ptr<row_t[]> row_ids, idx_t num_elements, Prefix &prefix);
	Leaf(row_t row_id, Prefix &prefix);
	~Leaf();

	row_t GetRowId(idx_t index);
	idx_t GetCapacity() const;
	bool IsInlined() const;
	row_t *GetRowIds();

public:
	static Leaf *New(Key &value, uint32_t depth, row_t row_id);
	static Leaf *New(Key &value, uint32_t depth, row_t *row_ids, idx_t num_elements);
	static Leaf *New(unique_ptr<row_t[]> row_ids, idx_t num_elements, Prefix &prefix);
	static Leaf *New(row_t row_id, Prefix &prefix);
	//! Insert a row_id into a leaf
	void Insert(row_t row_id);
	//! Remove a row_id from a leaf
	void Remove(row_t row_id);

	//! Returns the string representation of a leaf
	static string ToString(Node *node);
	//! Merge two NLeaf nodes
	static void Merge(Node *&l_node, Node *&r_node);

	//! Serialize a leaf
	BlockPointer Serialize(duckdb::MetaBlockWriter &writer);
	// Deserialize a leaf
	static Leaf *Deserialize(duckdb::MetaBlockReader &reader);

private:
	union {
		row_t inlined;
		row_t *ptr;
	} rowids;

private:
	row_t *Resize(row_t *current_row_ids, uint32_t current_count, idx_t new_capacity);
};

} // namespace duckdb
