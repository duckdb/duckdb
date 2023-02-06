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
	explicit Leaf();
	Leaf(Key &value, uint32_t depth, row_t row_id);
	Leaf(Key &value, uint32_t depth, row_t *row_ids, idx_t num_elements);
	Leaf(row_t *row_ids, idx_t num_elements, Prefix &prefix);
	Leaf(row_t row_id, Prefix &prefix);
	~Leaf();

	//! Get the row ID at idx
	row_t GetRowId(idx_t idx);
	//! Get the maximum capacity of the leaf, must not match with its count
	idx_t GetCapacity() const;
	//! Returns whether a leaf holds exactly one inlined row ID or multiple row IDs
	bool IsInlined() const;
	//! Returns a pointer to all row IDs of the leaf
	row_t *GetRowIds();

public:
	static Leaf *New();
	static Leaf *New(Key &value, uint32_t depth, row_t row_id);
	static Leaf *New(Key &value, uint32_t depth, row_t *row_ids, idx_t num_elements);
	static Leaf *New(row_t *row_ids, idx_t num_elements, Prefix &prefix);
	static Leaf *New(row_t row_id, Prefix &prefix);

	//! Returns the memory size of the leaf
	idx_t MemorySize(ART &art, const bool &recurse) override;
	//! Insert a row ID into a leaf
	void Insert(ART &art, row_t row_id);
	//! Remove a row ID from a leaf
	void Remove(ART &art, row_t row_id);

	//! Returns the string representation of a leaf
	static string ToString(Node *node);
	//! Merge two NLeaf nodes
	static void Merge(ART &art, Node *&l_node, Node *&r_node);

	//! Serialize a leaf
	BlockPointer Serialize(duckdb::MetaBlockWriter &writer);
	//! Deserialize a leaf
	void Deserialize(ART &art, duckdb::MetaBlockReader &reader);

private:
	union {
		row_t inlined;
		row_t *ptr;
	} rowids;

private:
	row_t *Resize(row_t *current_row_ids, uint32_t current_count, idx_t new_capacity);
};

} // namespace duckdb
