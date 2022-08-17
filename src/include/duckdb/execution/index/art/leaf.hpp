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

	BlockPointer SerializeLeaf(duckdb::MetaBlockWriter &writer);

	static Leaf *Deserialize(duckdb::MetaBlockReader &reader, uint32_t value_length);

private:
	unique_ptr<row_t[]> row_ids;
};

} // namespace duckdb
