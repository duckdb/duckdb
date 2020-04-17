//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class Leaf : public Node {
public:
	Leaf(ART &art, unique_ptr<Key> value, row_t row_id);

	unique_ptr<Key> value;
	idx_t capacity;
	idx_t num_elements;

	row_t GetRowId(idx_t index) {
		return row_ids[index];
	}

public:
	void Insert(row_t row_id);
	void Remove(row_t row_id);

private:
	unique_ptr<row_t[]> row_ids;
};

} // namespace duckdb
