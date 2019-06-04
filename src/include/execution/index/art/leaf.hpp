//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "node.hpp"

namespace duckdb {

class Leaf : public Node {
public:
	Leaf(ART &art, uint64_t value, row_t row_id);

	uint64_t value;
	index_t capacity;
	index_t num_elements;
	unique_ptr<row_t[]> row_ids;
public:
	void Insert(row_t row_id);
	void Remove(row_t row_id);
};

}
