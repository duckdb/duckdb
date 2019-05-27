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
	Leaf(uint64_t value, row_t row_id, uint8_t max_prefix_length);

	uint64_t value;
	index_t capacity;
	index_t num_elements;
	unique_ptr<row_t[]> row_id;
public:
	static void insert(Leaf *leaf, row_t row_id) ;

	//! TODO: Maybe shrink array dynamically?
	static void remove(Leaf *leaf, row_t row_id);
};

}
