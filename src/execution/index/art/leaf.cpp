#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/leaf.hpp"

#include <cstring>

using namespace duckdb;
using namespace std;

Leaf::Leaf(ART &art, unique_ptr<Key> value, row_t row_id) : Node(art, NodeType::NLeaf, 0) {
	this->value = move(value);
	this->capacity = 1;
	this->row_ids = unique_ptr<row_t[]>(new row_t[this->capacity]);
	this->row_ids[0] = row_id;
	this->num_elements = 1;
}

void Leaf::Insert(row_t row_id) {
	// Grow array
	if (num_elements == capacity) {
		auto new_row_id = unique_ptr<row_t[]>(new row_t[capacity * 2]);
		memcpy(new_row_id.get(), row_ids.get(), capacity * sizeof(row_t));
		capacity *= 2;
		row_ids = move(new_row_id);
	}
	row_ids[num_elements++] = row_id;
}

//! TODO: Maybe shrink array dynamically?
void Leaf::Remove(row_t row_id) {
	idx_t entry_offset = INVALID_INDEX;
	for (idx_t i = 0; i < num_elements; i++) {
		if (row_ids[i] == row_id) {
			entry_offset = i;
			break;
		}
	}
	if (entry_offset == INVALID_INDEX) {
		return;
	}
	num_elements--;
	for (idx_t j = entry_offset; j < num_elements; j++) {
		row_ids[j] = row_ids[j + 1];
	}
}
