#include "execution/index/art/node.hpp"
#include "execution/index/art/leaf.hpp"


Leaf::Leaf(uint64_t value, row_t row_id, uint8_t max_prefix_length) : Node(NodeType::NLeaf, max_prefix_length) {
	this->value = value;
	this->capacity = 1;
	this->row_id = unique_ptr<row_t[]>(new row_t[this->capacity]);
	this->row_id[0] = row_id;
	this->num_elements = 1;
}

void Leaf::insert(Leaf *leaf, row_t row_id) {
	// Grow array
	if (leaf->num_elements == leaf->capacity) {
		auto new_row_id = unique_ptr<row_t[]>(new row_t[leaf->capacity * 2]);
		memcpy(new_row_id.get(), leaf->row_id.get(), leaf->capacity * sizeof(row_t));
		leaf->capacity *= 2;
		leaf->row_id = move(new_row_id);
	}
	leaf->row_id[leaf->num_elements] = row_id;
	leaf->num_elements++;
}

//! TODO: Maybe shrink array dynamically?
void Leaf::remove(Leaf *leaf, row_t row_id) {
	index_t entry_offset = -1;
	for (index_t i = 0; i < leaf->num_elements; i++) {
		if (leaf->row_id[i] == row_id) {
			entry_offset = i;
			break;
		}
	}
	leaf->num_elements--;
	for (index_t j = entry_offset; j < leaf->num_elements; j++)
		leaf->row_id[j] = leaf->row_id[j + 1];
}
