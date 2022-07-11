#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include <cstring>

namespace duckdb {

Leaf::Leaf(unique_ptr<Key> value, row_t row_id) : Node(NodeType::NLeaf, 0) {
	this->value = move(value);
	this->capacity = 1;
	this->row_ids = unique_ptr<row_t[]>(new row_t[this->capacity]);
	this->row_ids[0] = row_id;
	this->num_elements = 1;
}

Leaf::Leaf(unique_ptr<Key> value, unique_ptr<row_t[]> row_ids, idx_t num_elements) : Node(NodeType::NLeaf, 0) {
	this->value = move(value);
	this->capacity = num_elements;
	this->row_ids = move(row_ids);
	this->num_elements = num_elements;
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

BlockPointer Leaf::SerializeLeaf(duckdb::MetaBlockWriter &writer) {
	auto block_id = writer.block->id;
	uint32_t offset = writer.offset;
	// Write Node Type
	writer.Write(type);
	// Write value
	// Write Value Length
	writer.Write((uint32_t)value->len);
	// Write Value Contents
	for (idx_t i = 0; i < value->len; i++) {
		writer.Write(value->data[i]);
	}
	// Write Row Ids
	// Length
	writer.Write(num_elements);
	// Actual Row Ids
	for (idx_t i = 0; i < num_elements; i++) {
		writer.Write(row_ids[i]);
	}
	return {block_id, offset};
}

Leaf *Leaf::Deserialize(MetaBlockReader &reader, uint32_t value_length) {
	unique_ptr<data_t[]> data = unique_ptr<data_t[]>(new data_t[value_length]);
	for (idx_t i = 0; i < value_length; i++) {
		data[i] = reader.Read<data_t>();
	}

	unique_ptr<Key> key_value = make_unique<Key>(move(data), value_length);

	auto num_elements = reader.Read<idx_t>();
	auto elements = unique_ptr<row_t[]>(new row_t[num_elements]);
	for (idx_t i = 0; i < num_elements; i++) {
		elements[i] = reader.Read<row_t>();
	}
	return new Leaf(move(key_value), move(elements), num_elements);
}

void Leaf::Remove(row_t row_id) {
	idx_t entry_offset = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < num_elements; i++) {
		if (row_ids[i] == row_id) {
			entry_offset = i;
			break;
		}
	}
	if (entry_offset == DConstants::INVALID_INDEX) {
		return;
	}
	num_elements--;
	if (capacity > 2 && num_elements < capacity / 2) {
		// Shrink array, if less than half full
		auto new_row_id = unique_ptr<row_t[]>(new row_t[capacity / 2]);
		memcpy(new_row_id.get(), row_ids.get(), entry_offset * sizeof(row_t));
		memcpy(new_row_id.get() + entry_offset, row_ids.get() + entry_offset + 1,
		       (num_elements - entry_offset) * sizeof(row_t));
		capacity /= 2;
		row_ids = move(new_row_id);
	} else {
		// Copy the rest
		for (idx_t j = entry_offset; j < num_elements; j++) {
			row_ids[j] = row_ids[j + 1];
		}
	}
}

} // namespace duckdb
