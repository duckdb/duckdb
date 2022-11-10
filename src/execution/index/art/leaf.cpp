#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include <cstring>

namespace duckdb {
idx_t Leaf::GetCapacity() const {
	return IsInlined() ? 1 : rowids.ptr[0];
}

bool Leaf::IsInlined() const {
	return count <= 1;
}

row_t Leaf::GetRowId(idx_t index) {
	D_ASSERT(index < count);
	if (IsInlined()) {
		return rowids.inlined;
	} else {
		D_ASSERT(rowids.ptr[0] >= count);
		return rowids.ptr[index + 1];
	}
}

row_t *Leaf::GetRowIds() {
	if (IsInlined()) {
		return &rowids.inlined;
	} else {
		return rowids.ptr + 1;
	}
}

Leaf::Leaf(Key &value, uint32_t depth, row_t row_id) : Node(NodeType::NLeaf) {
	count = 1;
	rowids.inlined = row_id;
	D_ASSERT(value.len >= depth);
	prefix = Prefix(value, depth, value.len - depth);
}

Leaf::Leaf(Key &value, uint32_t depth, row_t *row_ids_p, idx_t num_elements_p) : Node(NodeType::NLeaf) {
	D_ASSERT(num_elements_p >= 1);
	if (num_elements_p == 1) {
		// we can inline the row ids
		rowids.inlined = row_ids_p[0];
	} else {
		// new row ids of this leaf
		count = 0;
		Resize(row_ids_p, num_elements_p, num_elements_p);
	}
	count = num_elements_p;
	D_ASSERT(value.len >= depth);
	prefix = Prefix(value, depth, value.len - depth);
}

Leaf::Leaf(unique_ptr<row_t[]> row_ids_p, idx_t num_elements_p, Prefix &prefix_p) : Node(NodeType::NLeaf) {
	D_ASSERT(num_elements_p > 1);
	D_ASSERT(row_ids_p[0] == row_t(num_elements_p)); // first element should contain capacity
	rowids.ptr = row_ids_p.release();
	count = num_elements_p;
	prefix = prefix_p;
}

Leaf::Leaf(row_t row_id, Prefix &prefix_p) : Node(NodeType::NLeaf) {
	rowids.inlined = row_id;
	count = 1;
	prefix = prefix_p;
}

Leaf::~Leaf() {
	if (!IsInlined()) {
		delete[] rowids.ptr;
		count = 0;
	}
}

row_t *Leaf::Resize(row_t *current_row_ids, uint32_t current_count, idx_t new_capacity) {
	D_ASSERT(new_capacity >= current_count);
	auto new_allocation = unique_ptr<row_t[]>(new row_t[new_capacity + 1]);
	new_allocation[0] = new_capacity;
	auto new_row_ids = new_allocation.get() + 1;
	memcpy(new_row_ids, current_row_ids, current_count * sizeof(row_t));
	if (!IsInlined()) {
		// delete the old data
		delete[] rowids.ptr;
	}
	// set up the new pointers
	rowids.ptr = new_allocation.release();
	return new_row_ids;
}

void Leaf::Insert(row_t row_id) {
	auto capacity = GetCapacity();
	row_t *row_ids = GetRowIds();
	D_ASSERT(count <= capacity);
	if (count == capacity) {
		// Grow array
		row_ids = Resize(row_ids, count, capacity * 2);
	}
	row_ids[count++] = row_id;
}

void Leaf::Remove(row_t row_id) {
	idx_t entry_offset = DConstants::INVALID_INDEX;
	row_t *row_ids = GetRowIds();
	for (idx_t i = 0; i < count; i++) {
		if (row_ids[i] == row_id) {
			entry_offset = i;
			break;
		}
	}
	if (entry_offset == DConstants::INVALID_INDEX) {
		return;
	}
	if (IsInlined()) {
		D_ASSERT(count == 1);
		count--;
		return;
	}
	count--;
	if (count == 1) {
		// after erasing we can now inline the leaf
		// delete the pointer and inline the remaining rowid
		auto remaining_row_id = row_ids[0] == row_id ? row_ids[1] : row_ids[0];
		delete[] rowids.ptr;
		rowids.inlined = remaining_row_id;
		return;
	}
	auto capacity = GetCapacity();
	if (capacity > 2 && count < capacity / 2) {
		// Shrink array, if less than half full
		auto new_capacity = capacity / 2;
		auto new_allocation = unique_ptr<row_t[]>(new row_t[new_capacity + 1]);
		new_allocation[0] = new_capacity;
		auto new_row_ids = new_allocation.get() + 1;
		memcpy(new_row_ids, row_ids, entry_offset * sizeof(row_t));
		memcpy(new_row_ids + entry_offset, row_ids + entry_offset + 1, (count - entry_offset) * sizeof(row_t));
		rowids.ptr = new_allocation.release();
	} else {
		// Copy the rest
		memmove(row_ids + entry_offset, row_ids + entry_offset + 1, (count - entry_offset) * sizeof(row_t));
	}
}

string Leaf::ToString(Node *node) {
	Leaf *leaf = (Leaf *)node;
	string str = "Leaf: [";
	auto row_ids = leaf->GetRowIds();
	for (idx_t i = 0; i < leaf->count; i++) {
		str += i == 0 ? to_string(row_ids[i]) : ", " + to_string(row_ids[i]);
	}
	return str + "]";
}

void Leaf::Merge(Node *&l_node, Node *&r_node) {
	Leaf *l_n = (Leaf *)l_node;
	Leaf *r_n = (Leaf *)r_node;

	// append non-duplicate row_ids to l_n
	for (idx_t i = 0; i < r_n->count; i++) {
		l_n->Insert(r_n->GetRowId(i));
	}
}

BlockPointer Leaf::Serialize(duckdb::MetaBlockWriter &writer) {
	auto ptr = writer.GetBlockPointer();
	// Write Node Type
	writer.Write(type);
	// Write compression Info
	prefix.Serialize(writer);
	// Write Row Ids
	// Length
	writer.Write<uint16_t>(count);
	// Actual Row Ids
	auto row_ids = GetRowIds();
	for (idx_t i = 0; i < count; i++) {
		writer.Write(row_ids[i]);
	}
	return ptr;
}

Leaf *Leaf::Deserialize(MetaBlockReader &reader) {
	Prefix prefix;
	prefix.Deserialize(reader);
	auto num_elements = reader.Read<uint16_t>();
	if (num_elements == 1) {
		// inlined
		auto element = reader.Read<row_t>();
		return new Leaf(element, prefix);
	} else {
		// non-inlined
		auto elements = unique_ptr<row_t[]>(new row_t[num_elements + 1]);
		elements[0] = num_elements;
		for (idx_t i = 0; i < num_elements; i++) {
			elements[i + 1] = reader.Read<row_t>();
		}
		return new Leaf(move(elements), num_elements, prefix);
	}
}

} // namespace duckdb
