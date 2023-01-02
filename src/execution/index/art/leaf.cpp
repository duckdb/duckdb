#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/art.hpp"
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

Leaf::Leaf() : Node(NodeType::NLeaf) {
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

Leaf::Leaf(row_t *row_ids_p, idx_t num_elements_p, Prefix &prefix_p) : Node(NodeType::NLeaf) {
	D_ASSERT(num_elements_p > 1);
	D_ASSERT(row_ids_p[0] == row_t(num_elements_p)); // first element should contain capacity
	rowids.ptr = row_ids_p;
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
		DeleteArray<row_t>(rowids.ptr, rowids.ptr[0] + 1);
		count = 0;
	}
}

idx_t Leaf::MemorySize(ART &, const bool &) {
	if (IsInlined()) {
		return prefix.MemorySize() + sizeof(*this) + sizeof(row_t);
	}
	return prefix.MemorySize() + sizeof(*this) + sizeof(row_t) * (GetCapacity() + 1);
}

row_t *Leaf::Resize(row_t *current_row_ids, uint32_t current_count, idx_t new_capacity) {
	D_ASSERT(new_capacity >= current_count);
	auto new_allocation = AllocateArray<row_t>(new_capacity + 1);
	new_allocation[0] = new_capacity;
	auto new_row_ids = new_allocation + 1;
	memcpy(new_row_ids, current_row_ids, current_count * sizeof(row_t));
	if (!IsInlined()) {
		// delete the old data
		DeleteArray<row_t>(rowids.ptr, rowids.ptr[0] + 1);
	}
	// set up the new pointers
	rowids.ptr = new_allocation;
	return new_row_ids;
}

void Leaf::Insert(ART &art, row_t row_id) {
	auto capacity = GetCapacity();
	row_t *row_ids = GetRowIds();
	D_ASSERT(count <= capacity);

	if (count == capacity) {
		// grow array
		art.memory_size += capacity * sizeof(row_t);
		row_ids = Resize(row_ids, count, capacity * 2);
	}
	// insert new row ID
	row_ids[count++] = row_id;
}

void Leaf::Remove(ART &art, row_t row_id) {
	idx_t entry_offset = DConstants::INVALID_INDEX;
	row_t *row_ids = GetRowIds();

	// find the row ID in the leaf
	for (idx_t i = 0; i < count; i++) {
		if (row_ids[i] == row_id) {
			entry_offset = i;
			break;
		}
	}

	// didn't find the row ID
	if (entry_offset == DConstants::INVALID_INDEX) {
		return;
	}

	// now empty leaf
	if (IsInlined()) {
		D_ASSERT(count == 1);
		count--;
		art.memory_size -= sizeof(row_t);
		return;
	}

	count--;
	if (count == 1) {
		// after erasing we can now inline the leaf
		// delete the pointer and inline the remaining rowid
		auto remaining_row_id = row_ids[0] == row_id ? row_ids[1] : row_ids[0];
		DeleteArray<row_t>(rowids.ptr, rowids.ptr[0] + 1);
		rowids.inlined = remaining_row_id;
		art.memory_size -= sizeof(row_t) * 2;
		return;
	}

	// shrink array, if less than half full
	auto capacity = GetCapacity();
	if (capacity > 2 && count < capacity / 2) {

		art.memory_size -= capacity * sizeof(row_t);
		auto new_capacity = capacity / 2;
		auto new_allocation = AllocateArray<row_t>(new_capacity + 1);
		new_allocation[0] = new_capacity;

		auto new_row_ids = new_allocation + 1;
		memcpy(new_row_ids, row_ids, entry_offset * sizeof(row_t));
		memcpy(new_row_ids + entry_offset, row_ids + entry_offset + 1, (count - entry_offset) * sizeof(row_t));

		DeleteArray<row_t>(rowids.ptr, rowids.ptr[0] + 1);
		rowids.ptr = new_allocation;

	} else {
		// move the trailing row IDs (after entry_offset)
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

void Leaf::Merge(ART &art, Node *&l_node, Node *&r_node) {
	Leaf *l_n = (Leaf *)l_node;
	Leaf *r_n = (Leaf *)r_node;

	auto l_capacity = l_n->GetCapacity();
	auto l_row_ids = l_n->GetRowIds();
	auto r_row_ids = r_n->GetRowIds();

	if (l_n->count + r_n->count > l_capacity) {
		auto capacity = l_n->GetCapacity();
		auto new_capacity = NextPowerOfTwo(l_n->count + r_n->count);
		art.memory_size += sizeof(row_t) * (new_capacity - capacity);
		l_row_ids = l_n->Resize(l_row_ids, l_n->count, new_capacity);
	}

	// append row_ids to l_n
	memcpy(l_row_ids + l_n->count, r_row_ids, r_n->count * sizeof(row_t));
	l_n->count += r_n->count;
}

BlockPointer Leaf::Serialize(duckdb::MetaBlockWriter &writer) {

	auto ptr = writer.GetBlockPointer();
	writer.Write(type);
	prefix.Serialize(writer);
	writer.Write<uint16_t>(count);

	auto row_ids = GetRowIds();
	for (idx_t i = 0; i < count; i++) {
		writer.Write(row_ids[i]);
	}
	return ptr;
}

void Leaf::Deserialize(ART &art, MetaBlockReader &reader) {

	prefix.Deserialize(reader);
	count = reader.Read<uint16_t>();
	if (count == 1) {
		// inlined
		auto row_id = reader.Read<row_t>();
		rowids.inlined = row_id;

	} else {
		// non-inlined
		auto row_ids = AllocateArray<row_t>(count + 1);
		row_ids[0] = count;
		for (idx_t i = 0; i < count; i++) {
			row_ids[i + 1] = reader.Read<row_t>();
		}
		Resize(row_ids, count, count);
	}
}

} // namespace duckdb
