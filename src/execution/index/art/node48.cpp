#include "duckdb/execution/index/art/node48.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

void Node48::Free(ART &art, ARTNode &node) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());

	auto n48 = node.Get<Node48>(art);

	// free all children
	if (n48->count) {
		for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
			if (n48->child_index[i] != ARTNode::EMPTY_MARKER) {
				ARTNode::Free(art, n48->children[n48->child_index[i]]);
			}
		}
	}

	art.DecreaseMemorySize(sizeof(Node48));
}

Node48 *Node48::Initialize(ART &art, const ARTNode &node) {

	auto n48 = node.Get<Node48>(art);
	art.IncreaseMemorySize(sizeof(Node48));

	n48->count = 0;
	n48->prefix.Initialize();

	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		n48->child_index[i] = ARTNode::EMPTY_MARKER;
	}

	for (idx_t i = 0; i < ARTNode::NODE_48_CAPACITY; i++) {
		n48->children[i].Reset();
	}

	return n48;
}

Node48 *Node48::GrowNode16(ART &art, ARTNode &node48, ARTNode &node16) {

	ARTNode::New(art, node48, ARTNodeType::NODE_48);

	auto n16 = node16.Get<Node16>(art);
	auto n48 = node48.Get<Node48>(art);

	n48->count = n16->count;
	n48->prefix.Move(n16->prefix);

	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		n48->child_index[i] = ARTNode::EMPTY_MARKER;
	}

	for (idx_t i = 0; i < n16->count; i++) {
		n48->child_index[n16->key[i]] = i;
		n48->children[i] = n16->children[i];
	}

	for (idx_t i = n16->count; i < ARTNode::NODE_48_CAPACITY; i++) {
		n48->children[i].Reset();
	}

	n16->count = 0;
	ARTNode::Free(art, node16);
	return n48;
}

Node48 *Node48::ShrinkNode256(ART &art, ARTNode &node48, ARTNode &node256) {

	ARTNode::New(art, node48, ARTNodeType::NODE_48);

	auto n48 = node48.Get<Node48>(art);
	auto n256 = node256.Get<Node256>(art);

	n48->count = 0;
	n48->prefix.Move(n256->prefix);

	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (n256->children[i]) {
			n48->child_index[i] = n48->count;
			n48->children[n48->count] = n256->children[i];
			n48->count++;
		} else {
			n48->child_index[i] = ARTNode::EMPTY_MARKER;
		}
	}

	for (idx_t i = n48->count; i < ARTNode::NODE_48_CAPACITY; i++) {
		n48->children[i].Reset();
	}

	n256->count = 0;
	ARTNode::Free(art, node256);
	return n48;
}

void Node48::InitializeMerge(ART &art, const vector<idx_t> &buffer_counts) {

	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (child_index[i] != ARTNode::EMPTY_MARKER) {
			children[child_index[i]].InitializeMerge(art, buffer_counts);
		}
	}
}

void Node48::InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n48 = node.Get<Node48>(art);

#ifdef DEBUG
	// ensure that there is no other child at the same byte
	D_ASSERT(n48->child_index[byte] == ARTNode::EMPTY_MARKER);
#endif

	// insert new child node into node
	if (n48->count < ARTNode::NODE_48_CAPACITY) {
		// still space, just insert the child
		idx_t position = n48->count;
		if (n48->children[position]) {
			// find an empty position in the node list if the current position is occupied
			position = 0;
			while (n48->children[position]) {
				position++;
			}
		}
		n48->children[position] = child;
		n48->child_index[byte] = position;
		n48->count++;

	} else {
		// node is full, grow to Node256
		auto node48 = node;
		Node256::GrowNode48(art, node, node48);
		Node256::InsertChild(art, node, byte, child);
	}
}

void Node48::DeleteChild(ART &art, ARTNode &node, idx_t position) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n48 = node.Get<Node48>(art);

	// free the child and decrease the count
	ARTNode::Free(art, n48->children[n48->child_index[position]]);
	n48->child_index[position] = ARTNode::EMPTY_MARKER;
	n48->count--;

	// shrink node to Node16
	if (n48->count < ARTNode::NODE_48_SHRINK_THRESHOLD) {
		auto node48 = node;
		Node16::ShrinkNode48(art, node, node48);
	}
}

void Node48::ReplaceChild(const idx_t &position, ARTNode &child) {
	D_ASSERT(position < ARTNode::NODE_256_CAPACITY);
	D_ASSERT(child_index[position] < ARTNode::NODE_48_CAPACITY);
	children[child_index[position]] = child;
}

ARTNode *Node48::GetChild(const idx_t &position) {
	D_ASSERT(position < ARTNode::NODE_256_CAPACITY);
	D_ASSERT(child_index[position] < ARTNode::NODE_48_CAPACITY);
	return &children[child_index[position]];
}

uint8_t Node48::GetKeyByte(const idx_t &position) const {
	D_ASSERT(position < ARTNode::NODE_256_CAPACITY);
	return position;
}

idx_t Node48::GetChildPosition(const uint8_t &byte) const {
	if (child_index[byte] == ARTNode::EMPTY_MARKER) {
		return DConstants::INVALID_INDEX;
	}
	return byte;
}

idx_t Node48::GetChildPositionGreaterEqual(const uint8_t &byte, bool &inclusive) const {
	for (idx_t position = byte; position < ARTNode::NODE_256_CAPACITY; position++) {
		if (child_index[position] != ARTNode::EMPTY_MARKER) {
			inclusive = false;
			if (position == byte) {
				inclusive = true;
			}
			return position;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetMinPosition() const {
	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (child_index[i] != ARTNode::EMPTY_MARKER) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetNextPosition(idx_t position) const {
	position == DConstants::INVALID_INDEX ? position = 0 : position++;
	for (; position < ARTNode::NODE_256_CAPACITY; position++) {
		if (child_index[position] != ARTNode::EMPTY_MARKER) {
			return position;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetNextPositionAndByte(idx_t position, uint8_t &byte) const {
	position == DConstants::INVALID_INDEX ? position = 0 : position++;
	for (; position < ARTNode::NODE_256_CAPACITY; position++) {
		if (child_index[position] != ARTNode::EMPTY_MARKER) {
			byte = uint8_t(position);
			return position;
		}
	}
	return DConstants::INVALID_INDEX;
}

BlockPointer Node48::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into children and retrieve child block pointers
	vector<BlockPointer> child_block_pointers;
	for (idx_t i = 0; i < ARTNode::NODE_48_CAPACITY; i++) {
		child_block_pointers.push_back(children[i].Serialize(art, writer));
	}

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(ARTNodeType::NODE_48);
	writer.Write<uint8_t>(count);
	prefix.Serialize(art, writer);

	// write key values
	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		writer.Write(child_index[i]);
	}

	// write child block pointers
	for (auto &child_block_pointer : child_block_pointers) {
		writer.Write(child_block_pointer.block_id);
		writer.Write(child_block_pointer.offset);
	}

	return block_pointer;
}

void Node48::Deserialize(ART &art, MetaBlockReader &reader) {

	count = reader.Read<uint8_t>();
	prefix.Deserialize(art, reader);

	// read key values
	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		child_index[i] = reader.Read<uint8_t>();
	}

	// read child block pointers
	for (idx_t i = 0; i < ARTNode::NODE_48_CAPACITY; i++) {
		children[i] = ARTNode(reader);
	}

	art.IncreaseMemorySize(sizeof(Node48));
}

} // namespace duckdb
