#include "duckdb/execution/index/art/node16.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

void Node16::Free(ART &art, ARTNode &node) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());

	auto n16 = node.Get<Node16>(art);

	// free all children
	if (n16->count) {
		for (idx_t i = 0; i < n16->count; i++) {
			ARTNode::Free(art, n16->children[i]);
		}
	}

	art.DecreaseMemorySize(sizeof(Node16));
}

Node16 *Node16::Initialize(ART &art, const ARTNode &node) {

	auto n16 = node.Get<Node16>(art);
	art.IncreaseMemorySize(sizeof(Node16));

	n16->count = 0;
	n16->prefix.Initialize();

	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		n16->children[i].Reset();
	}

	return n16;
}

Node16 *Node16::GrowNode4(ART &art, ARTNode &node16, ARTNode &node4) {

	ARTNode::New(art, node16, ARTNodeType::NODE_16);

	auto n4 = node4.Get<Node4>(art);
	auto n16 = node16.Get<Node16>(art);

	n16->count = n4->count;
	n16->prefix.Move(n4->prefix);

	for (idx_t i = 0; i < n4->count; i++) {
		n16->key[i] = n4->key[i];
		n16->children[i] = n4->children[i];
	}

	for (idx_t i = n4->count; i < ARTNode::NODE_16_CAPACITY; i++) {
		n16->children[i].Reset();
	}

	n4->count = 0;
	ARTNode::Free(art, node4);
	return n16;
}

Node16 *Node16::ShrinkNode48(ART &art, ARTNode &node16, ARTNode &node48) {

	ARTNode::New(art, node16, ARTNodeType::NODE_16);

	auto n16 = node16.Get<Node16>(art);
	auto n48 = node48.Get<Node48>(art);

	n16->count = 0;
	n16->prefix.Move(n48->prefix);

	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (n48->child_index[i] != ARTNode::EMPTY_MARKER) {
			n16->key[n16->count] = i;
			n16->children[n16->count] = n48->children[n48->child_index[i]];
			n16->count++;
		}
	}

	for (idx_t i = n16->count; i < ARTNode::NODE_16_CAPACITY; i++) {
		n16->children[i].Reset();
	}

	n48->count = 0;
	ARTNode::Free(art, node48);
	return n16;
}

void Node16::InitializeMerge(ART &art, const vector<idx_t> &buffer_counts) {

	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, buffer_counts);
	}
}

void Node16::InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n16 = node.Get<Node16>(art);

#ifdef DEBUG
	// ensure that there is no other child at the same byte
	for (idx_t i = 0; i < n16->count; i++) {
		D_ASSERT(n16->key[i] != byte);
	}
#endif

	// insert new child node into node
	if (n16->count < ARTNode::NODE_16_CAPACITY) {
		// still space, just insert the child
		idx_t position = 0;
		while (position < n16->count && n16->key[position] < byte) {
			position++;
		}
		if (n16->children[position]) {
			for (idx_t i = n16->count; i > position; i--) {
				n16->key[i] = n16->key[i - 1];
				n16->children[i] = n16->children[i - 1];
			}
		}
		n16->key[position] = byte;
		n16->children[position] = child;
		n16->count++;

	} else {
		// node is full, grow to Node48
		auto node16 = node;
		Node48::GrowNode16(art, node, node16);
		Node48::InsertChild(art, node, byte, child);
	}
}

void Node16::DeleteChild(ART &art, ARTNode &node, idx_t position) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n16 = node.Get<Node16>(art);

	D_ASSERT(position < n16->count);

	// free the child and decrease the count
	ARTNode::Free(art, n16->children[position]);
	n16->count--;

	// potentially move any children backwards
	for (; position < n16->count; position++) {
		n16->key[position] = n16->key[position + 1];
		n16->children[position] = n16->children[position + 1];
	}

	// shrink node to Node4
	if (n16->count < ARTNode::NODE_4_CAPACITY) {
		auto node16 = node;
		Node4::ShrinkNode16(art, node, node16);
	}
}

void Node16::ReplaceChild(const idx_t &position, ARTNode &child) {
	D_ASSERT(position < ARTNode::NODE_16_CAPACITY);
	children[position] = child;
}

ARTNode *Node16::GetChild(const idx_t &position) {
	D_ASSERT(position < count);
	return &children[position];
}

uint8_t Node16::GetKeyByte(const idx_t &position) const {
	D_ASSERT(position < count);
	return key[position];
}

idx_t Node16::GetChildPosition(const uint8_t &byte) const {
	for (idx_t position = 0; position < count; position++) {
		if (key[position] == byte) {
			return position;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node16::GetChildPositionGreaterEqual(const uint8_t &byte, bool &inclusive) const {
	for (idx_t position = 0; position < count; position++) {
		if (key[position] >= byte) {
			inclusive = false;
			if (key[position] == byte) {
				inclusive = true;
			}
			return position;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node16::GetMinPosition() const {
	return 0;
}

idx_t Node16::GetNextPosition(idx_t position) const {
	if (position == DConstants::INVALID_INDEX) {
		return 0;
	}
	position++;
	return position < count ? position : DConstants::INVALID_INDEX;
}

idx_t Node16::GetNextPositionAndByte(idx_t position, uint8_t &byte) const {
	if (position == DConstants::INVALID_INDEX) {
		byte = key[0];
		return 0;
	}
	position++;
	if (position < count) {
		byte = key[position];
		return position;
	}
	return DConstants::INVALID_INDEX;
}

BlockPointer Node16::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into children and retrieve child block pointers
	vector<BlockPointer> child_block_pointers;
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		child_block_pointers.push_back(children[i].Serialize(art, writer));
	}

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(ARTNodeType::NODE_16);
	writer.Write<uint8_t>(count);
	prefix.Serialize(art, writer);

	// write key values
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		writer.Write(key[i]);
	}

	// write child block pointers
	for (auto &child_block_pointer : child_block_pointers) {
		writer.Write(child_block_pointer.block_id);
		writer.Write(child_block_pointer.offset);
	}

	return block_pointer;
}

void Node16::Deserialize(ART &art, MetaBlockReader &reader) {

	count = reader.Read<uint8_t>();
	prefix.Deserialize(art, reader);

	// read key values
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		key[i] = reader.Read<uint8_t>();
	}

	// read child block pointers
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		children[i] = ARTNode(reader);
	}

	art.IncreaseMemorySize(sizeof(Node16));
}

} // namespace duckdb
