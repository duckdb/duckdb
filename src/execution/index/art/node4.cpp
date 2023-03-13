#include "duckdb/execution/index/art/node4.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/node16.hpp"

namespace duckdb {

void Node4::Free(ART &art, ARTNode &node) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());

	auto n4 = node.Get<Node4>(art);

	// free all children
	if (n4->count) {
		for (idx_t i = 0; i < n4->count; i++) {
			ARTNode::Free(art, n4->children[i]);
		}
	}

	art.DecreaseMemorySize(sizeof(Node4));
}

Node4 *Node4::Initialize(ART &art, const ARTNode &node) {

	auto n4 = node.Get<Node4>(art);
	art.IncreaseMemorySize(sizeof(Node4));

	n4->count = 0;
	n4->prefix.Initialize();

	for (idx_t i = 0; i < ARTNode::NODE_4_CAPACITY; i++) {
		n4->children[i].Reset();
	}

	return n4;
}

Node4 *Node4::ShrinkNode16(ART &art, ARTNode &node4, ARTNode &node16) {

	ARTNode::New(art, node4, ARTNodeType::NODE_4);

	auto n4 = node4.Get<Node4>(art);
	auto n16 = node16.Get<Node16>(art);

	n4->count = n16->count;
	n4->prefix.Move(n16->prefix);

	for (idx_t i = 0; i < n16->count; i++) {
		n4->key[i] = n16->key[i];
		n4->children[i] = n16->children[i];
	}

	for (idx_t i = n4->count; i < ARTNode::NODE_4_CAPACITY; i++) {
		n4->children[i].Reset();
	}

	n16->count = 0;
	ARTNode::Free(art, node16);
	return n4;
}

void Node4::InitializeMerge(ART &art, const vector<idx_t> &buffer_counts) {

	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, buffer_counts);
	}
}

void Node4::InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n4 = node.Get<Node4>(art);

#ifdef DEBUG
	// ensure that there is no other child at the same byte
	for (idx_t i = 0; i < n4->count; i++) {
		D_ASSERT(n4->key[i] != byte);
	}
#endif

	// insert new child node into node
	if (n4->count < ARTNode::NODE_4_CAPACITY) {
		// still space, just insert the child
		idx_t position = 0;
		while ((position < n4->count) && (n4->key[position] < byte)) {
			position++;
		}
		if (n4->children[position]) {
			for (idx_t i = n4->count; i > position; i--) {
				n4->key[i] = n4->key[i - 1];
				n4->children[i] = n4->children[i - 1];
			}
		}
		n4->key[position] = byte;
		n4->children[position] = child;
		n4->count++;

	} else {
		// node is full, grow to Node16
		auto node4 = node;
		Node16::GrowNode4(art, node, node4);
		Node16::InsertChild(art, node, byte, child);
	}
}

void Node4::DeleteChild(ART &art, ARTNode &node, idx_t position) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n4 = node.Get<Node4>(art);

	D_ASSERT(position < n4->count);
	D_ASSERT(n4->count > 1);

	// free the child and decrease the count
	ARTNode::Free(art, n4->children[position]);
	n4->count--;

	// potentially move any children backwards
	for (; position < n4->count; position++) {
		n4->key[position] = n4->key[position + 1];
		n4->children[position] = n4->children[position + 1];
	}

	// this is a one way node, compress
	if (n4->count == 1) {

		// get only child and concatenate prefixes
		auto child = *n4->GetChild(0);
		child.GetPrefix(art)->Concatenate(art, n4->key[0], n4->prefix);
		n4->count--;

		ARTNode::Free(art, node);
		node = child;
	}
}

void Node4::ReplaceChild(const idx_t &position, ARTNode &child) {
	D_ASSERT(position < ARTNode::NODE_4_CAPACITY);
	children[position] = child;
}

ARTNode *Node4::GetChild(const idx_t &position) {
	D_ASSERT(position < count);
	return &children[position];
}

uint8_t Node4::GetKeyByte(const idx_t &position) const {
	D_ASSERT(position < count);
	return key[position];
}

idx_t Node4::GetChildPosition(const uint8_t &byte) const {
	for (idx_t position = 0; position < count; position++) {
		if (key[position] == byte) {
			return position;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node4::GetChildPositionGreaterEqual(const uint8_t &byte, bool &inclusive) const {
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

idx_t Node4::GetMinPosition() const {
	return 0;
}

idx_t Node4::GetNextPosition(idx_t position) const {
	if (position == DConstants::INVALID_INDEX) {
		return 0;
	}
	position++;
	return position < count ? position : DConstants::INVALID_INDEX;
}

idx_t Node4::GetNextPositionAndByte(idx_t position, uint8_t &byte) const {
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

BlockPointer Node4::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into children and retrieve child block pointers
	vector<BlockPointer> child_block_pointers;
	for (idx_t i = 0; i < ARTNode::NODE_4_CAPACITY; i++) {
		child_block_pointers.push_back(children[i].Serialize(art, writer));
	}

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(ARTNodeType::NODE_4);
	writer.Write<uint8_t>(count);
	prefix.Serialize(art, writer);

	// write key values
	for (idx_t i = 0; i < ARTNode::NODE_4_CAPACITY; i++) {
		writer.Write(key[i]);
	}

	// write child block pointers
	for (auto &child_block_pointer : child_block_pointers) {
		writer.Write(child_block_pointer.block_id);
		writer.Write(child_block_pointer.offset);
	}

	return block_pointer;
}

void Node4::Deserialize(ART &art, MetaBlockReader &reader) {

	count = reader.Read<uint8_t>();
	prefix.Deserialize(art, reader);

	// read key values
	for (idx_t i = 0; i < ARTNode::NODE_4_CAPACITY; i++) {
		key[i] = reader.Read<uint8_t>();
	}

	// read child block pointers
	for (idx_t i = 0; i < ARTNode::NODE_4_CAPACITY; i++) {
		children[i] = ARTNode(reader);
	}

	art.IncreaseMemorySize(sizeof(Node4));
}

} // namespace duckdb
