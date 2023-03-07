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
		n4->key[i] = 0;
		n4->children[i] = ARTNode();
	}
	return n4;
}

void Node4::Vacuum(ART &art, const unordered_set<ARTNodeType, ARTNodeTypeHash> &vacuum_nodes) {

	for (idx_t i = 0; i < count; i++) {
		ARTNode::Vacuum(art, children[i], vacuum_nodes);
	}
}

void Node4::InitializeMerge(ART &art, unordered_map<ARTNodeType, idx_t, ARTNodeTypeHash> &buffer_counts) {

	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, buffer_counts);
	}
}

void Node4::InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n4 = node.Get<Node4>(art);

	// insert new child node into node
	if (n4->count < ARTNode::NODE_4_CAPACITY) {
		// still space, just insert the child
		idx_t pos = 0;
		while ((pos < n4->count) && (n4->key[pos] < byte)) {
			pos++;
		}
		if (n4->children[pos]) {
			for (idx_t i = n4->count; i > pos; i--) {
				n4->key[i] = n4->key[i - 1];
				n4->children[i] = n4->children[i - 1];
			}
		}
		n4->key[pos] = byte;
		n4->children[pos] = child;
		n4->count++;

	} else {
		// node is full, grow to Node16
		auto new_n16_node = ARTNode::New(art, ARTNodeType::NODE_16);
		auto new_n16 = Node16::Initialize(art, new_n16_node);

		new_n16->count = n4->count;
		new_n16->prefix.Move(n4->prefix);

		for (idx_t i = 0; i < n4->count; i++) {
			new_n16->key[i] = n4->key[i];
			new_n16->children[i] = n4->children[i];
		}

		n4->count = 0;
		ARTNode::Free(art, node);
		node = new_n16_node;
		Node16::InsertChild(art, node, byte, child);
	}
}

void Node4::DeleteChild(ART &art, ARTNode &node, idx_t pos) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n4 = node.Get<Node4>(art);

	D_ASSERT(pos < n4->count);
	D_ASSERT(n4->count > 1);

	// free the child and decrease the count
	ARTNode::Free(art, n4->children[pos]);
	n4->count--;
	D_ASSERT(n4->count >= 1);

	// potentially move any children backwards
	for (; pos < n4->count; pos++) {
		n4->key[pos] = n4->key[pos + 1];
		n4->children[pos] = n4->children[pos + 1];
	}
	// set any remaining nodes as nullptr
	for (; pos < ARTNode::NODE_4_CAPACITY; pos++) {
		n4->children[pos] = ARTNode();
	}

	// this is a one way node, compress
	if (n4->count == 1) {

		// get only child and concatenate prefixes
		auto child = *n4->GetChild(0);
		child.GetPrefix(art)->Concatenate(art, n4->key[0], *node.GetPrefix(art));
		n4->count--;

		ARTNode::Free(art, node);
		node = child;
	}
}

void Node4::ReplaceChild(const idx_t &pos, ARTNode &child) {
	D_ASSERT(pos < ARTNode::NODE_4_CAPACITY);
	children[pos] = child;
}

ARTNode *Node4::GetChild(const idx_t &pos) {
	D_ASSERT(pos < count);
	return &children[pos];
}

uint8_t Node4::GetKeyByte(const idx_t &pos) const {
	D_ASSERT(pos < count);
	return key[pos];
}

idx_t Node4::GetChildPos(const uint8_t &byte) const {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == byte) {
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node4::GetChildPosGreaterEqual(const uint8_t &byte, bool &inclusive) const {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] >= byte) {
			inclusive = false;
			if (key[pos] == byte) {
				inclusive = true;
			}
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node4::GetMinPos() const {
	return 0;
}

idx_t Node4::GetNextPos(idx_t pos) const {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

idx_t Node4::GetNextPosAndByte(idx_t pos, uint8_t &byte) const {
	if (pos == DConstants::INVALID_INDEX) {
		byte = key[0];
		return 0;
	}
	pos++;
	if (pos < count) {
		byte = key[pos];
		return pos;
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
	writer.Write<uint16_t>(count);
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

	count = reader.Read<uint16_t>();
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
