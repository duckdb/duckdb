#include "duckdb/execution/index/art/node16.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

void Node16::Free(ART &art, ARTNode &node) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());

	auto n16 = node.Get<Node16>(art.n16_nodes);

	// free all children
	if (n16->count) {
		for (idx_t i = 0; i < n16->count; i++) {
			ARTNode::Free(art, n16->children[i]);
		}
	}

	art.DecreaseMemorySize(sizeof(Node16));
}

Node16 *Node16::Initialize(ART &art, const ARTNode &node) {
	auto n16 = node.Get<Node16>(art.n16_nodes);
	art.IncreaseMemorySize(sizeof(Node16));

	n16->count = 0;
	n16->prefix.Initialize();
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		n16->key[i] = 0;
		n16->children[i] = ARTNode();
	}
	return n16;
}

void Node16::InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n16 = node.Get<Node16>(art.n16_nodes);

	// insert new child node into node
	if (n16->count < ARTNode::NODE_16_CAPACITY) {
		// still space, just insert the child
		idx_t pos = 0;
		while (pos < n16->count && n16->key[pos] < byte) {
			pos++;
		}
		if (n16->children[pos]) {
			for (idx_t i = n16->count; i > pos; i--) {
				n16->key[i] = n16->key[i - 1];
				n16->children[i] = n16->children[i - 1];
			}
		}
		n16->key[pos] = byte;
		n16->children[pos] = child;
		n16->count++;

	} else {
		// node is full, grow to Node48
		auto new_n48_node = ARTNode::New(art, ARTNodeType::N48);
		auto new_n48 = Node48::Initialize(art, new_n48_node);

		new_n48->count = n16->count;
		new_n48->prefix.Move(n16->prefix);

		for (idx_t i = 0; i < n16->count; i++) {
			new_n48->child_index[n16->key[i]] = i;
			new_n48->children[i] = n16->children[i];
		}

		n16->count = 0;
		ARTNode::Free(art, node);
		node = new_n48_node;
		Node48::InsertChild(art, node, byte, child);
	}
}

void Node16::DeleteChild(ART &art, ARTNode &node, idx_t pos) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n16 = node.Get<Node16>(art.n16_nodes);

	D_ASSERT(pos < n16->count);

	// free the child and decrease the count
	ARTNode::Free(art, n16->children[pos]);
	n16->count--;

	// potentially move any children backwards
	for (; pos < n16->count; pos++) {
		n16->key[pos] = n16->key[pos + 1];
		n16->children[pos] = n16->children[pos + 1];
	}
	// set any remaining nodes as nullptr
	for (; pos < ARTNode::NODE_16_CAPACITY; pos++) {
		if (!n16->children[pos]) {
			break;
		}
		n16->children[pos] = ARTNode();
	}

	// shrink node to Node4
	if (n16->count < ARTNode::NODE_4_CAPACITY) {

		auto new_n4_node = ARTNode::New(art, ARTNodeType::N4);
		auto new_n4 = Node4::Initialize(art, new_n4_node);

		new_n4->prefix.Move(n16->prefix);

		for (idx_t i = 0; i < n16->count; i++) {
			new_n4->key[new_n4->count] = n16->key[i];
			new_n4->children[new_n4->count++] = n16->children[i];
		}

		n16->count = 0;
		ARTNode::Free(art, node);
		node = new_n4_node;
	}
}

void Node16::ReplaceChild(const idx_t &pos, ARTNode &child) {
	D_ASSERT(pos < ARTNode::NODE_16_CAPACITY);
	children[pos] = child;
}

ARTNode Node16::GetChild(const idx_t &pos) const {
	D_ASSERT(pos < count);
	return children[pos];
}

uint8_t Node16::GetKeyByte(const idx_t &pos) const {
	D_ASSERT(pos < count);
	return key[pos];
}

idx_t Node16::GetChildPos(const uint8_t &byte) const {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == byte) {
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node16::GetChildPosGreaterEqual(const uint8_t &byte, bool &inclusive) const {
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

idx_t Node16::GetMinPos() const {
	return 0;
}

idx_t Node16::GetNextPos(idx_t pos) const {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

idx_t Node16::GetNextPosAndByte(idx_t pos, uint8_t &byte) const {
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

BlockPointer Node16::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into children and retrieve child block pointers
	vector<BlockPointer> child_block_pointers;
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		child_block_pointers.push_back(children[i].Serialize(art, writer));
	}

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(ARTNodeType::N16);
	writer.Write<uint16_t>(count);
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

	count = reader.Read<uint16_t>();
	prefix.Deserialize(art, reader);

	// read key values
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		key[i] = reader.Read<uint8_t>();
	}

	// read child block pointers
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		children[i] = ARTNode(reader);
	}

	art.IncreaseMemorySize(MemorySize());
}

idx_t Node16::MemorySize() {
#ifdef DEBUG
	return prefix.MemorySize() + sizeof(*this);
#endif
}

bool Node16::ChildIsInMemory(const idx_t &pos) {
#ifdef DEBUG
	D_ASSERT(pos < count);
	return children[pos].InMemory();
#endif
}

} // namespace duckdb
