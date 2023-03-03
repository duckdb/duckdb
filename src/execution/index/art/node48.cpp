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
		n48->children[i] = ARTNode();
	}
	return n48;
}

void Node48::Vacuum(ART &art, const unordered_set<ARTNodeType, ARTNodeTypeHash> &vacuum_nodes) {

	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (child_index[i] != ARTNode::EMPTY_MARKER) {
			ARTNode::Vacuum(art, children[child_index[i]], vacuum_nodes);
		}
	}
}

void Node48::InitializeMerge(ART &art, unordered_map<ARTNodeType, idx_t, ARTNodeTypeHash> &buffer_counts) {

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

	// insert new child node into node
	if (n48->count < ARTNode::NODE_48_CAPACITY) {
		// still space, just insert the child
		idx_t pos = n48->count;
		if (n48->children[pos]) {
			// find an empty position in the node list if the current position is occupied
			pos = 0;
			while (n48->children[pos]) {
				pos++;
			}
		}
		n48->children[pos] = child;
		n48->child_index[byte] = pos;
		n48->count++;

	} else {
		// node is full, grow to Node256
		auto new_n256_node = ARTNode::New(art, ARTNodeType::NODE_256);
		auto new_n256 = Node256::Initialize(art, new_n256_node);

		new_n256->count = n48->count;
		new_n256->prefix.Move(n48->prefix);

		for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
			if (n48->child_index[i] != ARTNode::EMPTY_MARKER) {
				new_n256->children[i] = n48->children[n48->child_index[i]];
				n48->child_index[i] = ARTNode::EMPTY_MARKER;
			}
		}

		n48->count = 0;
		ARTNode::Free(art, node);
		node = new_n256_node;
		Node256::InsertChild(art, node, byte, child);
	}
}

void Node48::DeleteChild(ART &art, ARTNode &node, idx_t pos) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n48 = node.Get<Node48>(art);

	// free the child and decrease the count
	ARTNode::Free(art, n48->children[n48->child_index[pos]]);
	n48->child_index[pos] = ARTNode::EMPTY_MARKER;
	n48->count--;

	// shrink node to Node16
	if (n48->count < ARTNode::NODE_48_SHRINK_THRESHOLD) {

		auto new_n16_node = ARTNode::New(art, ARTNodeType::NODE_16);
		auto new_n16 = Node16::Initialize(art, new_n16_node);

		new_n16->prefix.Move(n48->prefix);

		for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
			if (n48->child_index[i] != ARTNode::EMPTY_MARKER) {
				new_n16->key[new_n16->count] = i;
				new_n16->children[new_n16->count++] = n48->children[n48->child_index[i]];
				n48->child_index[i] = ARTNode::EMPTY_MARKER;
			}
		}

		n48->count = 0;
		ARTNode::Free(art, node);
		node = new_n16_node;
	}
}

void Node48::ReplaceChild(const idx_t &pos, ARTNode &child) {
	D_ASSERT(pos < ARTNode::NODE_256_CAPACITY);
	D_ASSERT(child_index[pos] < ARTNode::NODE_48_CAPACITY);
	children[child_index[pos]] = child;
}

ARTNode *Node48::GetChild(const idx_t &pos) {
	D_ASSERT(pos < ARTNode::NODE_256_CAPACITY);
	D_ASSERT(child_index[pos] < ARTNode::NODE_48_CAPACITY);
	return &children[child_index[pos]];
}

uint8_t Node48::GetKeyByte(const idx_t &pos) const {
	D_ASSERT(pos < ARTNode::NODE_256_CAPACITY);
	return pos;
}

idx_t Node48::GetChildPos(const uint8_t &byte) const {
	if (child_index[byte] == ARTNode::EMPTY_MARKER) {
		return DConstants::INVALID_INDEX;
	}
	return byte;
}

idx_t Node48::GetChildPosGreaterEqual(const uint8_t &byte, bool &inclusive) const {
	for (idx_t pos = byte; pos < ARTNode::NODE_256_CAPACITY; pos++) {
		if (child_index[pos] != ARTNode::EMPTY_MARKER) {
			inclusive = false;
			if (pos == byte) {
				inclusive = true;
			}
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetMinPos() const {
	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (child_index[i] != ARTNode::EMPTY_MARKER) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetNextPos(idx_t pos) const {
	pos == DConstants::INVALID_INDEX ? pos = 0 : pos++;
	for (; pos < ARTNode::NODE_256_CAPACITY; pos++) {
		if (child_index[pos] != ARTNode::EMPTY_MARKER) {
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetNextPosAndByte(idx_t pos, uint8_t &byte) const {
	pos == DConstants::INVALID_INDEX ? pos = 0 : pos++;
	for (; pos < ARTNode::NODE_256_CAPACITY; pos++) {
		if (child_index[pos] != ARTNode::EMPTY_MARKER) {
			byte = uint8_t(pos);
			return pos;
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
	writer.Write<uint16_t>(count);
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

	count = reader.Read<uint16_t>();
	prefix.Deserialize(art, reader);

	// read key values
	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		child_index[i] = reader.Read<uint8_t>();
	}

	// read child block pointers
	for (idx_t i = 0; i < ARTNode::NODE_48_CAPACITY; i++) {
		children[i] = ARTNode(reader);
	}

	art.IncreaseMemorySize(MemorySize());
}

idx_t Node48::MemorySize() {
#ifdef DEBUG
	return prefix.MemorySize() + sizeof(*this);
#endif
}

bool Node48::ChildIsInMemory(const idx_t &pos) {
#ifdef DEBUG
	D_ASSERT(pos < ARTNode::NODE_256_CAPACITY);
	D_ASSERT(child_index[pos] < ARTNode::NODE_48_CAPACITY);
	return children[child_index[pos]].InMemory();
#endif
}

} // namespace duckdb
