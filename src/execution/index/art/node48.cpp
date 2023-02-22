#include "duckdb/execution/index/art/node48.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

void Node48::Initialize() {
	count = 0;
	prefix = Prefix();
	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		child_index[i] = ARTNode::EMPTY_MARKER;
	}
	for (idx_t i = 0; i < ARTNode::NODE_48_CAPACITY; i++) {
		children[i] = ARTNode();
	}
}

void Node48::InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n48 = art.n48_nodes.GetDataAtPosition<Node48>(node.GetPointer());

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
		ARTNode new_n256_node(art, ARTNodeType::N256);
		auto new_n256 = art.n256_nodes.GetDataAtPosition<Node48>(new_n256_node.GetPointer());
		new_n256->Initialize();
		art.IncreaseMemorySize(new_n256->MemorySize());

		new_n256->count = n48->count;
		new_n256->prefix = std::move(n48->prefix);

		for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
			if (n48->child_index[i] != ARTNode::EMPTY_MARKER) {
				new_n256->children[i] = n48->children[n48->child_index[i]];
				n48->children[n48->child_index[i]] = ARTNode();
			}
		}

		art.DecreaseMemorySize(n48->MemorySize());
		art.n48_nodes.FreePosition(node.GetPointer());
		node = new_n256_node;
		Node256::InsertChild(art, node, byte, child);
	}
}

void Node48::DeleteChild(ART &art, ARTNode &node, idx_t pos) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n48 = art.n48_nodes.GetDataAtPosition<Node48>(node.GetPointer());

#ifdef DEBUG
	// adjust the ART size
	if (n48->ChildIsInMemory(pos)) {
		auto child = n48->GetChild(pos);
		art.DecreaseMemorySize(child.MemorySize(art, true));
	}
#endif

	// erase the child and decrease the count
	ARTNode::Delete(art, n48->children[n48->child_index[pos]]);
	n48->child_index[pos] = ARTNode::EMPTY_MARKER;
	n48->count--;

	// shrink node to Node16
	if (n48->count < ARTNode::NODE_48_SHRINK_THRESHOLD) {

		ARTNode new_n16_node(art, ARTNodeType::N16);
		auto new_n16 = art.n16_nodes.GetDataAtPosition<Node16>(new_n16_node.GetPointer());
		new_n16->Initialize();
		art.IncreaseMemorySize(new_n16->MemorySize());

		new_n16->prefix = std::move(n48->prefix);

		for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
			if (n48->child_index[i] != ARTNode::EMPTY_MARKER) {
				new_n16->key[new_n16->count] = i;
				new_n16->children[new_n16->count++] = n48->children[n48->child_index[i]];
				n48->children[n48->child_index[i]] = ARTNode();
			}
		}

		art.DecreaseMemorySize(n48->MemorySize());
		art.n48_nodes.FreePosition(node.GetPointer());
		node = new_n16_node;
	}
}

void Node48::Delete(ART &art, ARTNode &node) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());

	auto n48 = art.n48_nodes.GetDataAtPosition<Node48>(node.GetPointer());

	// delete all children
	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (n48->child_index[i] != ARTNode::EMPTY_MARKER) {
			ARTNode::Delete(art, n48->children[n48->child_index[i]]);
		}
	}
	art.n48_nodes.FreePosition(node.GetPointer());
}

void Node48::ReplaceChild(const idx_t &pos, ARTNode &child) {
	D_ASSERT(pos < ARTNode::NODE_256_CAPACITY);
	D_ASSERT(child_index[pos] < ARTNode::NODE_48_CAPACITY);
	children[child_index[pos]] = child;
}

ARTNode Node48::GetChild(const idx_t &pos) {
	D_ASSERT(pos < ARTNode::NODE_256_CAPACITY);
	D_ASSERT(child_index[pos] < ARTNode::NODE_48_CAPACITY);
	return children[child_index[pos]];
}

idx_t Node48::GetChildPos(const uint8_t &byte) {
	if (child_index[byte] == ARTNode::EMPTY_MARKER) {
		return DConstants::INVALID_INDEX;
	}
	return byte;
}

idx_t Node48::GetChildPosGreaterEqual(const uint8_t &byte, bool &inclusive) {
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

idx_t Node48::GetMinPos() {
	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (child_index[i] != ARTNode::EMPTY_MARKER) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetNextPos(idx_t pos) {
	pos == DConstants::INVALID_INDEX ? pos = 0 : pos++;
	for (; pos < ARTNode::NODE_256_CAPACITY; pos++) {
		if (child_index[pos] != ARTNode::EMPTY_MARKER) {
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetNextPosAndByte(idx_t pos, uint8_t &byte) {
	pos == DConstants::INVALID_INDEX ? pos = 0 : pos++;
	for (; pos < ARTNode::NODE_256_CAPACITY; pos++) {
		if (child_index[pos] != ARTNode::EMPTY_MARKER) {
			byte = uint8_t(pos);
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
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
