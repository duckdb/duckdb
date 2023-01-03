#include "duckdb/execution/index/art/node48.hpp"

#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

Node48::Node48() : Node(NodeType::N48) {
	for (idx_t i = 0; i < 256; i++) {
		child_index[i] = Node::EMPTY_MARKER;
	}
}

idx_t Node48::MemorySize(ART &art, const bool &recurse) {
	if (recurse) {
		return prefix.MemorySize() + sizeof(*this) + RecursiveMemorySize(art);
	}
	return prefix.MemorySize() + sizeof(*this);
}

idx_t Node48::GetChildPos(uint8_t k) {
	if (child_index[k] == Node::EMPTY_MARKER) {
		return DConstants::INVALID_INDEX;
	} else {
		return k;
	}
}

idx_t Node48::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			if (pos == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node48::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

idx_t Node48::GetNextPosAndByte(idx_t pos, uint8_t &byte) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			byte = uint8_t(pos);
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

Node *Node48::GetChild(ART &art, idx_t pos) {
	D_ASSERT(child_index[pos] != Node::EMPTY_MARKER);
	return children[child_index[pos]].Unswizzle(art);
}

void Node48::ReplaceChildPointer(idx_t pos, Node *node) {
	children[child_index[pos]] = node;
}

bool Node48::GetARTPointer(idx_t pos) {
	return children[child_index[pos]] && !children[child_index[pos]].IsSwizzled();
}

void Node48::InsertChild(ART &art, Node *&node, uint8_t key_byte, Node *new_child) {
	auto n = (Node48 *)node;

	// insert new child node into node
	if (node->count < Node48::GetSize()) {
		// still space, just insert the child
		idx_t pos = n->count;
		if (n->children[pos]) {
			// find an empty position in the node list if the current position is occupied
			pos = 0;
			while (n->children[pos]) {
				pos++;
			}
		}
		n->children[pos] = new_child;
		n->child_index[key_byte] = pos;
		n->count++;

	} else {
		// node is full, grow to Node256
		auto new_node = Node256::New();
		art.memory_size += new_node->MemorySize(art, false);
		new_node->count = n->count;
		new_node->prefix = move(n->prefix);

		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->children[i] = n->children[n->child_index[i]];
				n->children[n->child_index[i]] = nullptr;
			}
		}

		D_ASSERT(art.memory_size >= node->MemorySize(art, false));
		art.memory_size -= node->MemorySize(art, false);
		Node::Delete(node);
		node = new_node;
		Node256::InsertChild(art, node, key_byte, new_child);
	}
}

void Node48::EraseChild(ART &art, Node *&node, idx_t pos) {
	auto n = (Node48 *)(node);

	// adjust the ART size
	if (n->GetARTPointer(pos)) {
		auto child = n->GetChild(art, pos);
		D_ASSERT(art.memory_size >= child->MemorySize(art, true));
		art.memory_size -= child->MemorySize(art, true);
	}

	// erase the child and decrease the count
	n->children[n->child_index[pos]].Reset();
	n->child_index[pos] = Node::EMPTY_MARKER;
	n->count--;

	// shrink node to Node16
	if (node->count < Node16::GetSize() - 3) {

		auto new_node = Node16::New();
		art.memory_size += new_node->MemorySize(art, false);
		new_node->prefix = move(n->prefix);

		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->key[new_node->count] = i;
				new_node->children[new_node->count++] = n->children[n->child_index[i]];
				n->children[n->child_index[i]] = nullptr;
			}
		}

		D_ASSERT(art.memory_size >= node->MemorySize(art, false));
		art.memory_size -= node->MemorySize(art, false);
		Node::Delete(node);
		node = new_node;
	}
}

idx_t Node48::GetSize() {
	return 48;
}

} // namespace duckdb
