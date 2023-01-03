#include "duckdb/execution/index/art/node256.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

Node256::Node256() : Node(NodeType::N256) {
}

idx_t Node256::MemorySize(ART &art, const bool &recurse) {
	if (recurse) {
		return prefix.MemorySize() + sizeof(*this) + RecursiveMemorySize(art);
	}
	return prefix.MemorySize() + sizeof(*this);
}

idx_t Node256::GetChildPos(uint8_t k) {
	if (children[k]) {
		return k;
	} else {
		return DConstants::INVALID_INDEX;
	}
}

idx_t Node256::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < Node256::GetSize(); pos++) {
		if (children[pos]) {
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

idx_t Node256::GetMin() {
	for (idx_t i = 0; i < Node256::GetSize(); i++) {
		if (children[i]) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetNextPos(idx_t pos) {
	pos == DConstants::INVALID_INDEX ? pos = 0 : pos++;
	for (; pos < Node256::GetSize(); pos++) {
		if (children[pos]) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

idx_t Node256::GetNextPosAndByte(idx_t pos, uint8_t &byte) {
	pos == DConstants::INVALID_INDEX ? pos = 0 : pos++;
	for (; pos < Node256::GetSize(); pos++) {
		if (children[pos]) {
			byte = uint8_t(pos);
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

Node *Node256::GetChild(ART &art, idx_t pos) {
	return children[pos].Unswizzle(art);
}

void Node256::ReplaceChildPointer(idx_t pos, Node *node) {
	children[pos] = node;
}

bool Node256::GetARTPointer(idx_t pos) {
	return children[pos] && !children[pos].IsSwizzled();
}

void Node256::InsertChild(ART &, Node *&node, uint8_t key_byte, Node *new_child) {
	auto n = (Node256 *)(node);

	n->count++;
	n->children[key_byte] = new_child;
}

void Node256::EraseChild(ART &art, Node *&node, idx_t pos) {
	auto n = (Node256 *)(node);

	// adjust the ART size
	if (n->GetARTPointer(pos)) {
		auto child = n->GetChild(art, pos);
		D_ASSERT(art.memory_size >= child->MemorySize(art, true));
		art.memory_size -= child->MemorySize(art, true);
	}

	// erase the child and decrease the count
	n->children[pos].Reset();
	n->count--;

	// shrink node to Node48
	if (node->count <= Node48::GetSize() - 11) {

		auto new_node = Node48::New();
		art.memory_size += new_node->MemorySize(art, false);
		new_node->prefix = move(n->prefix);

		for (idx_t i = 0; i < Node256::GetSize(); i++) {
			if (n->children[i]) {
				new_node->child_index[i] = new_node->count;
				new_node->children[new_node->count++] = n->children[i];
				n->children[i] = nullptr;
			}
		}

		D_ASSERT(art.memory_size >= node->MemorySize(art, false));
		art.memory_size -= node->MemorySize(art, false);
		Node::Delete(node);
		node = new_node;
	}
}

idx_t Node256::GetSize() {
	return 256;
}

} // namespace duckdb
