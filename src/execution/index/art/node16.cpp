#include "duckdb/execution/index/art/node16.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"

#include <cstring>

namespace duckdb {

Node16::Node16() : Node(NodeType::N16) {
	memset(key, 16, sizeof(key));
}

idx_t Node16::MemorySize(ART &art, const bool &recurse) {
	if (recurse) {
		return prefix.MemorySize() + sizeof(*this) + RecursiveMemorySize(art);
	}
	return prefix.MemorySize() + sizeof(*this);
}

idx_t Node16::GetChildPos(uint8_t k) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return Node::GetChildPos(k);
}

idx_t Node16::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] >= k) {
			if (key[pos] == k) {
				equal = true;
			} else {
				equal = false;
			}

			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node16::GetMin() {
	return 0;
}

idx_t Node16::GetNextPos(idx_t pos) {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

idx_t Node16::GetNextPosAndByte(idx_t pos, uint8_t &byte) {
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

Node *Node16::GetChild(ART &art, idx_t pos) {
	D_ASSERT(pos < count);
	return children[pos].Unswizzle(art);
}

void Node16::ReplaceChildPointer(idx_t pos, Node *node) {
	children[pos] = node;
}

bool Node16::GetARTPointer(idx_t pos) {
	return children[pos] && !children[pos].IsSwizzled();
}

void Node16::InsertChild(ART &art, Node *&node, uint8_t key_byte, Node *new_child) {
	Node16 *n = (Node16 *)node;

	// insert new child node into node
	if (n->count < Node16::GetSize()) {
		// still space, just insert the child
		idx_t pos = 0;
		while (pos < node->count && n->key[pos] < key_byte) {
			pos++;
		}
		if (n->children[pos]) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->children[i] = n->children[i - 1];
			}
		}
		n->key[pos] = key_byte;
		n->children[pos] = new_child;
		n->count++;

	} else {
		// node is full, grow to Node48
		auto new_node = Node48::New();
		art.memory_size += new_node->MemorySize(art, false);
		new_node->count = node->count;
		new_node->prefix = move(n->prefix);

		for (idx_t i = 0; i < node->count; i++) {
			new_node->child_index[n->key[i]] = i;
			new_node->children[i] = n->children[i];
			n->children[i] = nullptr;
		}

		D_ASSERT(art.memory_size >= node->MemorySize(art, false));
		art.memory_size -= node->MemorySize(art, false);
		Node::Delete(node);
		node = new_node;
		Node48::InsertChild(art, node, key_byte, new_child);
	}
}

void Node16::EraseChild(ART &art, Node *&node, idx_t pos) {

	auto n = (Node16 *)node;
	D_ASSERT(pos < n->count);

	// adjust the ART size
	if (n->GetARTPointer(pos)) {
		auto child = n->GetChild(art, pos);
		D_ASSERT(art.memory_size >= child->MemorySize(art, true));
		art.memory_size -= child->MemorySize(art, true);
	}

	// erase the child and decrease the count
	n->children[pos].Reset();
	n->count--;

	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->children[pos] = n->children[pos + 1];
	}
	// set any remaining nodes as nullptr
	for (; pos < Node16::GetSize(); pos++) {
		if (!n->children[pos]) {
			break;
		}
		n->children[pos] = nullptr;
	}

	// shrink node to Node4
	if (node->count < Node4::GetSize()) {

		auto new_node = Node4::New();
		art.memory_size += new_node->MemorySize(art, false);
		new_node->prefix = move(n->prefix);

		for (idx_t i = 0; i < n->count; i++) {
			new_node->key[new_node->count] = n->key[i];
			new_node->children[new_node->count++] = n->children[i];
			n->children[i] = nullptr;
		}

		D_ASSERT(art.memory_size >= node->MemorySize(art, false));
		art.memory_size -= node->MemorySize(art, false);
		Node::Delete(node);
		node = new_node;
	}
}

idx_t Node16::GetSize() {
	return 16;
}

} // namespace duckdb
