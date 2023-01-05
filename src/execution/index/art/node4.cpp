#include "duckdb/execution/index/art/node4.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

Node4::Node4() : Node(NodeType::N4) {
	memset(key, 0, sizeof(key));
}

idx_t Node4::MemorySize(ART &art, const bool &recurse) {
	if (recurse) {
		return prefix.MemorySize() + sizeof(*this) + RecursiveMemorySize(art);
	}
	return prefix.MemorySize() + sizeof(*this);
}

idx_t Node4::GetChildPos(uint8_t k) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return Node::GetChildPos(k);
}

idx_t Node4::GetChildGreaterEqual(uint8_t k, bool &equal) {
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

idx_t Node4::GetMin() {
	return 0;
}

idx_t Node4::GetNextPos(idx_t pos) {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

idx_t Node4::GetNextPosAndByte(idx_t pos, uint8_t &byte) {
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

Node *Node4::GetChild(ART &art, idx_t pos) {
	D_ASSERT(pos < count);
	return children[pos].Unswizzle(art);
}

void Node4::ReplaceChildPointer(idx_t pos, Node *node) {
	children[pos] = node;
}

bool Node4::GetARTPointer(idx_t pos) {
	return children[pos] && !children[pos].IsSwizzled();
}

void Node4::InsertChild(ART &art, Node *&node, uint8_t key_byte, Node *new_child) {
	Node4 *n = (Node4 *)node;

	// insert new child node into node
	if (node->count < Node4::GetSize()) {
		// still space, just insert the child
		idx_t pos = 0;
		while ((pos < node->count) && (n->key[pos] < key_byte)) {
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
		// node is full, grow to Node16
		auto new_node = Node16::New();
		art.memory_size += new_node->MemorySize(art, false);
		new_node->count = n->count;
		new_node->prefix = move(node->prefix);

		for (idx_t i = 0; i < n->count; i++) {
			new_node->key[i] = n->key[i];
			new_node->children[i] = n->children[i];
			n->children[i] = nullptr;
		}
		n->count = 0;

		D_ASSERT(art.memory_size >= node->MemorySize(art, false));
		art.memory_size -= node->MemorySize(art, false);
		Node::Delete(node);
		node = new_node;
		Node16::InsertChild(art, node, key_byte, new_child);
	}
}

void Node4::EraseChild(ART &art, Node *&node, idx_t pos) {

	Node4 *n = (Node4 *)node;
	D_ASSERT(pos < n->count);
	D_ASSERT(n->count > 1);

	// adjust the ART size
	if (n->GetARTPointer(pos)) {
		auto child = n->GetChild(art, pos);
		D_ASSERT(art.memory_size >= child->MemorySize(art, true));
		art.memory_size -= child->MemorySize(art, true);
	}

	// erase the child and decrease the count
	n->children[pos].Reset();
	n->count--;
	D_ASSERT(n->count >= 1);

	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->children[pos] = n->children[pos + 1];
	}
	// set any remaining nodes as nullptr
	for (; pos < Node4::GetSize(); pos++) {
		n->children[pos] = nullptr;
	}

	// this is a one way node, compress
	if (n->count == 1) {

		// get only child and concatenate prefixes
		auto child_ref = n->GetChild(art, 0);
		// concatenate prefixes
		child_ref->prefix.Concatenate(art, n->key[0], node->prefix);
		// free this node
		n->children[0] = nullptr;

		D_ASSERT(art.memory_size >= n->MemorySize(art, false));
		art.memory_size -= n->MemorySize(art, false);
		Node::Delete(node);
		node = child_ref;
	}
}

idx_t Node4::GetSize() {
	return 4;
}

} // namespace duckdb
