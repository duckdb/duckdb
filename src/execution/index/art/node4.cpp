#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

Node4::Node4() : Node(NodeType::N4) {
	memset(key, 0, sizeof(key));
}

void Node4::ReplaceChildPointer(idx_t pos, Node *node) {
	children[pos] = node;
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
	return Node::GetChildGreaterEqual(k, equal);
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

Node *Node4::GetChild(ART &art, idx_t pos) {
	D_ASSERT(pos < count);
	return children[pos].Unswizzle(art);
}

void Node4::Insert(Node *&node, uint8_t key_byte, Node *new_child) {
	Node4 *n = (Node4 *)node;

	// Insert leaf into inner node
	if (node->count < 4) {
		// Insert element
		idx_t pos = 0;
		while ((pos < node->count) && (n->key[pos] < key_byte)) {
			pos++;
		}
		if (n->children[pos] != 0) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->children[i] = n->children[i - 1];
			}
		}
		n->key[pos] = key_byte;
		n->children[pos] = new_child;
		n->count++;
	} else {
		// Grow to Node16
		auto new_node = new Node16();
		new_node->count = 4;
		new_node->prefix = move(node->prefix);
		for (idx_t i = 0; i < 4; i++) {
			new_node->key[i] = n->key[i];
			new_node->children[i] = n->children[i];
			n->children[i] = nullptr;
		}
		// Delete old node and replace it with new node
		delete node;
		node = new_node;
		Node16::Insert(node, key_byte, new_child);
	}
}

void Node4::Erase(Node *&node, int pos, ART &art) {
	Node4 *n = (Node4 *)node;
	D_ASSERT(pos < n->count);
	// erase the child and decrease the count
	n->children[pos].Reset();
	n->count--;
	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->children[pos] = n->children[pos + 1];
	}
	// set any remaining nodes as nullptr
	for (; pos < 4; pos++) {
		n->children[pos] = nullptr;
	}

	// This is a one way node
	if (n->count == 1) {
		auto child_ref = n->GetChild(art, 0);
		// concatenate prefixes
		child_ref->prefix.Concatenate(n->key[0], node->prefix);
		n->children[0] = nullptr;
		delete node;
		node = child_ref;
	}
}

} // namespace duckdb
