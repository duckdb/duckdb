#include "duckdb/execution/index/art/node4.hpp"

#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

Node4::Node4() : Node(NodeType::N4) {
	memset(key, 0, sizeof(key));
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

Node *Node4::GetChild(ART &art, idx_t pos) {
	D_ASSERT(pos < count);
	return children[pos].Unswizzle(art);
}

void Node4::ReplaceChildPointer(idx_t pos, Node *node) {
	children[pos] = node;
}

void Node4::InsertChild(Node *&node, uint8_t key_byte, Node *new_child) {
	Node4 *n = (Node4 *)node;

	// Insert new child node into node
	if (node->count < 4) {
		// Insert element
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
		// Grow to Node16
		auto new_node = Node16::New();
		new_node->count = 4;
		new_node->prefix = move(node->prefix);
		for (idx_t i = 0; i < 4; i++) {
			new_node->key[i] = n->key[i];
			new_node->children[i] = n->children[i];
			n->children[i] = nullptr;
		}
		// Delete old node and replace it with new Node16
		Node::Delete(node);
		node = new_node;
		Node16::InsertChild(node, key_byte, new_child);
	}
}

void Node4::EraseChild(Node *&node, int pos, ART &art) {
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
		Node::Delete(node);
		node = child_ref;
	}
}

bool Node4::Merge(MergeInfo &info, idx_t depth, Node *&l_parent, idx_t l_pos) {

	Node4 *r_n = (Node4 *)info.r_node;

	for (idx_t i = 0; i < info.r_node->count; i++) {

		auto l_child_pos = info.l_node->GetChildPos(r_n->key[i]);
		if (!Node::MergeAtByte(info, depth, l_child_pos, i, r_n->key[i], l_parent, l_pos)) {
			return false;
		}
	}
	return true;
}

idx_t Node4::GetSize() {
	return 4;
}

} // namespace duckdb
