#include "duckdb/execution/index/art/node48.hpp"

#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

Node48::Node48() : Node(NodeType::N48) {
	for (idx_t i = 0; i < 256; i++) {
		child_index[i] = Node::EMPTY_MARKER;
	}
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

Node *Node48::GetChild(ART &art, idx_t pos) {
	D_ASSERT(child_index[pos] != Node::EMPTY_MARKER);
	return children[child_index[pos]].Unswizzle(art);
}

void Node48::ReplaceChildPointer(idx_t pos, Node *node) {
	children[child_index[pos]] = node;
}

void Node48::InsertChild(Node *&node, uint8_t key_byte, Node *new_child) {
	auto n = (Node48 *)node;

	// Insert new child node into node
	if (node->count < 48) {
		// Insert element
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
		// Grow to Node256
		auto new_node = Node256::New();
		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->children[i] = n->children[n->child_index[i]];
				n->children[n->child_index[i]] = nullptr;
			}
		}
		new_node->count = n->count;
		new_node->prefix = move(n->prefix);
		Node::Delete(node);
		node = new_node;
		Node256::InsertChild(node, key_byte, new_child);
	}
}

void Node48::EraseChild(Node *&node, int pos, ART &art) {
	auto n = (Node48 *)(node);
	n->children[n->child_index[pos]].Reset();
	n->child_index[pos] = Node::EMPTY_MARKER;
	n->count--;
	if (node->count <= 12) {
		auto new_node = Node16::New();
		new_node->prefix = move(n->prefix);
		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->key[new_node->count] = i;
				new_node->children[new_node->count++] = n->children[n->child_index[i]];
				n->children[n->child_index[i]] = nullptr;
			}
		}
		Node::Delete(node);
		node = new_node;
	}
}

bool Node48::Merge(MergeInfo &info, idx_t depth, Node *&l_parent, idx_t l_pos) {

	Node48 *r_n = (Node48 *)info.r_node;

	for (idx_t i = 0; i < 256; i++) {
		if (r_n->child_index[i] != Node::EMPTY_MARKER) {

			auto l_child_pos = info.l_node->GetChildPos(i);
			auto key_byte = (uint8_t)i;
			if (!Node::MergeAtByte(info, depth, l_child_pos, i, key_byte, l_parent, l_pos)) {
				return false;
			}
		}
	}
	return true;
}

idx_t Node48::GetSize() {
	return 48;
}

} // namespace duckdb
