#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

Node4::Node4(ART &art, size_t compression_length) : Node(art, NodeType::N4, compression_length) {
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

unique_ptr<Node> *Node4::GetChild(idx_t pos) {
	D_ASSERT(pos < count);
	return &child[pos];
}

void Node4::Insert(ART &art, unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node4 *n = static_cast<Node4 *>(node.get());

	// Insert leaf into inner node
	if (node->count < 4) {
		// Insert element
		idx_t pos = 0;
		while ((pos < node->count) && (n->key[pos] < key_byte)) {
			pos++;
		}
		if (n->child[pos] != nullptr) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->child[i] = move(n->child[i - 1]);
			}
		}
		n->key[pos] = key_byte;
		n->child[pos] = move(child);
		n->count++;
	} else {
		// Grow to Node16
		auto new_node = make_unique<Node16>(art, n->prefix_length);
		new_node->count = 4;
		CopyPrefix(art, node.get(), new_node.get());
		for (idx_t i = 0; i < 4; i++) {
			new_node->key[i] = n->key[i];
			new_node->child[i] = move(n->child[i]);
		}
		node = move(new_node);
		Node16::Insert(art, node, key_byte, child);
	}
}

void Node4::Erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node4 *n = static_cast<Node4 *>(node.get());
	D_ASSERT(pos < n->count);

	// erase the child and decrease the count
	n->child[pos].reset();
	n->count--;
	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->child[pos] = move(n->child[pos + 1]);
	}

	// This is a one way node
	if (n->count == 1) {
		auto childref = n->child[0].get();
		//! concatenate prefixes
		auto new_length = node->prefix_length + childref->prefix_length + 1;
		//! have to allocate space in our prefix array
		unique_ptr<uint8_t[]> new_prefix = unique_ptr<uint8_t[]>(new uint8_t[new_length]);

		//! first move the existing prefix (if any)
		for (uint32_t i = 0; i < childref->prefix_length; i++) {
			new_prefix[new_length - (i + 1)] = childref->prefix[childref->prefix_length - (i + 1)];
		}
		//! now move the current key as part of the prefix
		new_prefix[node->prefix_length] = n->key[0];
		//! finally add the old prefix
		for (uint32_t i = 0; i < node->prefix_length; i++) {
			new_prefix[i] = node->prefix[i];
		}
		//! set new prefix and move the child
		childref->prefix = move(new_prefix);
		childref->prefix_length = new_length;
		node = move(n->child[0]);
	}
}

} // namespace duckdb
