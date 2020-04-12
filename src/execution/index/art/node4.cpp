#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/art.hpp"

using namespace duckdb;

Node4::Node4(ART &art, size_t compressionLength) : Node(art, NodeType::N4, compressionLength) {
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
	if (pos == INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : INVALID_INDEX;
}

unique_ptr<Node> *Node4::GetChild(idx_t pos) {
	assert(pos < count);
	return &child[pos];
}

void Node4::insert(ART &art, unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child) {
	Node4 *n = static_cast<Node4 *>(node.get());

	// Insert leaf into inner node
	if (node->count < 4) {
		// Insert element
		unsigned pos;
		for (pos = 0; (pos < node->count) && (n->key[pos] < keyByte); pos++)
			;
		if (n->child[pos] != nullptr) {
			for (unsigned i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->child[i] = move(n->child[i - 1]);
			}
		}
		n->key[pos] = keyByte;
		n->child[pos] = move(child);
		n->count++;
	} else {
		// Grow to Node16
		auto newNode = make_unique<Node16>(art, n->prefix_length);
		newNode->count = 4;
		CopyPrefix(art, node.get(), newNode.get());
		for (unsigned i = 0; i < 4; i++) {
			newNode->key[i] = n->key[i];
			newNode->child[i] = move(n->child[i]);
		}
		node = move(newNode);
		Node16::insert(art, node, keyByte, child);
	}
}

void Node4::erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node4 *n = static_cast<Node4 *>(node.get());
	assert(pos < n->count);

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
		;

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
