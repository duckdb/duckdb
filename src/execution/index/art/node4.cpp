#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/art.hpp"

using namespace duckdb;

Node4::Node4(ART &art) : Node(art, NodeType::N4) {
	memset(key, 0, sizeof(key));
}

index_t Node4::GetChildPos(uint8_t k) {
	for (index_t pos = 0; pos < count; pos++) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return Node::GetChildPos(k);
}

index_t Node4::GetChildGreaterEqual(uint8_t k) {
	for (index_t pos = 0; pos < count; pos++) {
		if (key[pos] >= k) {
			return pos;
		}
	}
	return Node::GetChildGreaterEqual(k);
}

index_t Node4::GetNextPos(index_t pos) {
	if (pos == INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : INVALID_INDEX;
}

unique_ptr<Node> *Node4::GetChild(index_t pos) {
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
		auto newNode = make_unique<Node16>(art);
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
		if (childref->type == NodeType::NLeaf) {
			// Concantenate prefixes
			uint32_t l1 = childref->prefix_length;
			if (l1 < art.maxPrefix) {
				n->prefix[l1] = n->key[0];
				l1++;
			}
			if (l1 < art.maxPrefix) {
				uint32_t l2 = std::min(childref->prefix_length, art.maxPrefix - l1);
				for (index_t i = 0; i < l2; i++) {
					n->prefix[l1 + i] = childref->prefix[l2];
				}
				l1 += l2;
			}
			for (index_t i = 0; i < l1; i++) {
				childref->prefix[i] = n->prefix[i];
			}
		}
		node = move(n->child[0]);
	}
}
