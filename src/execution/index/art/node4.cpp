#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"

using namespace duckdb;

unique_ptr<Node> *Node4::getChild(const uint8_t k) {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return &child[i];
		}
	}
	return nullptr;
}

int Node4::getPos(const uint8_t k) {
	int pos;
	for (pos = 0; pos < count; ++pos) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return -1;
}

unique_ptr<Node> *Node4::getMin() {
	return &child[0];
}

void Node4::insert(unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child) {
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
		auto newNode = make_unique<Node16>(node->maxPrefixLength);
		newNode->count = 4;
		copyPrefix(node.get(), newNode.get());
		for (unsigned i = 0; i < 4; i++) {
			newNode->key[i] = n->key[i];
			newNode->child[i] = move(n->child[i]);
		}
		node = move(newNode);
		Node16::insert(node, keyByte, child);
	}
}

void Node4::erase(unique_ptr<Node> &node, int pos) {
	Node4 *n = static_cast<Node4 *>(node.get());

	if (n->count == 4) {
		n->child[3].reset();
		n->count--;
	} else {
		for (; pos < n->count; pos++) {
			n->key[pos] = n->key[pos + 1];
			n->child[pos] = move(n->child[pos + 1]);
		}
		n->count--;
	}
	// This is a one way node
	if (n->count == 1) {
		auto childref = n->child[0].get();
		if (childref->type == NodeType::NLeaf) {
			// Concantenate prefixes
			int l1 = childref->prefixLength;
			if (l1 < n->maxPrefixLength) {
				n->prefix[l1] = n->key[0];
				l1++;
			}
			if (l1 < n->maxPrefixLength) {
				int l2 = min(childref->prefixLength, n->maxPrefixLength - l1);
				for (int i = 0; i < l2; i++) {
					n->prefix[l1 + i] = childref->prefix[l2];
				}
				l1 += l2;
			}
			for (int i = 0; i < l1; i++) {
				childref->prefix[i] = n->prefix[i];
			}
		}
		node = move(n->child[0]);
	}
}
