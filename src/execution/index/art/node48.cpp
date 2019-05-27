#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"

using namespace duckdb;

unique_ptr<Node> *Node48::getChild(const uint8_t k) {
	if (childIndex[k] == Node::EMPTY_MARKER) {
		return nullptr;
	} else {
		return &child[childIndex[k]];
	}
}

int Node48::getPos(const uint8_t k) {
	return k;
}

unique_ptr<Node> *Node48::getMin() {
	unsigned pos = 0;
	while (childIndex[pos] == Node::EMPTY_MARKER) {
		pos++;
	}
	return &child[childIndex[pos]];
}

void Node48::insert(unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child) {
	Node48 *n = static_cast<Node48 *>(node.get());

	// Insert leaf into inner node
	if (node->count < 48) {
		// Insert element
		index_t pos = n->count;
		if (n->child[pos]) {
			// find an empty position in the node list if the current position is occupied
			pos = 0;
			while(n->child[pos]) {
				pos++;
			}
		}
		n->child[pos] = move(child);
		n->childIndex[keyByte] = pos;
		n->count++;
	} else {
		// Grow to Node256
		auto newNode = make_unique<Node256>(node->max_prefix_length);
		for (index_t i = 0; i < 256; i++) {
			if (n->childIndex[i] != 48) {
				newNode->child[i] = move(n->child[n->childIndex[i]]);
			}
		}
		newNode->count = n->count;
		copyPrefix(n, newNode.get());
		node = move(newNode);
		Node256::insert(node, keyByte, child);
	}
}

void Node48::erase(unique_ptr<Node> &node, int pos) {
	Node48 *n = static_cast<Node48 *>(node.get());

	if (node->count > 12) {
		n->child[pos].reset();
		n->childIndex[pos] = 48;
		n->count--;
	} else {
		auto newNode = make_unique<Node16>(node->max_prefix_length);
		copyPrefix(n, newNode.get());
		for (index_t i = 0; i < 256; i++) {
			if (n->childIndex[i] != Node::EMPTY_MARKER) {
				newNode->key[newNode->count] = i;
				newNode->child[newNode->count++] = move(n->child[n->childIndex[i]]);
			}
		}
		node = move(newNode);
	}
}
