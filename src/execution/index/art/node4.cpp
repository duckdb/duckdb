#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"

using namespace duckdb;

unique_ptr<Node>* Node4::getChild(const uint8_t k) {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return &child[i];
		}
	}
	return nullptr;
}

unique_ptr<Node>* Node4::getChild(const uint8_t k, int& pos) {
	for (pos = 0; pos < count; ++pos) {
		if (key[pos] == k) {
			return &child[pos];
		}
	}
	return nullptr;
}

unique_ptr<Node>* Node4::getMin() {
	auto result = &child[0];
	auto res_key = key[0];
	if (count > 1){
		for (uint32_t i = 1; i < count; ++i) {
			if (key[i] < res_key) {
				result = &child[i];
                res_key = key[i];
			}
		}
	}
	return result;
}

void Node4::insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child) {
    Node4 *n = static_cast<Node4 *>(node.get());

    // Insert leaf into inner node
	if (node->count < 4) {
		// Insert element
        n->key[n->count] = keyByte;
        n->child[n->count] = move(child);
        n->count++;
	} else {
		// Grow to Node16
        auto newNode = make_unique<Node16>(node->maxPrefixLength);
        newNode->count = 4;
		copyPrefix(node.get(), newNode.get());
		for (unsigned i = 0; i < 4; i++){
			newNode->key[i] = n->key[i];
			newNode->child[i] = move(n->child[i]);
		}
		node = move(newNode);
		Node16::insert(node, keyByte, child);
	}
}

