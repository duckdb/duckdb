#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"

using namespace duckdb;

// TODO : In the future this can be performed using SIMD (#include <emmintrin.h>  x86 SSE intrinsics)
unique_ptr<Node>* Node16::getChild(const uint8_t k) {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return &child[i];
		}
	}
	return nullptr;
}

unique_ptr<Node>* Node16::getChild(const uint8_t k, int& pos) {
    for (pos = 0; pos < count; ++pos) {
        if (key[pos] == k) {
            return &child[pos];
        }
    }
    return nullptr;
}


unique_ptr<Node>* Node16::getMin() {
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

void Node16::insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child){
    Node16 *n = static_cast<Node16 *>(node.get());

if (n->count < 16) {

        n->key[n->count] = keyByte;
        n->child[n->count] = move(child);
        n->count++;
	} else {
		// Grow to Node48
        auto newNode = make_unique<Node48>(node->maxPrefixLength);
		for (unsigned i = 0; i < node->count; i++) {
            newNode->childIndex[n->key[i]] = i;
            newNode->child[i] = move(n->child[i]);
        }
		copyPrefix(n, newNode.get());
		newNode->count = node->count;
		node = move(newNode);

    Node48::insert(node, keyByte, child);
	}
}

void Node16::shrink (unique_ptr<Node>& node){
    Node16 *n = static_cast<Node16 *>(node.get());
    auto newNode = make_unique<Node4>(node->maxPrefixLength);
    for (unsigned i = 0; i < 16; i++)
        if (n->key[i] != 16){
            newNode->key[newNode->count] = n->key[i];
            newNode->child[newNode->count] = move(n->child[n->key[i]]);
            newNode->count++;
        }
    copyPrefix(n, newNode.get());
    node = move(newNode);
}