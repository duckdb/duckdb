#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"

using namespace duckdb;

unique_ptr<Node>* Node256::getChild(const uint8_t k) {
	if (child[k]) {
		return &child[k];
	} else {
		return NULL;
	}
}

int  Node256::getPos(const uint8_t k){
 return k;
}

unique_ptr<Node>* Node256::getMin() {
	unsigned pos = 0;
	while (!child[pos])
		pos++;
	auto result = &child[pos];
	return result;
}

void Node256::insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child) {
    Node256 *n = static_cast<Node256 *>(node.get());

    n->count++;
    n->child[keyByte] = move(child);
}


void Node256::erase(unique_ptr<Node>& node,int pos){
    Node256 *n = static_cast<Node256 *>(node.get());

    if (node->count > 37){
        n->child[pos].reset();
        n->count--;
    }
    else{
        auto newNode = make_unique<Node48>(node->maxPrefixLength);
        copyPrefix(n, newNode.get());
        for (unsigned b = 0; b < 256; b++) {
            if (n->child[b]) {
                newNode->childIndex[b] = newNode->count;
                newNode->child[newNode->count] = move(n->child[b]);
                newNode->count++;
            }
        }
    }

}
