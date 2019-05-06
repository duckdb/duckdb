#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"

using namespace duckdb;

Node **Node256::getChild(const uint8_t k) {
	return &child[k];
}
void Node256::insert(Node256 *node, Node **nodeRef, uint8_t keyByte, Node *child) {
	node->count++;
	node->child[keyByte] = child;
}

void Node256::erase(Node256* node,Node** nodeRef,uint8_t keyByte) {
    // Delete leaf from inner node
    node->child[keyByte]=NULL;
    node->count--;

    if (node->count==37) {
        // Shrink to Node48
        Node48 *newNode=new Node48();
        *nodeRef=newNode;
        copyPrefix(node,newNode);
        for (unsigned b=0;b<256;b++) {
            if (node->child[b]) {
                newNode->childIndex[b]=newNode->count;
                newNode->child[newNode->count]=node->child[b];
                newNode->count++;
            }
        }
        delete node;
    }
}