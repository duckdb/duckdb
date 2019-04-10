#include "execution/index/art/node256.hpp"

using namespace duckdb;


Node *Node256::getChild(const uint8_t k) const {
    return child[k];
}
void Node256::insert(Node256* node,Node** nodeRef,uint8_t keyByte,Node* child) {
    node->count++;
    node->child[keyByte]=child;
}