#include "execution/index/art/node48.hpp"

using namespace duckdb;


Node *Node48::getChild(const uint8_t k) const {
    if (childIndex[k] == emptyMarker) {
        return nullptr;
    } else {
        return child[childIndex[k]];
    }
}