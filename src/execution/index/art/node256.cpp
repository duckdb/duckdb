#include "execution/index/art/node256.hpp"

using namespace duckdb;


Node *Node256::getChild(const uint8_t k) const {
    return child[k];
}
