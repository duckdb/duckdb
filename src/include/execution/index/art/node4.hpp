#pragma once
#import "node.hpp"

namespace duckdb {

    class Node4 : public Node {
    public:
        uint8_t key[4];
        Node *child[4];

        Node4() : Node(NodeType::N4) {
            memset(key, 0, sizeof(key));
            memset(child, 0, sizeof(child));
        }

        Node *getChild(const uint8_t k) const;
        void insertNode4(Node4* node,Node** nodeRef,uint8_t keyByte,Node* child);
    };
}