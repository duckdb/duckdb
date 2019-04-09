#pragma once
#include "common/common.hpp"


namespace duckdb {
    enum class NodeType : uint8_t {
        N4 = 0, N16 = 1, N48 = 2, N256 = 3
    };
    // The maximum prefix length for compressed paths stored in the
    // header, if the path is longer it is loaded from the database on
    // demand
    static const unsigned maxPrefixLength=9;

    class Node {
    public:
        void insert(Node* node,Node** nodeRef,uint8_t key[],unsigned depth,uintptr_t value,unsigned maxKeyLength);
        //! length of the compressed path (prefix)
        uint32_t prefixLength;
        //! number of non-null children
        uint16_t count;
        //! node type
        NodeType type;
        //! compressed path (prefix)
        uint8_t prefix[maxPrefixLength];
        static const uint8_t emptyMarker = 48;

        Node(NodeType type) : prefixLength(0), count(0), type(type) {}

    private:
        //! Helper function to compare two elements and return the smaller
        unsigned min(unsigned a,unsigned b);
        //! Find the leaf with smallest element in the tree
        Node* minimum(Node* node);
        //! Returns the stored in the leaf
        inline uintptr_t getLeafValue(Node* node);
        //! Checks if node is Leaf
        inline bool isLeaf(Node* node);
        //! Create a Leaf
        inline Node* makeLeaf(uintptr_t tid);
        //! Store the key of the tuple into the key vector
        void  loadKey(uintptr_t tid,uint8_t key[]);
        //! Find the next child for the keyByte
        Node * findChild(const uint8_t k, const Node *node);
    };

}