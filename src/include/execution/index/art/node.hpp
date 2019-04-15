#pragma once
#include "common/common.hpp"


namespace duckdb {
    enum class NodeType : uint8_t {
        N4 = 0, N16 = 1, N48 = 2, N256 = 3
    };
    //! The maximum prefix length for compressed paths stored in the
    //! header, if the path is longer it is loaded from the database on demand
    static const unsigned maxPrefixLength=9;

    class Node {
    public:
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
        //! Count trailing zeros, only defined for x>0 (from Hacker's Delight)
        static inline unsigned ctz(uint16_t x);
        //! Find the node with a matching key, optimistic version
        Node* lookup(Node* node,uint8_t key[],unsigned keyLength,unsigned depth,unsigned maxKeyLength);
        //! Insert the leaf value into the tree
        static void insert(Node* node,Node** nodeRef,uint8_t key[],unsigned depth,uintptr_t value,unsigned maxKeyLength);
        //! Store the key of the tuple into the key vector
        static void loadKey(uintptr_t tid,uint8_t key[]);
        //! Copies the prefix from the source to the destination node
        static void copyPrefix(Node* src,Node* dst);
        //! Flip the sign bit, enables signed SSE comparison of unsigned values
        static uint8_t flipSign(uint8_t keyByte);
        //! Returns the stored in the leaf
        static inline uint64_t getLeafValue(const Node* node);

    private:
        //! Compare two elements and return the smaller
        static unsigned min(unsigned a, unsigned b);
        //! Find the leaf with smallest element in the tree
        static Node* minimum(Node* node);
        //! Checks if node is Leaf
        static inline bool isLeaf(Node* node);
        //! Create a Leaf
        static inline Node* makeLeaf(uint64_t tid);
        //! Find the next child for the keyByte
        static Node * findChild(const uint8_t k, const Node *node);
        //! Compare the key with the prefix of the node, return the number matching bytes
        static unsigned prefixMismatch(Node* node,uint8_t key[],unsigned depth,unsigned maxKeyLength);
        //! Insert leaf into inner node
        static void insertLeaf(Node* node,Node** nodeRef,uint8_t key, Node* newNode);

        };
    //!TODO: For duplicates
    class Leaf: public Node{
    public:
        uint64_t row_id;
    };

}