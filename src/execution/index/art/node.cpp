#include "execution/index/art/node.hpp"
#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"

using namespace duckdb;

inline bool Node::isLeaf(Node* node) {
    return reinterpret_cast<uintptr_t>(node)&1;
}
inline Node*  Node::makeLeaf(uintptr_t tid) {
    return reinterpret_cast<Node*>((tid<<1)|1);
}

inline uintptr_t  Node::getLeafValue(Node* node) {
    return reinterpret_cast<uintptr_t>(node)>>1;
}

void  Node::loadKey(uintptr_t tid,uint8_t key[]) {
    reinterpret_cast<uint64_t*>(key)[0]=__builtin_bswap64(tid);
}

unsigned Node::min(unsigned a,unsigned b) {
    return (a<b)?a:b;
}

uint8_t Node::flipSign(uint8_t keyByte) {
    return keyByte^128;
}

void Node::copyPrefix(Node* src,Node* dst) {
    dst->prefixLength=src->prefixLength;
    memcpy(dst->prefix,src->prefix,min(src->prefixLength,maxPrefixLength));
}

inline unsigned Node::ctz(uint16_t x) {
    unsigned n = 1;
    if ((x & 0xFF) == 0) {
      n += 8;
      x = x >> 8;
    }
    if ((x & 0x0F) == 0) {
      n += 4;
      x = x >> 4;
    }
    if ((x & 0x03) == 0) {
      n += 2;
      x = x >> 2;
    }
    return n - (x & 1);
}

Node* Node::minimum(Node* node) {
    if (!node)
        return NULL;

    if (isLeaf(node))
        return node;

    switch (node->type) {
        case NodeType::N4: {
            Node4* n=static_cast<Node4*>(node);
            return minimum(n->child[0]);
        }
        case NodeType::N16: {
            Node16* n=static_cast<Node16*>(node);
            return minimum(n->child[0]);
        }
        case NodeType::N48: {
            Node48* n=static_cast<Node48*>(node);
            unsigned pos=0;
            while (n->childIndex[pos]==emptyMarker)
                pos++;
            return minimum(n->child[n->childIndex[pos]]);
        }
        case NodeType::N256: {
            Node256* n=static_cast<Node256*>(node);
            unsigned pos=0;
            while (!n->child[pos])
                pos++;
            return minimum(n->child[pos]);
        }
    }
}

Node * Node::findChild(const uint8_t k, const Node *node) {
    switch (node->type) {
        case NodeType::N4: {
            auto n = static_cast<const Node4 *>(node);
            return n->getChild(k);
        }
        case NodeType::N16: {
            auto n = static_cast<const Node16 *>(node);
            return n->getChild(k);
        }
        case NodeType::N48: {
            auto n = static_cast<const Node48 *>(node);
            return n->getChild(k);
        }
        case NodeType::N256: {
            auto n = static_cast<const Node256 *>(node);
            return n->getChild(k);
        }
    }
    assert(false);
}

unsigned Node::prefixMismatch(Node* node,uint8_t key[],unsigned depth,unsigned maxKeyLength) {
    unsigned pos;
    if (node->prefixLength>maxPrefixLength) {
        for (pos=0;pos<maxPrefixLength;pos++)
            if (key[depth+pos]!=node->prefix[pos])
                return pos;
        uint8_t minKey[maxKeyLength];
        loadKey(getLeafValue(minimum(node)),minKey);
        for (;pos<node->prefixLength;pos++)
            if (key[depth+pos]!=minKey[depth+pos])
                return pos;
    } else {
        for (pos=0;pos<node->prefixLength;pos++)
            if (key[depth+pos]!=node->prefix[pos])
                return pos;
    }
    return pos;
}

void Node::insertLeaf(Node* node,Node** nodeRef,uint8_t key, Node* newNode){
    switch (node->type) {
        case NodeType::N4:
            Node4::insert(static_cast<Node4*>(node),nodeRef,key,newNode);
            break;
        case NodeType::N16:
            Node16::insert(static_cast<Node16*>(node),nodeRef,key,newNode);
            break;
        case NodeType::N48:
            Node48::insert(static_cast<Node48*>(node),nodeRef,key,newNode);
            break;
        case NodeType::N256:
            Node256::insert(static_cast<Node256*>(node),nodeRef,key,newNode);
            break;
    }
}


void Node::insert(Node* node,Node** nodeRef,uint8_t key[],unsigned depth,uintptr_t value,unsigned maxKeyLength) {
    // Insert the leaf value into the tree

    if (node==NULL) {
        *nodeRef=makeLeaf(value);
        return;
    }

    if (isLeaf(node)) {
        // Replace leaf with Node4 and store both leaves in it
        uint8_t existingKey[maxKeyLength];
        loadKey(getLeafValue(node),existingKey);
        unsigned newPrefixLength=0;
        while (existingKey[depth+newPrefixLength]==key[depth+newPrefixLength])
            newPrefixLength++;

        Node4* newNode=new Node4();
        newNode->prefixLength=newPrefixLength;
        memcpy(newNode->prefix,key+depth,min(newPrefixLength,maxPrefixLength));
        *nodeRef=newNode;

        Node4::insert(newNode,nodeRef,existingKey[depth+newPrefixLength],node);
        Node4::insert(newNode,nodeRef,key[depth+newPrefixLength],makeLeaf(value));
        return;
    }

    // Handle prefix of inner node
    if (node->prefixLength) {
        unsigned mismatchPos=prefixMismatch(node,key,depth,maxKeyLength);
        if (mismatchPos!=node->prefixLength) {
            // Prefix differs, create new node
            Node4* newNode=new Node4();
            *nodeRef=newNode;
            newNode->prefixLength=mismatchPos;
            memcpy(newNode->prefix,node->prefix,min(mismatchPos,maxPrefixLength));
            // Break up prefix
            if (node->prefixLength<maxPrefixLength) {
                Node4::insert(newNode,nodeRef,node->prefix[mismatchPos],node);
                node->prefixLength-=(mismatchPos+1);
                memmove(node->prefix,node->prefix+mismatchPos+1,min(node->prefixLength,maxPrefixLength));
            } else {
                node->prefixLength-=(mismatchPos+1);
                uint8_t minKey[maxKeyLength];
                loadKey(getLeafValue(minimum(node)),minKey);
                Node4::insert(newNode,nodeRef,minKey[depth+mismatchPos],node);
                memmove(node->prefix,minKey+depth+mismatchPos+1,min(node->prefixLength,maxPrefixLength));
            }
            Node4::insert(newNode,nodeRef,key[depth+mismatchPos],makeLeaf(value));
            return;
        }
        depth+=node->prefixLength;
    }

    // Recurse
    Node* child=findChild(key[depth],node);
    if (child) {
        insert(child,&child,key,depth+1,value,maxKeyLength);
        return;
    }

    Node* newNode=makeLeaf(value);
    insertLeaf(node,nodeRef,key[depth],newNode);
}