//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {
enum class NodeType : uint8_t { NLeaf = 0, N4 = 1, N16 = 2, N48 = 3, N256 = 4 };
class ART;
class Node;

// Note: SwizzleablePointer assumes top 33 bits of the block_id are 0. Use a different
// pointer implementation if that does not hold.
class SwizzleablePointer;
using ARTPointer = SwizzleablePointer;

struct InternalType {
	explicit InternalType(Node *n);

	void Set(uint8_t *key_p, uint16_t key_size_p, ARTPointer *children_p, uint16_t children_size_p);
	uint8_t *key;
	uint16_t key_size;
	ARTPointer *children;
	uint16_t children_size;
};

struct MergeInfo {
	MergeInfo(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node)
	    : l_art(l_art), r_art(r_art), l_node(l_node), r_node(r_node) {};
	ART *l_art;
	ART *r_art;
	Node *&l_node;
	Node *&r_node;
};

struct ParentsOfNodes {
	ParentsOfNodes(Node *&l_parent, idx_t l_pos, Node *&r_parent, idx_t r_pos)
	    : l_parent(l_parent), l_pos(l_pos), r_parent(r_parent), r_pos(r_pos) {};
	Node *&l_parent;
	idx_t l_pos;
	Node *&r_parent;
	idx_t r_pos;
};

class Node {
public:
	static const uint8_t EMPTY_MARKER = 48;

public:
	explicit Node(NodeType type);
	virtual ~Node() {
	}

	//! Number of non-null children
	uint16_t count;
	//! Node type
	NodeType type;
	//! Compressed path (prefix)
	Prefix prefix;

	static void Delete(Node *node);
	//! Returns the memory size of the node
	virtual idx_t MemorySize(ART &art, const bool &recurse);
	//! Get the position of a child corresponding exactly to the specific byte, returns DConstants::INVALID_INDEX if not
	//! exists
	virtual idx_t GetChildPos(uint8_t k) {
		return DConstants::INVALID_INDEX;
	}
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	virtual idx_t GetChildGreaterEqual(uint8_t k, bool &equal) {
		throw InternalException("Unimplemented GetChildGreaterEqual for ART node");
	}
	//! Get the position of the minimum element in the node
	virtual idx_t GetMin();
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position. if pos ==
	//! DConstants::INVALID_INDEX, then the first valid position in the node is returned
	virtual idx_t GetNextPos(idx_t pos) {
		return DConstants::INVALID_INDEX;
	}
	//! Get the next position and byte in the node, or DConstants::INVALID_INDEX if there is no next position. if pos ==
	//! DConstants::INVALID_INDEX, then the first valid position in the node is returned
	virtual idx_t GetNextPosAndByte(idx_t pos, uint8_t &byte) {
		return DConstants::INVALID_INDEX;
	}
	//! Get the child at the specified position in the node. pos should be between [0, count). Throws an assertion if
	//! the element is not found
	virtual Node *GetChild(ART &art, idx_t pos);
	//! Replaces the pointer to a child node
	virtual void ReplaceChildPointer(idx_t pos, Node *node);
	//! Returns the ART pointer at pos
	virtual ARTPointer &GetARTPointer(idx_t pos);

	//! Insert a new child node at key_byte into the node
	static void InsertChild(ART &art, Node *&node, uint8_t key_byte, Node *new_child);
	//! Erase child node entry from node
	static void EraseChild(ART &art, Node *&node, idx_t pos);
	//! Get the corresponding node type for the provided size
	static NodeType GetTypeBySize(idx_t size);
	//! Create a new node of the specified type
	static void New(const NodeType &type, Node *&node);

	//! Returns the string representation of a node
	string ToString(ART &art);
	//! Serialize this node
	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);
	//! Returns the memory size of the node
	idx_t RecursiveMemorySize(ART &art);

	//! Deserialize this node
	static Node *Deserialize(ART &art, idx_t block_id, idx_t offset);
	//! Merge two ART
	static bool MergeARTs(ART *l_art, ART *r_art);

private:
	//! Serialize internal nodes
	BlockPointer SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type);
	//! Deserialize internal nodes
	void DeserializeInternal(ART &art, duckdb::MetaBlockReader &reader);
};

} // namespace duckdb
