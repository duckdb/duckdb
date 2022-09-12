//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {
enum class NodeType : uint8_t { NLeaf = 0, N4 = 1, N16 = 2, N48 = 3, N256 = 4 };
class ART;
class Node;
class SwizzleablePointer;

struct InternalType {
	explicit InternalType(Node *n);

	void Set(uint8_t *key_p, uint16_t key_size_p, SwizzleablePointer *children_p, uint16_t children_size_p);

	uint8_t *key;
	uint16_t key_size;
	SwizzleablePointer *children;
	uint16_t children_size;
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
	//! Get the child at the specified position in the node. pos should be between [0, count). Throws an assertion if
	//! the element is not found
	virtual Node *GetChild(ART &art, idx_t pos);
	//! Replaces the pointer to a child node
	virtual void ReplaceChildPointer(idx_t pos, Node *node);

	//! Insert a new child node at key_byte into the node
	static void InsertChildNode(Node *&node, uint8_t key_byte, Node *new_child);
	//! Erase child node entry from node
	static void Erase(Node *&node, idx_t pos, ART &art);
	//! Get the corresponding node type for the provided size
	static NodeType GetTypeBySize(idx_t size);

	//! Serialize this node
	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);
	//! Deserialize this node
	static Node *Deserialize(ART &art, idx_t block_id, idx_t offset);

	//! Resolve the prefixes of two nodes and then merge r_node into l_node
	static void ResolvePrefixesAndMerge(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth,
	                                    Node *&l_node_parent, idx_t l_node_pos, Node *&r_node_parent, idx_t r_node_pos);
	//! Merge r_node into l_node, they have matching prefixes
	static void Merge(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth, Node *&l_node_parent,
	                  idx_t l_node_pos, Node *&r_node_parent, idx_t r_node_pos);
	//! Merge Node with Node16 or Node4
	template <class R_NODE_TYPE>
	static void MergeNodeWithNode16OrNode4(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth,
	                                       Node *&l_node_parent, idx_t l_node_pos);
	//! Merge Node with Node48
	static void MergeNodeWithNode48(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth,
	                                Node *&l_node_parent, idx_t l_node_pos);
	//! Merge Node with Node256
	static void MergeNodeWithNode256(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth,
	                                 Node *&l_node_parent, idx_t l_node_pos);
	//! Merge a specific byte of two nodes and its possible children
	static void MergeByte(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth, idx_t &l_child_pos,
	                      idx_t &r_pos, uint8_t &key_byte, Node *&l_node_parent, idx_t l_node_pos);

private:
	//! Serialize internal nodes
	BlockPointer SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type);
	//! Deserialize internal nodes
	void DeserializeInternal(duckdb::MetaBlockReader &reader);
};

} // namespace duckdb
