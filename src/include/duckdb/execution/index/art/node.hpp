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
	//! Create a new node of the specified type
	static void NewNode(NodeType &type, Node *&node);

	//! Serialize this node
	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);
	//! Deserialize this node
	static Node *Deserialize(ART &art, idx_t block_id, idx_t offset);

	//! Merge two ART
	static void MergeARTs(ART *l_art, ART *r_art);

private:
	//! Serialize internal nodes
	BlockPointer SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type);
	//! Deserialize internal nodes
	void DeserializeInternal(duckdb::MetaBlockReader &reader);
};

} // namespace duckdb
