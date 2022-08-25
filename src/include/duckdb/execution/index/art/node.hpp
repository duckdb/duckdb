//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {
enum class NodeType : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3, NLeaf = 4 };

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
	//! number of non-null children
	uint16_t count;
	//! node type
	NodeType type;
	//! compressed path (prefix)
	Prefix prefix;

	//! Get the position of a child corresponding exactly to the specific byte, returns DConstants::INVALID_INDEX if not
	//! exists
	virtual idx_t GetChildPos(uint8_t k) {
		return DConstants::INVALID_INDEX;
	}
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	virtual idx_t GetChildGreaterEqual(uint8_t k, bool &equal) {
		throw InternalException("Unimplemented GetChildGreaterEqual for ARTNode");
	}
	//! Get the position of the biggest element in node
	virtual idx_t GetMin();

	//! Serialize this Node
	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);

	static Node *Deserialize(ART &art, idx_t block_id, idx_t offset);

	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position. if pos ==
	//! DConstants::INVALID_INDEX, then the first valid position in the node will be returned.
	virtual idx_t GetNextPos(idx_t pos) {
		return DConstants::INVALID_INDEX;
	}
	//! Get the child at the specified position in the node. pos should be between [0, count). Throws an assertion if
	//! the element is not found.
	virtual Node *GetChild(ART &art, idx_t pos);

	//! Replaces the pointer
	virtual void ReplaceChildPointer(idx_t pos, Node *node);

	//! Insert leaf into inner node
	static void InsertLeaf(Node *&node, uint8_t key, Node *new_node);
	//! Erase entry from node
	static void Erase(Node *&node, idx_t pos, ART &art);

private:
	//! Serialize Internal Nodes
	BlockPointer SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type);
	//! Deserialize Internal Nodes
	void DeserializeInternal(duckdb::MetaBlockReader &reader);
};

} // namespace duckdb
