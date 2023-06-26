//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

// classes
enum class NType : uint8_t {
	PREFIX = 1,
	LEAF = 2,
	NODE_4 = 3,
	NODE_16 = 4,
	NODE_48 = 5,
	NODE_256 = 6,
	LEAF_INLINED = 7,
};
class FixedSizeAllocator;
class ART;
class Prefix;
class MetaBlockReader;
class MetaBlockWriter;

// structs
struct BlockPointer;
struct ARTFlags;
struct BufferPointer {
	//! The offset in a buffer
	uint32_t offset : 24;
	//! The buffer ID
	uint32_t buffer_id : 32;
};

//! The Node is the pointer class of the ART index.
//! If the swizzle flag is set, then the pointer points to a storage address (and has no type),
//! otherwise the pointer has a type and stores other information (e.g., a buffer location or a row ID).
class Node {
public:
	//! Node thresholds
	static constexpr uint8_t NODE_48_SHRINK_THRESHOLD = 12;
	static constexpr uint8_t NODE_256_SHRINK_THRESHOLD = 36;
	//! Node sizes
	static constexpr uint8_t NODE_4_CAPACITY = 4;
	static constexpr uint8_t NODE_16_CAPACITY = 16;
	static constexpr uint8_t NODE_48_CAPACITY = 48;
	static constexpr uint16_t NODE_256_CAPACITY = 256;
	//! Other constants
	static constexpr uint8_t EMPTY_MARKER = 48;
	static constexpr uint8_t LEAF_SIZE = 4;
	static constexpr uint8_t PREFIX_SIZE = 15;

public:
	//! Constructs an empty Node
	Node() : swizzle_flag(0), type(0) {};
	//! Constructs a swizzled Node from a block ID and an offset
	explicit Node(MetaBlockReader &reader);
	//! Constructs a non-swizzled Node from a buffer ID and an offset
	Node(uint32_t offset, uint32_t buffer_id) : swizzle_flag(0), type(0) {
		data.node_ptr.buffer_id = buffer_id;
		data.node_ptr.offset = offset;
	};

	//! The swizzle flag, set if swizzled, not set otherwise
	uint8_t swizzle_flag : 1;
	//! The node type
	uint8_t type : 7;
	//! Depending on the type, this is either a BufferPointer or an inlined row ID
	union {
		BufferPointer node_ptr;
		uint64_t row_id : 56;
	} data;

public:
	//! Comparison operator
	inline bool operator==(const Node &node) const {
		return swizzle_flag == node.swizzle_flag && type == node.type &&
		       data.node_ptr.offset == node.data.node_ptr.offset &&
		       data.node_ptr.buffer_id == node.data.node_ptr.buffer_id;
	}
	//! Returns the swizzle flag
	inline bool IsSwizzled() const {
		return swizzle_flag;
	}
	//! Returns true, if neither the swizzle flag nor the info is set, and false otherwise
	inline bool IsSet() const {
		return swizzle_flag || type;
	}
	//! Reset the Node pointer
	inline void Reset() {
		swizzle_flag = 0;
		type = 0;
	}
	//! Retrieve the node type from the pointer info
	inline NType DecodeNodeType() const {
		D_ASSERT(!IsSwizzled());
		D_ASSERT(type >= (uint8_t)NType::PREFIX);
		D_ASSERT(type <= (uint8_t)NType::LEAF_INLINED);
		return NType(type);
	}

	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static void New(ART &art, Node &node, const NType type);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);

	//! Replace the child node at the respective byte
	void ReplaceChild(const ART &art, const uint8_t byte, const Node child);
	//! Insert the child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at the respective byte
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte);

	//! Get the child for the respective byte in the node
	optional_ptr<Node> GetChild(ART &art, const uint8_t byte) const;
	//! Get the first child that is greater or equal to the specific byte
	optional_ptr<Node> GetNextChild(ART &art, uint8_t &byte, const bool deserialize = true) const;

	//! Serialize the node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize the node
	void Deserialize(ART &art);

	//! Returns the string representation of the node, or only traverses and verifies the node and its subtree
	string VerifyAndToString(ART &art, const bool only_verify);
	//! Returns the capacity of the node
	idx_t GetCapacity() const;
	//! Returns the matching node type for a given count
	static NType GetARTNodeTypeByCount(const idx_t count);
	//! Get references to the different allocators
	static FixedSizeAllocator &GetAllocator(const ART &art, NType type);

	//! Initializes a merge by fully deserializing the subtree of the node and incrementing its buffer IDs
	void InitializeMerge(ART &art, const ARTFlags &flags);
	//! Merge another node into this node
	bool Merge(ART &art, Node &other);
	//! Merge two nodes by first resolving their prefixes
	bool ResolvePrefixes(ART &art, Node &other);
	//! Merge two nodes that have no prefix or the same prefix
	bool MergeInternal(ART &art, Node &other);

	//! Vacuum all nodes that exceed their respective vacuum thresholds
	void Vacuum(ART &art, const ARTFlags &flags);
};

} // namespace duckdb
