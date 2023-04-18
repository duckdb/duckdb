//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

namespace duckdb {

// classes
enum class NType : uint8_t {
	PREFIX_SEGMENT = 1,
	LEAF_SEGMENT = 2,
	LEAF = 3,
	NODE_4 = 4,
	NODE_16 = 5,
	NODE_48 = 6,
	NODE_256 = 7
};
class ART;
class Node;
class Prefix;
class MetaBlockReader;
class MetaBlockWriter;

// structs
struct BlockPointer;
struct ARTFlags;

//! The ARTNode is the swizzleable pointer class of the ART index.
//! If the ARTNode pointer is not swizzled, then the leftmost byte identifies the NType.
//! The remaining bytes are the position in the respective ART buffer.
class Node : public SwizzleablePointer {
public:
	// constants (this allows testing performance with different ART node sizes)

	//! Node prefixes (NOTE: this should always hold: PREFIX_SEGMENT_SIZE >= PREFIX_INLINE_BYTES)
	static constexpr uint32_t PREFIX_INLINE_BYTES = 8;
	static constexpr uint32_t PREFIX_SEGMENT_SIZE = 32;
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
	static constexpr uint32_t LEAF_SEGMENT_SIZE = 8;

public:
	//! Constructs an empty ARTNode
	Node();
	//! Constructs a swizzled pointer from a block ID and an offset
	explicit Node(MetaBlockReader &reader);
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static void New(ART &art, Node &node, const NType type);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);

	//! Retrieve the node type from the leftmost byte
	inline NType DecodeARTNodeType() const {
		return NType(type);
	}

	//! Set the pointer
	inline void SetPtr(const SwizzleablePointer ptr) {
		offset = ptr.offset;
		buffer_id = ptr.buffer_id;
	}

	//! Replace the child node at the respective byte
	void ReplaceChild(const ART &art, const uint8_t byte, const Node child);
	//! Insert the child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at the respective byte
	static void DeleteChild(ART &art, Node &node, const uint8_t byte);

	//! Get the child for the respective byte in the node
	optional_ptr<Node> GetChild(ART &art, const uint8_t byte) const;
	//! Get the first child that is greater or equal to the specific byte
	optional_ptr<Node> GetNextChild(ART &art, uint8_t &byte) const;

	//! Serialize the node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize the node
	void Deserialize(ART &art);

	//! Returns the string representation of the node
	string ToString(ART &art) const;
	//! Returns the capacity of the node
	idx_t GetCapacity() const;
	//! Returns a pointer to the prefix of the node
	Prefix &GetPrefix(ART &art);
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
	static void Vacuum(ART &art, Node &node, const ARTFlags &flags);
};

} // namespace duckdb
