//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/typedefs.hpp"

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

//! The Node is the pointer class of the ART index.
//! If the node is serialized, then the pointer points to a storage address (and has no type),
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
	//! Bit-shifting
	static constexpr uint8_t TYPE_SIZE = 7;
	static constexpr uint8_t RESET_SERIALIZED_FLAG = 127;
	static constexpr uint8_t SET_SERIALIZED_FLAG = 128;
	static constexpr uint8_t BUFFER_ID_SIZE = 32;
	static constexpr uint64_t OFFSET_TO_ZERO = 0x00000000FFFFFFFF;
	//! Other constants
	static constexpr uint8_t EMPTY_MARKER = 48;
	static constexpr uint8_t LEAF_SIZE = 4;
	static constexpr uint8_t PREFIX_SIZE = 15;

public:
	//! Constructs an empty Node
	Node() : info(0) {};
	//! Constructs a serialized Node pointer from a block ID and an offset
	explicit Node(MetaBlockReader &reader);
	//! Constructs an in-memory Node from a buffer ID and an offset
	Node(uint32_t buffer_id, uint32_t offset) : info(0) {
		SetPtr(buffer_id, offset);
	};

	//! The serialized flag (first bit) and the node type
	//! NOTE: We combine this into one uint8_t because Windows will otherwise allocate
	//! 2 * sizeof(uint64_t) for a Node instead of sizeof(uint64_t) bytes
	//! https://learn.microsoft.com/en-us/cpp/cpp/cpp-bit-fields?view=msvc-170
	uint8_t info : 8;
	//! Depending on the type, this is either a buffer/block pointer or an inlined row ID
	uint64_t data : 56;

public:
	//! Get the block/buffer ID
	inline idx_t GetBufferId() const {
		return data & Node::OFFSET_TO_ZERO;
	}
	//! Get the offset
	inline idx_t GetOffset() const {
		return (data >> Node::BUFFER_ID_SIZE);
	}
	//! Set the block/buffer ID and offset
	inline void SetPtr(const uint32_t buffer_id, const uint32_t offset) {
		data = offset;
		data <<= Node::BUFFER_ID_SIZE;
		data += buffer_id;
	}

	//! Get the type
	inline NType GetType() const {
		D_ASSERT(!IsSerialized());
		auto type = info & Node::RESET_SERIALIZED_FLAG;
		D_ASSERT(type >= (uint8_t)NType::PREFIX);
		D_ASSERT(type <= (uint8_t)NType::LEAF_INLINED);
		return NType(type);
	}
	//! Set the type
	inline void SetType(const uint8_t type) {
		info += type;
	}
	//! Returns whether the node is serialized or not
	inline bool IsSerialized() const {
		return info >> Node::TYPE_SIZE;
	}
	//! Set the serialized flag
	inline void SetSerialized() {
		info |= Node::SET_SERIALIZED_FLAG;
	}
	//! Returns true, if the node is not zero
	inline bool IsSet() const {
		return info;
	}
	//! Reset the Node pointer by setting the node info to zero
	inline void Reset() {
		info = 0;
	}

	//! Comparison operator
	inline bool operator==(const Node &node) const {
		return info == node.info && data == node.data;
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

static_assert(sizeof(Node) == sizeof(uint64_t), "Invalid size for Node type.");

} // namespace duckdb
