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
#include "duckdb/common/limits.hpp"

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
class MetadataReader;
class MetadataWriter;

// structs
struct BlockPointer;
struct ARTFlags;
struct MetaBlockPointer;

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
	static constexpr uint64_t SHIFT_OFFSET = 32;
	static constexpr uint64_t SHIFT_TYPE = 56;
	static constexpr uint64_t SHIFT_SERIALIZED_FLAG = 63;
	//! AND operations
	static constexpr uint64_t AND_OFFSET = 0x0000000000FFFFFF;
	static constexpr uint64_t AND_BUFFER_ID = 0x00000000FFFFFFFF;
	static constexpr uint64_t AND_IS_SET = 0xFF00000000000000;
	static constexpr uint64_t AND_RESET = 0x00FFFFFFFFFFFFFF;
	//! OR operations
	static constexpr uint64_t SET_SERIALIZED_FLAG = 0x8000000000000000;
	//! Other constants
	static constexpr uint8_t EMPTY_MARKER = 48;
	static constexpr uint8_t LEAF_SIZE = 4;
	static constexpr uint8_t PREFIX_SIZE = 15;

public:
	//! Constructors

	//! Constructs an empty Node
	Node() : data(0) {};
	//! Constructs a serialized Node pointer from a block ID and an offset
	explicit Node(MetadataReader &reader);
	//! Constructs an in-memory Node from a buffer ID and an offset
	Node(const uint32_t buffer_id, const uint32_t offset) : data(0) {
		SetPtr(buffer_id, offset);
	};

public:
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
	BlockPointer Serialize(ART &art, MetadataWriter &writer);
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

	// Getters and Setters

	//! Returns whether the node is serialized or not (zero bit)
	inline bool IsSerialized() const {
		return data >> Node::SHIFT_SERIALIZED_FLAG;
	}
	//! Get the type (1st to 7th bit)
	inline NType GetType() const {
		D_ASSERT(!IsSerialized());
		auto type = data >> Node::SHIFT_TYPE;
		D_ASSERT(type >= (uint8_t)NType::PREFIX);
		D_ASSERT(type <= (uint8_t)NType::LEAF_INLINED);
		return NType(type);
	}
	//! Get the offset (8th to 23rd bit)
	inline idx_t GetOffset() const {
		auto offset = data >> Node::SHIFT_OFFSET;
		return offset & Node::AND_OFFSET;
	}
	//! Get the block/buffer ID (24th to 63rd bit)
	inline idx_t GetBufferId() const {
		return data & Node::AND_BUFFER_ID;
	}
	//! Get the row ID (8th to 63rd bit)
	inline row_t GetRowId() const {
		return data & Node::AND_RESET;
	}

	//! Set the serialized flag (zero bit)
	inline void SetSerialized() {
		data &= Node::AND_RESET;
		data |= Node::SET_SERIALIZED_FLAG;
	}
	//! Set the type (1st to 7th bit)
	inline void SetType(const uint8_t type) {
		D_ASSERT(!IsSerialized());
		data += (uint64_t)type << Node::SHIFT_TYPE;
	}
	//! Set the block/buffer ID (24th to 63rd bit) and offset (8th to 23rd bit)
	inline void SetPtr(const uint32_t buffer_id, const uint32_t offset) {
		D_ASSERT(!(data & Node::AND_RESET));
		auto shifted_offset = ((uint64_t)offset) << Node::SHIFT_OFFSET;
		data += shifted_offset;
		data += buffer_id;
	}
	//! Set the row ID (8th to 63rd bit)
	inline void SetRowId(const row_t row_id) {
		D_ASSERT(!(data & Node::AND_RESET));
		data += row_id;
	}

	//! Returns true, if neither the serialized flag is set nor the type
	inline bool IsSet() const {
		return data & Node::AND_IS_SET;
	}
	//! Reset the Node pointer by setting the node info to zero
	inline void Reset() {
		data = 0;
	}

	//! Adds an idx_t to a buffer ID, the rightmost 32 bits contain the buffer ID
	inline void AddToBufferID(const idx_t summand) {
		D_ASSERT(summand < NumericLimits<uint32_t>().Maximum());
		data += summand;
	}

	//! Comparison operator
	inline bool operator==(const Node &node) const {
		return data == node.data;
	}

private:
	//! Data holds all the information contained in a Node pointer
	//! [0: serialized flag, 1 - 7: type,
	//! 8 - 23: offset, 24 - 63: buffer/block ID OR
	//! 8 - 63: row ID]
	//! NOTE: a Node pointer can be either serialized OR have a type
	//! NOTE: we do not use bit fields because when using bit fields Windows compiles
	//! the Node class into 16 bytes instead of the intended 8 bytes, doubling the
	//! space requirements
	//! https://learn.microsoft.com/en-us/cpp/cpp/cpp-bit-fields?view=msvc-170
	uint64_t data;
};

static_assert(sizeof(Node) == sizeof(uint64_t), "Invalid size for Node type.");

} // namespace duckdb
