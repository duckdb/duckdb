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
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

// classes
enum class ARTNodeType : uint8_t { NLeaf = 0, N4 = 1, N16 = 2, N48 = 3, N256 = 4 };
class ART;
class ARTNode;
class Prefix;

// structs
struct MergeInfo {
	MergeInfo(ART *l_art, ART *r_art, ART *root_l_art, ART *root_r_art, ARTNode *&l_node, ARTNode *&r_node)
	    : l_art(l_art), r_art(r_art), root_l_art(root_l_art), root_r_art(root_r_art), l_node(l_node), r_node(r_node) {};
	ART *l_art;
	ART *r_art;
	ART *root_l_art;
	ART *root_r_art;
	ARTNode *&l_node;
	ARTNode *&r_node;
};
struct ParentsOfARTNodes {
	ParentsOfARTNodes(ARTNode *&l_parent, idx_t l_pos, ARTNode *&r_parent, idx_t r_pos)
	    : l_parent(l_parent), l_pos(l_pos), r_parent(r_parent), r_pos(r_pos) {};
	ARTNode *&l_parent;
	idx_t l_pos;
	ARTNode *&r_parent;
	idx_t r_pos;
};

//! The ARTNode is the swizzleable pointer class of the ART index.
//! If the ARTNode pointer is not swizzled, then the leftmost byte identifies the ARTNodeType.
//! The remaining bytes are the position in the respective ART buffer.
class ARTNode : public SwizzleablePointer {
public:
	// constants (this allows testing performance with different ART node sizes)
	// node prefixes (NOTE: this should always hold: PREFIX_SEGMENT_SIZE >= PREFIX_INLINE_BYTES)
	static constexpr uint32_t PREFIX_INLINE_BYTES = 8;
	static constexpr uint32_t PREFIX_SEGMENT_SIZE = 32;
	// node thresholds
	static constexpr uint8_t NODE_48_SHRINK_THRESHOLD = 12;
	static constexpr uint8_t NODE_256_SHRINK_THRESHOLD = 36;
	// node sizes
	static constexpr uint16_t NODE_4_CAPACITY = 4;
	static constexpr uint16_t NODE_16_CAPACITY = 16;
	static constexpr uint16_t NODE_48_CAPACITY = 48;
	static constexpr uint16_t NODE_256_CAPACITY = 256;
	// others
	static constexpr uint8_t EMPTY_MARKER = 48;
	static constexpr uint32_t LEAF_SEGMENT_SIZE = 8;

public:
	//! Constructs an empty ARTNode
	ARTNode();
	//! Constructs a swizzled pointer from a block ID and an offset
	explicit ARTNode(MetaBlockReader &reader);
	//! Get a new pointer to a node, might cause a new buffer allocation
	static ARTNode New(ART &art, const ARTNodeType &type);
	//! Free the node (and its subtree)
	static void Free(ART &art, ARTNode &node);
	//! Initializes a new ART node
	static void Initialize(ART &art, ARTNode &node, const ARTNodeType &type);

	//! Get the node
	template <class T>
	inline T *Get(FixedSizeAllocator &allocator) const;

	//! Set the leftmost byte to contain the node type
	void EncodeARTNodeType(const ARTNodeType &type);
	//! Retrieve the node type from the leftmost byte
	ARTNodeType DecodeARTNodeType() const;

	//! Replace a child node at pos
	void ReplaceChild(ART &art, const idx_t &pos, ARTNode &child);
	//! Insert a child node at byte
	static void InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child);
	//! Delete the child node at pos
	static void DeleteChild(ART &art, ARTNode &node, idx_t pos);

	//! Get the child at the specified position in the node. pos must be between [0, count)
	ARTNode GetChild(ART &art, const idx_t &pos) const;
	//! Get the byte at the specified position
	uint8_t GetKeyByte(ART &art, const idx_t &pos) const;
	//! Get the position of a child corresponding exactly to the specific byte, returns DConstants::INVALID_INDEX if
	//! the child does not exist
	idx_t GetChildPos(ART &art, const uint8_t &byte) const;
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	idx_t GetChildPosGreaterEqual(ART &art, const uint8_t &byte, bool &inclusive) const;
	//! Get the position of the minimum child node in the node
	idx_t GetMinPos(ART &art) const;
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position. If pos ==
	//! DConstants::INVALID_INDEX, then the first valid position in the node is returned
	idx_t GetNextPos(ART &art, idx_t pos) const;
	//! Get the next position and byte in the node, or DConstants::INVALID_INDEX if there is no next position. If pos ==
	//! DConstants::INVALID_INDEX, then the first valid position and byte in the node are returned
	idx_t GetNextPosAndByte(ART &art, idx_t pos, uint8_t &byte) const;

	//! Serialize an ART node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize this node
	void Deserialize(ART &art, idx_t block_id, idx_t offset);

	//! Returns the string representation of a node
	string ToString(ART &art) const;
	//! Returns the capacity of a node
	idx_t GetCapacity() const;
	//! Returns a pointer to the prefix of a node
	Prefix *GetPrefix(ART &art) const;
	//! Returns the matching node type for a given count
	static ARTNodeType GetARTNodeTypeByCount(const idx_t &count);

	//! Merge two ARTs
	static bool MergeARTs(ART *l_art, ART *r_art);

	//! Returns whether the ART node is in-memory
	bool InMemory();
	//! Returns the size of the ART node in-memory and its subtree
	idx_t MemorySize(ART &art, const bool &recurse);
	//! Returns whether the child at pos is in-memory
	bool ChildIsInMemory(ART &art, const idx_t &pos);
};

} // namespace duckdb
