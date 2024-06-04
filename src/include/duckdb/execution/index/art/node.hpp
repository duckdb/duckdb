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
#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"

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

class ART;
class Prefix;
class MetadataReader;
class MetadataWriter;

// structs
struct BlockPointer;
struct ARTFlags;
struct MetaBlockPointer;

//! The Node is the pointer class of the ART index.
//! It inherits from the IndexPointer, and adds ART-specific functionality
class Node : public IndexPointer {
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
	static constexpr idx_t AND_ROW_ID = 0x00FFFFFFFFFFFFFF;

public:
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static void New(ART &art, Node &node, const NType type);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);

	//! Get references to the allocator
	static FixedSizeAllocator &GetAllocator(const ART &art, const NType type);
	//! Get a (immutable) reference to the node. If dirty is false, then T should be a const class
	template <class NODE>
	static inline const NODE &Ref(const ART &art, const Node ptr, const NType type) {
		return *(GetAllocator(art, type).Get<const NODE>(ptr, false));
	}
	//! Get a (const) reference to the node. If dirty is false, then T should be a const class
	template <class NODE>
	static inline NODE &RefMutable(const ART &art, const Node ptr, const NType type) {
		return *(GetAllocator(art, type).Get<NODE>(ptr));
	}

	//! Replace the child node at byte
	void ReplaceChild(const ART &art, const uint8_t byte, const Node child) const;
	//! Insert the child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at byte
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte);

	//! Get the child (immutable) for the respective byte in the node
	optional_ptr<const Node> GetChild(ART &art, const uint8_t byte) const;
	//! Get the child for the respective byte in the node
	optional_ptr<Node> GetChildMutable(ART &art, const uint8_t byte) const;
	//! Get the first child (immutable) that is greater or equal to the specific byte
	optional_ptr<const Node> GetNextChild(ART &art, uint8_t &byte) const;
	//! Get the first child that is greater or equal to the specific byte
	optional_ptr<Node> GetNextChildMutable(ART &art, uint8_t &byte) const;

	//! Returns the string representation of the node, or only traverses and verifies the node and its subtree
	string VerifyAndToString(ART &art, const bool only_verify) const;
	//! Returns the capacity of the node
	idx_t GetCapacity() const;
	//! Returns the matching node type for a given count
	static NType GetARTNodeTypeByCount(const idx_t count);

	//! Initializes a merge by incrementing the buffer IDs of a node and its subtree
	void InitializeMerge(ART &art, const ARTFlags &flags);
	//! Merge another node into this node
	bool Merge(ART &art, Node &other);
	//! Merge two nodes by first resolving their prefixes
	bool ResolvePrefixes(ART &art, Node &other);
	//! Merge two nodes that have no prefix or the same prefix
	bool MergeInternal(ART &art, Node &other);

	//! Vacuum all nodes that exceed their respective vacuum thresholds
	void Vacuum(ART &art, const ARTFlags &flags);

	//! Get the row ID (8th to 63rd bit)
	inline row_t GetRowId() const {
		return Get() & AND_ROW_ID;
	}
	//! Set the row ID (8th to 63rd bit)
	inline void SetRowId(const row_t row_id) {
		Set((Get() & AND_METADATA) | UnsafeNumericCast<idx_t>(row_id));
	}

	//! Returns the type of the node, which is held in the metadata
	inline NType GetType() const {
		return NType(GetMetadata());
	}

	//! Assign operator
	inline void operator=(const IndexPointer &ptr) {
		Set(ptr.Get());
	}
};
} // namespace duckdb
