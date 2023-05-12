//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

// classes
class ARTKey;

// TODO: remove
//// structs
////! This struct contains all information for splitting prefixes. If match is true, then we don't
////! need to split: after_node is not set, and neither are the bytes. Otherwise, before_node is a
////! reference to the first node that follows the split (usually a Node4), and after_node is the
////! start of all remaining prefix bytes and their next node(s). prefix_split_byte holds the mismatching
////! byte of the prefix, and other_split_byte holds the mismatching byte of the key/other prefix.
// struct PrefixSplitInfo {
//	Node &before_node;
//	Node &after_node;
//	uint8_t prefix_split_byte;
//	uint8_t other_split_byte;
//	bool match;
// };

//! The Prefix is a special node type that contains eight bytes (1 byte count,
//! 7 bytes of prefix data) and a Node pointer. This pointer either points to another prefix
//! node or the 'actual' ART node.
class Prefix {
public:
	//! A count and up to seven bytes of prefix data
	uint8_t data[Node::PREFIX_SIZE];
	//! A pointer to the next ART node
	Node ptr;

public:
	//! Get a new chain of prefix nodes, might cause new buffer allocations,
	//! with the node parameter holding the tail of the chain
	static void New(ART &art, Node &node, const ARTKey &key, const uint32_t depth, uint32_t count);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);
	//! Get a reference to the prefix
	static inline Prefix &Get(const ART &art, const Node ptr) {
		return *Node::GetAllocator(art, NType::PREFIX).Get<Prefix>(ptr);
	}

	//! Initializes a merge by incrementing the buffer ID of the child node
	inline void InitializeMerge(ART &art, const ARTFlags &flags) {
		ptr.InitializeMerge(art, flags);
	}

	//! Appends a byte and a child_prefix to prefix. If there is no prefix, than it pushes the
	//! byte on top of child_prefix. If there is no child_prefix, then it creates a new
	//! prefix node containing that byte
	static void Concatenate(ART &art, Node &prefix, const uint8_t byte, Node &child_prefix);
	//! Traverse prefixes until (1) encountering two non-prefix nodes,
	//! (2) encountering one non-prefix node, (3) encountering a mismatching byte.
	//! Also frees all fully traversed r_node prefixes
	static idx_t Traverse(const ART &art, reference<Node> &l_node, reference<Node> &r_node);
	//! Returns the byte at position
	static uint8_t GetByte(const ART &art, const Node &prefix, const idx_t position);
	//! Removes the first n bytes from the prefix and shifts all subsequent bytes in the
	//! prefix node(s) by n
	static void Reduce(ART &art, Node &prefix, const idx_t n);
	//! Splits the prefix at position. prefix then references the ptr (if any bytes left before
	//! the split), or stays unchanged (no bytes left before the split). child references
	//! the node after the split, which is either a new Prefix node, or ptr
	static void Split(ART &art, reference<Node> &prefix, Node &child, idx_t position);

	//! Serialize this node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize this node
	void Deserialize(MetaBlockReader &reader);

	//! Vacuum the child of the node
	inline void Vacuum(ART &art, const ARTFlags &flags) {
		Node::Vacuum(art, ptr, flags);
	}

	//	static bool FindMismatchPosition(const ART &art, Node &node, const ARTKey &key,
	//	                                const uint32_t depth, idx_t &mismatch_position);
	//	static uint8_t Split(ART &art, Node &node, Node &remaining_node, const idx_t compare_count);
	//

	//	//! Returns PrefixSplitInfo and splits a prefix, if necessary. Compares the prefix to bytes of a key.
	//	static PrefixSplitInfo SplitPrefix(const ART &art, Node &before_node, const ARTKey &key,
	//	                                   const uint32_t depth);
	//	//! Returns PrefixSplitInfo and splits a prefix, if necessary. Compares the prefix to bytes of another prefix.
	//	static PrefixSplitInfo SplitPrefix(const ART &art, Node &before_node, const Prefix &other);
};

} // namespace duckdb
