//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

// classes
class ARTKey;

// structs
//! This struct contains all information for splitting prefixes. If match is true, then we don't
//! need to split: after_node is not set, and neither are the bytes. Otherwise, before_node is a
//! reference to the first node that follows the split (usually a Node4), and after_node is the
//! start of all remaining prefix bytes and their next node(s). prefix_split_byte holds the mismatching
//! byte of the prefix, and other_split_byte holds the mismatching byte of the key/other prefix.
struct PrefixSplitInfo {
	Node &before_node;
	Node &after_node;
	uint8_t prefix_split_byte;
	uint8_t other_split_byte;
	bool match;
};

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
	//! Get a new chain of prefix nodes, might cause a new buffer allocations,
	//! with the node parameter holding the tail of the chain
	static void New(ART &art, Node &node, const ARTKey &key, const uint32_t depth, uint32_t count);
	//! Get a reference to the prefix
	static inline Prefix &Get(const ART &art, const Node ptr) {
		return *Node::GetAllocator(art, NType::PREFIX).Get<Prefix>(ptr);
	}

	static bool FindMismatchPosition(const ART &art, Node &node, const ARTKey &key,
	                                const uint32_t depth, idx_t &compare_count);
	static uint8_t Split(ART &art, Node &node, Node &remaining_node, const idx_t compare_count);



	//! Returns PrefixSplitInfo and splits a prefix, if necessary. Compares the prefix to bytes of a key.
	static PrefixSplitInfo SplitPrefix(const ART &art, Node &before_node, const ARTKey &key,
	                                   const uint32_t depth);
	//! Returns PrefixSplitInfo and splits a prefix, if necessary. Compares the prefix to bytes of another prefix.
	static PrefixSplitInfo SplitPrefix(const ART &art, Node &before_node, const Prefix &other);
};

} // namespace duckdb
