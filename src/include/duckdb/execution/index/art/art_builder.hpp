//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_builder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/stack.hpp"

namespace duckdb {

class ARTBuilder {
public:
	ARTBuilder() = delete;
	ARTBuilder(ArenaAllocator &arena, ART &art, const unsafe_vector<ARTKey> &keys, const unsafe_vector<ARTKey> &row_ids)
	    : arena(arena), art(art), keys(keys), row_ids(row_ids) {
	}

public:
	//! Initialize the ART builder with the root, which the first entry writes into.
	void Init(NodePtr &node_ptr, const idx_t end) {
		root_ptr = node_ptr;
		s.emplace(NodePtr(), 0, 0, end, 0);
	}
	//! Build the ART starting at the first entry in the stack.
	ARTConflictType Build();

private:
	struct NodeEntry {
		NodeEntry() = delete;
		NodeEntry(const NodePtr parent_ptr, const uint8_t byte, const idx_t start, const idx_t end, const idx_t depth)
		    : parent_ptr(parent_ptr), byte(byte), start(start), end(end), depth(depth) {};

		//! The parent node, or an empty NodePtr for the root entry.
		NodePtr parent_ptr;
		//! The byte of the parent's empty child that receives this entry's subtree.
		uint8_t byte;
		idx_t start;
		idx_t end;
		idx_t depth;
	};

private:
	//! Write the subtree's NodePtr into the entry's empty child, or into the root.
	void SetChild(const NodeEntry &entry, const NodePtr node_ptr);

private:
	//! The arena holds any temporary memory allocated during the Build phase.
	ArenaAllocator &arena;
	//! The ART holding the node memory.
	ART &art;
	//! The keys to build the ART from.
	const unsafe_vector<ARTKey> &keys;
	//! The row IDs matching the keys.
	const unsafe_vector<ARTKey> &row_ids;
	//! The root of the ART.
	optional_ptr<NodePtr> root_ptr;
	//! The stack. While merging, NodeEntry elements are pushed onto of the stack.
	stack<NodeEntry> s;
};

} // namespace duckdb
