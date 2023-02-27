//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

// classes
class ART;

class Leaf {
public:
	//! Number of row IDs
	uint32_t count;
	//! Compressed path (prefix)
	Prefix prefix;
	union {
		//! The position to the head of the list of leaf segments
		idx_t position;
		//! Inlined row ID
		row_t inlined;
	} row_ids;

public:
	//! Initializes a leaf holding one row ID and a prefix starting at depth
	static Leaf *Initialize(ART &art, const ARTNode &node, const Key &key, const uint32_t &depth, const row_t &row_id);
	//! Initializes a leaf holding n_row_ids row IDs and a prefix starting at depth
	static Leaf *Initialize(ART &art, const ARTNode &node, const Key &key, const uint32_t &depth, const row_t *row_ids,
	                        const idx_t &count);

	//! Insert a row ID into a leaf
	void Insert(ART &art, const row_t &row_id);
	//! Remove a row ID from a leaf
	void Remove(ART &art, const row_t &row_id);

	//! Get the row ID at pos
	uint8_t GetRowID(ART &art, const idx_t &position) const;
	//! Returns the position of a row ID, and an invalid index, if the leaf does not contain the row ID
	idx_t FindRowID(ART &art, const row_t &row_id) const;

private:
	//! Returns whether this leaf is inlined
	bool IsInlined() const;
	//! Moves the inlined row ID onto a leaf segment, does not change the size
	//! so this will be an (temporarily) invalid leaf
	void MoveInlinedToSegment(ART &art);
};

} // namespace duckdb
