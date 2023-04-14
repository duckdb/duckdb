//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/iterator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/stack.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

struct IteratorEntry {
	IteratorEntry() {
	}
	IteratorEntry(Node node, uint8_t byte) : node(node), byte(byte) {
	}

	Node node;
	uint8_t byte = 0;
};

//! Keeps track of the current key in the iterator
class IteratorCurrentKey {
public:
	//! Push byte into current key
	void Push(const uint8_t key);
	//! Pops n elements from the key
	void Pop(const idx_t n);

	//! Subscript operator
	uint8_t &operator[](idx_t idx);
	//! Greater than operator
	bool operator>(const ARTKey &k) const;
	//! Greater than or equal to operator
	bool operator>=(const ARTKey &k) const;
	//! Equal to operator
	bool operator==(const ARTKey &k) const;

private:
	//! The current key position
	idx_t cur_key_pos = 0;
	//! The current key corresponding to the current leaf
	vector<uint8_t> key;
};

class Iterator {
public:
	//! All information about the current key
	IteratorCurrentKey cur_key;
	//! Pointer to the ART
	ART *art = nullptr;

	//! Scan the tree
	bool Scan(const ARTKey &key, const idx_t &max_count, vector<row_t> &result_ids, const bool &is_inclusive);
	//! Finds the minimum value of the tree
	void FindMinimum(Node &node);
	//! Goes to the lower bound of the tree
	bool LowerBound(Node node, const ARTKey &key, const bool &is_inclusive);

private:
	//! Stack of iterator entries
	stack<IteratorEntry> nodes;
	//! Last visited leaf
	Leaf *last_leaf = nullptr;

	//! Go to the next node
	bool Next();
	//! Push part of the key to the current key
	void PushKey(const Node &node, const uint8_t byte);
	//! Pop node from the stack of iterator entries
	void PopNode();
};
} // namespace duckdb
