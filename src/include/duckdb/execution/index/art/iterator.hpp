//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/iterator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/stack.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

struct IteratorEntry {
	IteratorEntry() {
	}
	IteratorEntry(Node *node, idx_t pos) : node(node), pos(pos) {
	}

	Node *node = nullptr;
	idx_t pos = 0;
};

//! Keeps track of the current key in the iterator
class IteratorCurrentKey {
public:
	//! Push Byte
	void Push(uint8_t key);
	//! Pops n elements
	void Pop(idx_t n);
	//! Subscript operator
	uint8_t &operator[](idx_t idx);
	bool operator>(const Key &k) const;
	bool operator>=(const Key &k) const;
	bool operator==(const Key &k) const;

private:
	//! The current key position
	idx_t cur_key_pos = 0;
	//! The current key of the Leaf Node
	vector<uint8_t> key;
};

class Iterator {
public:
	//! Current Key
	IteratorCurrentKey cur_key;
	//! Pointer to the ART tree we are iterating
	ART *art = nullptr;

	//! Scan the tree
	bool Scan(Key &bound, idx_t max_count, vector<row_t> &result_ids, bool is_inclusive);
	//! Finds minimum value of the tree
	void FindMinimum(Node &node);
	//! Goes to lower bound
	bool LowerBound(Node *node, Key &key, bool inclusive);

private:
	//! Stack of iterator entries
	stack<IteratorEntry> nodes;
	//! Last visited leaf
	Leaf *last_leaf = nullptr;
	//! Go to the next node
	bool Next();
	//! Push part of the key to cur_key
	void PushKey(Node *node, uint16_t pos);
	//! Pop node
	void PopNode();
};
} // namespace duckdb
