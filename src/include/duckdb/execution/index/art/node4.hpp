//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node4.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class Node4 : public Node {
public:
	explicit Node4(size_t compression_length);

	uint8_t key[4];
	// Pointers to the deserialized nodes
	unique_ptr<Node> child[4];
	// Block offsets
	std::pair<idx_t, idx_t> block_offsets[4];

public:
	//! Get position of a byte, returns -1 if not exists
	idx_t GetChildPos(uint8_t k) override;
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	idx_t GetChildGreaterEqual(uint8_t k, bool &equal) override;
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position
	idx_t GetNextPos(idx_t pos) override;
	//! Get Node4 Child
	unique_ptr<Node> *GetChild(idx_t pos) override;

	idx_t GetMin() override;

	//! Insert Leaf to the Node4
	static void Insert(unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child);
	//! Remove Leaf from Node4
	static void Erase(unique_ptr<Node> &node, int pos);

	//! Serialize Node
	std::pair<idx_t, idx_t> Serialize(duckdb::MetaBlockWriter &writer) override;
	static unique_ptr<Node4> Deserialize(duckdb::Deserializer &source);
};
} // namespace duckdb
