//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node256.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class Node256 : public Node {
public:
	explicit Node256(size_t compression_length);

	unique_ptr<Node> child[256];

	// Block offsets
	std::pair<idx_t, idx_t> block_offsets[256];

public:
	//! Get position of a specific byte, returns DConstants::INVALID_INDEX if not exists
	idx_t GetChildPos(uint8_t k) override;
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	idx_t GetChildGreaterEqual(uint8_t k, bool &equal) override;
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position
	idx_t GetNextPos(idx_t pos) override;
	//! Get Node256 Child
	unique_ptr<Node> *GetChild(ART &art, idx_t pos) override;

	idx_t GetMin() override;

	//! Insert node From Node256
	static void Insert(unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child);

	//! Shrink to node 48
	static void Erase(unique_ptr<Node> &node, int pos);

	//! Serialize Node
	std::pair<idx_t, idx_t> Serialize(duckdb::MetaBlockWriter &writer) override;

	static unique_ptr<Node256> Deserialize(duckdb::MetaBlockReader &source);
};
} // namespace duckdb
