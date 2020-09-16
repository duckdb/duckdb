//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/morsel_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/table/segment_base.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

enum class VersionNodeType : uint8_t {
	VERSION_NODE_LEAF,
	VERSION_NODE_INTERNAL
};

struct VersionNode {
	VersionNode(VersionNodeType type) : type(type) {}
	virtual ~VersionNode() {}

	VersionNodeType type;
};

class MorselInfo : public SegmentBase {
public:
	constexpr static idx_t MORSEL_VECTOR_COUNT = 100;
	constexpr static idx_t MORSEL_SIZE = STANDARD_VECTOR_SIZE * MORSEL_VECTOR_COUNT;

	constexpr static idx_t MORSEL_LAYER_COUNT = 10;
	constexpr static idx_t MORSEL_LAYER_SIZE = MORSEL_SIZE / MORSEL_LAYER_COUNT;
public:
	MorselInfo(idx_t start, idx_t count) : SegmentBase(start, count) {}

	unique_ptr<VersionNode> root;
public:
	idx_t GetSelVector(Transaction &transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);

	//! For a specific row, returns true if it should be used for the transaction and false otherwise.
	bool Fetch(Transaction &transaction, idx_t row);

	//! Append count rows to the morsel info
	void Append(Transaction &transaction, idx_t start, idx_t count, transaction_t commit_id);
private:
	ChunkInfo *GetChunkInfo(idx_t vector_idx);
private:
	mutex morsel_lock;
};

struct VersionNodeInternal : public VersionNode {
	VersionNodeInternal() : VersionNode(VersionNodeType::VERSION_NODE_INTERNAL) {}

	unique_ptr<VersionNode> children[MorselInfo::MORSEL_LAYER_COUNT];
};

struct VersionNodeLeaf : public VersionNode {
	VersionNodeLeaf() : VersionNode(VersionNodeType::VERSION_NODE_LEAF) {}

	unique_ptr<ChunkInfo> info;
};

}
