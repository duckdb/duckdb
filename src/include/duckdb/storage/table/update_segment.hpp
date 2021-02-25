//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/morsel_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/morsel_info.hpp"
#include "duckdb/storage/table/segment_base.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class DataTable;
class Vector;
struct UpdateNode;

class BaseUpdateInfo : public SegmentBase {
public:
	static constexpr const idx_t MORSEL_VECTOR_COUNT = MorselInfo::MORSEL_VECTOR_COUNT;
	static constexpr const idx_t MORSEL_SIZE = MorselInfo::MORSEL_SIZE;

	static constexpr const idx_t MORSEL_LAYER_COUNT = MorselInfo::MORSEL_LAYER_COUNT;
	static constexpr const idx_t MORSEL_LAYER_SIZE = MorselInfo::MORSEL_LAYER_SIZE;

public:
	BaseUpdateInfo(idx_t start, idx_t count) : SegmentBase(start, count) {
	}

	unique_ptr<UpdateNode> root;

public:
	idx_t GetSelVector(Transaction &transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);

private:
	UpdateInfo *GetUpdateInfo(idx_t vector_idx);

private:
	mutex morsel_lock;
};

struct UpdateNode {
	unique_ptr<UpdateInfo> info[BaseUpdateInfo::MORSEL_VECTOR_COUNT];
};

} // namespace duckdb
