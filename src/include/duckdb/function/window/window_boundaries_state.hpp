//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_boundaries_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/function/window_function.hpp"

namespace duckdb {

class BoundWindowExpression;
class WindowExecutorGlobalState;

struct WindowInputExpression {
	WindowInputExpression(DataChunk &chunk, column_t col_idx)
	    : ptype(PhysicalType::INVALID), scalar(true), chunk(chunk), col_idx(col_idx) {
		if (col_idx < chunk.data.size()) {
			auto &col = chunk.data[col_idx];
			ptype = col.GetType().InternalType();
			scalar = (col.GetVectorType() == VectorType::CONSTANT_VECTOR);
			if (!scalar && col.GetVectorType() != VectorType::FLAT_VECTOR) {
				col.Flatten(chunk.size());
			}
		}
	}

	inline PhysicalType InternalType() const {
		return ptype;
	}

	template <typename T>
	inline T GetCell(idx_t i) const {
		D_ASSERT(!chunk.data.empty());
		const auto data = FlatVector::GetData<T>(chunk.data[col_idx]);
		return data[scalar ? 0 : i];
	}

	inline bool CellIsNull(idx_t i) const {
		D_ASSERT(!chunk.data.empty());
		auto &col = chunk.data[col_idx];

		if (scalar) {
			return ConstantVector::IsNull(col);
		}
		return FlatVector::IsNull(col, i);
	}

	inline void CopyCell(Vector &target, idx_t target_offset, idx_t width = 1) const {
		D_ASSERT(!chunk.data.empty());
		auto &source = chunk.data[col_idx];
		auto source_offset = scalar ? 0 : target_offset;
		VectorOperations::Copy(source, target, source_offset + width, source_offset, target_offset);
	}

private:
	PhysicalType ptype;
	bool scalar;
	DataChunk &chunk;
	const column_t col_idx;
};

class WindowBoundariesState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	static bool HasPrecedingRange(const BoundWindowExpression &wexpr);
	static bool HasFollowingRange(const BoundWindowExpression &wexpr);
	static void AddImpliedBounds(WindowBoundsSet &bounds, const BoundWindowExpression &wexpr);
	static idx_t FindNextStart(const ValidityMask &mask, idx_t l, const idx_t r, idx_t &n);
	static idx_t FindPrevStart(const ValidityMask &mask, const idx_t l, idx_t r, idx_t &n);
	static void ClampFrame(const idx_t count, idx_t *values, const idx_t *begin, const idx_t *end) {
		for (idx_t i = 0; i < count; ++i) {
			values[i] = MinValue(MaxValue(values[i], begin[i]), end[i]);
		}
	}

	WindowBoundariesState(ExecutionContext &context, const WindowExecutorGlobalState &gstate);

	void Finalize(CollectionPtr collection);
	void UpdateBounds(idx_t row_idx, DataChunk &eval_chunk);

	//! The requested bounds.
	DataChunk bounds;
	//! The state used for reading the range collection
	unique_ptr<WindowCursor> range;

private:
	// Generate the partition start indices
	void PartitionBegin(idx_t row_idx, const idx_t count, bool is_jump);
	void PartitionEnd(idx_t row_idx, const idx_t count, bool is_jump);
	void PeerBegin(idx_t row_idx, const idx_t count, bool is_jump);
	void PeerEnd(idx_t row_idx, const idx_t count);
	void ValidBegin(idx_t row_idx, const idx_t count, bool is_jump);
	void ValidEnd(idx_t row_idx, const idx_t count, bool is_jump);
	void FrameBegin(idx_t row_idx, const idx_t count, WindowInputExpression &boundary_begin);
	void FrameEnd(idx_t row_idx, const idx_t count, WindowInputExpression &boundary_end);

	// Frame management
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;

	// Cached lookups
	WindowBoundsSet required;
	const ExpressionType type;
	const idx_t input_size;
	const WindowBoundary start_boundary;
	const WindowBoundary end_boundary;
	const idx_t boundary_start_idx;
	const idx_t boundary_end_idx;
	const size_t partition_count;
	const size_t order_count;
	const OrderType range_sense;
	const bool has_preceding_range;
	const bool has_following_range;
	const idx_t range_idx;

	// Carried between chunks
	idx_t next_pos = 0;
	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t valid_start = 0;
	idx_t valid_end = 0;

	FrameBounds prev;

	// Extra range cursor
	optional_ptr<WindowCursor> range_lo;
	unique_ptr<WindowCursor> range_hi;
};

} // namespace duckdb
