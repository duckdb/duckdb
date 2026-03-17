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

namespace duckdb {

class BoundWindowExpression;

//	Column indexes of the bounds chunk
enum WindowBounds : uint8_t {
	PARTITION_BEGIN,
	PARTITION_END,
	PEER_BEGIN,
	PEER_END,
	VALID_BEGIN,
	VALID_END,
	FRAME_BEGIN,
	FRAME_END
};

// C++ 11 won't do this automatically...
struct WindowBoundsHash {
	inline uint64_t operator()(const WindowBounds &value) const {
		return value;
	}
};

using WindowBoundsSet = unordered_set<WindowBounds, WindowBoundsHash>;

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

struct WindowBoundariesState {
	static bool HasPrecedingRange(const BoundWindowExpression &wexpr);
	static bool HasFollowingRange(const BoundWindowExpression &wexpr);
	static WindowBoundsSet GetWindowBounds(const BoundWindowExpression &wexpr);
	static idx_t FindNextStart(const ValidityMask &mask, idx_t l, const idx_t r, idx_t &n);
	static idx_t FindPrevStart(const ValidityMask &mask, const idx_t l, idx_t r, idx_t &n);

	WindowBoundariesState(const BoundWindowExpression &wexpr, const idx_t input_size);

	// Generate the partition start indices
	void PartitionBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
	                    const ValidityMask &partition_mask);
	void PartitionEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
	                  const ValidityMask &partition_mask);
	void PeerBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
	               const ValidityMask &partition_mask, const ValidityMask &order_mask);
	void PeerEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, const ValidityMask &partition_mask,
	             const ValidityMask &order_mask);
	void ValidBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
	                const ValidityMask &partition_mask, const ValidityMask &order_mask,
	                optional_ptr<WindowCursor> range);
	void ValidEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump, const ValidityMask &partition_mask,
	              const ValidityMask &order_mask, optional_ptr<WindowCursor> range);
	void FrameBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, WindowInputExpression &boundary_begin,
	                const ValidityMask &order_mask, optional_ptr<WindowCursor> range);
	void FrameEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, WindowInputExpression &boundary_end,
	              const ValidityMask &order_mask, optional_ptr<WindowCursor> range);

	static void ClampFrame(const idx_t count, idx_t *values, const idx_t *begin, const idx_t *end) {
		for (idx_t i = 0; i < count; ++i) {
			values[i] = MinValue(MaxValue(values[i], begin[i]), end[i]);
		}
	}

	void Bounds(DataChunk &bounds, idx_t row_idx, optional_ptr<WindowCursor> range, const idx_t count,
	            WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
	            const ValidityMask &partition_mask, const ValidityMask &order_mask);

	// Cached lookups
	WindowBoundsSet required;
	const ExpressionType type;
	const idx_t input_size;
	const WindowBoundary start_boundary;
	const WindowBoundary end_boundary;
	const size_t partition_count;
	const size_t order_count;
	const OrderType range_sense;
	const bool has_preceding_range;
	const bool has_following_range;

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
