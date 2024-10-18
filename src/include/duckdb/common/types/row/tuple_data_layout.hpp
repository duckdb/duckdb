//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/row/row_layout.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class TupleDataLayout {
public:
	using Aggregates = vector<AggregateObject>;
	using ValidityBytes = TemplatedValidityMask<uint8_t>;

	//! Creates an empty TupleDataLayout
	TupleDataLayout();
	//! Create a copy of this TupleDataLayout
	TupleDataLayout Copy() const;

public:
	//! Initializes the TupleDataLayout with the specified types and aggregates to an empty TupleDataLayout
	void Initialize(vector<LogicalType> types_p, Aggregates aggregates_p, bool align = true, bool heap_offset = true);
	//! Initializes the TupleDataLayout with the specified types to an empty TupleDataLayout
	void Initialize(vector<LogicalType> types, bool align = true, bool heap_offset = true);
	//! Initializes the TupleDataLayout with the specified aggregates to an empty TupleDataLayout
	void Initialize(Aggregates aggregates_p, bool align = true, bool heap_offset = true);

	//! Returns the number of data columns
	inline idx_t ColumnCount() const {
		return types.size();
	}
	//! Returns a list of the column types for this data chunk
	inline const vector<LogicalType> &GetTypes() const {
		return types;
	}
	//! Returns the number of aggregates
	inline idx_t AggregateCount() const {
		return aggregates.size();
	}
	//! Returns a list of the aggregates for this data chunk
	inline Aggregates &GetAggregates() {
		return aggregates;
	}
	const inline Aggregates &GetAggregates() const {
		return aggregates;
	}
	//! Returns a map from column id to the struct TupleDataLayout
	const inline TupleDataLayout &GetStructLayout(idx_t col_idx) const {
		D_ASSERT(struct_layouts->find(col_idx) != struct_layouts->end());
		return struct_layouts->find(col_idx)->second;
	}
	//! Returns the total width required for each row, including padding
	inline idx_t GetRowWidth() const {
		return row_width;
	}
	//! Returns the offset to the start of the data
	inline idx_t GetDataOffset() const {
		return flag_width;
	}
	//! Returns the total width required for the data, including padding
	inline idx_t GetDataWidth() const {
		return data_width;
	}
	//! Returns the offset to the start of the aggregates
	inline idx_t GetAggrOffset() const {
		return flag_width + data_width;
	}
	//! Returns the total width required for the aggregates, including padding
	inline idx_t GetAggrWidth() const {
		return aggr_width;
	}
	//! Returns the column offsets into each row
	inline const vector<idx_t> &GetOffsets() const {
		return offsets;
	}
	//! Returns whether all columns in this layout are constant size
	inline bool AllConstant() const {
		return all_constant;
	}
	//! Gets offset to where heap size is stored
	inline idx_t GetHeapSizeOffset() const {
		return heap_size_offset;
	}
	//! Returns whether any of the aggregates have a destructor
	inline bool HasDestructor() const {
		return !aggr_destructor_idxs.empty();
	}
	//! Returns the indices of the aggregates that have destructors
	inline const vector<idx_t> &GetAggregateDestructorIndices() const {
		return aggr_destructor_idxs;
	}

private:
	//! The types of the data columns
	vector<LogicalType> types;
	//! The aggregate functions
	Aggregates aggregates;
	//! Structs are a recursive TupleDataLayout
	unique_ptr<unordered_map<idx_t, TupleDataLayout>> struct_layouts;
	//! The width of the validity header
	idx_t flag_width;
	//! The width of the data portion
	idx_t data_width;
	//! The width of the aggregate state portion
	idx_t aggr_width;
	//! The width of the entire row
	idx_t row_width;
	//! The offsets to the columns and aggregate data in each row
	vector<idx_t> offsets;
	//! Whether all columns in this layout are constant size
	bool all_constant;
	//! Offset to the heap size of every row
	idx_t heap_size_offset;
	//! Indices of aggregate functions that have a destructor
	vector<idx_t> aggr_destructor_idxs;
};

} // namespace duckdb
