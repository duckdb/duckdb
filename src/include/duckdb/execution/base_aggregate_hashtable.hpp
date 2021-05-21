//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/base_aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {
class BoundAggregateExpression;
class BufferManager;

struct AggregateObject {
	AggregateObject(AggregateFunction function, FunctionData *bind_data, idx_t child_count, idx_t payload_size,
	                bool distinct, PhysicalType return_type, Expression *filter = nullptr)
	    : function(move(function)), bind_data(bind_data), child_count(child_count), payload_size(payload_size),
	      distinct(distinct), return_type(return_type), filter(filter) {
	}

	AggregateFunction function;
	FunctionData *bind_data;
	idx_t child_count;
	idx_t payload_size;
	bool distinct;
	PhysicalType return_type;
	Expression *filter = nullptr;

	static vector<AggregateObject> CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings);
};

class RowLayout {
public:
	using Aggregates = vector<AggregateObject>;
	using ValidityBytes = TemplatedValidityMask<uint8_t>;

	static inline idx_t Align(idx_t n) {
		return ((n + 7) / 8) * 8;
	}

	//! Creates an empty RowLayout
	RowLayout();

public:
	//! Initializes the RowLayout with the specified types and aggregates to an empty RowLayout
	void Initialize(vector<LogicalType> types_p, Aggregates aggregates_p);
	//! Initializes the RowLayout with the specified types to an empty RowLayout
	void Initialize(vector<LogicalType> types);
	//! Initializes the RowLayout with the specified aggregates to an empty RowLayout
	void Initialize(Aggregates aggregates_p);
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

private:
	//! The types of the data columns
	vector<LogicalType> types;
	//! The aggregate functions
	Aggregates aggregates;
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
};

class BaseAggregateHashTable {
public:
	using ValidityBytes = TemplatedValidityMask<uint8_t>;

	BaseAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types,
	                       vector<LogicalType> payload_types, vector<AggregateObject> aggregate_objects);
	virtual ~BaseAggregateHashTable() {
	}

	static idx_t Align(idx_t n) {
		return ((n + 7) / 8) * 8;
	}

protected:
	BufferManager &buffer_manager;
	//! A helper for managing offsets into the data buffers
	RowLayout layout;
	//! The types of the payload columns stored in the hashtable
	vector<LogicalType> payload_types;

	//! The empty payload data
	unique_ptr<data_t[]> empty_payload_data;

protected:
	void CallDestructors(Vector &state_vector, idx_t count);
};

} // namespace duckdb
