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
	//! The aggregates to be computed
	vector<AggregateObject> aggregates;
	//! The types of the group columns stored in the hashtable
	vector<LogicalType> group_types;
	//! The types of the payload columns stored in the hashtable
	vector<LogicalType> payload_types;
	//! The size of the groups in bytes
	idx_t group_width;
	//! The size of the group validity mask in bytes
	idx_t group_mask_width;
	//! some optional padding to align payload
	idx_t group_padding;
	//! The size of the payload (aggregations) in bytes
	idx_t payload_width;

	//! The empty payload data
	unique_ptr<data_t[]> empty_payload_data;

protected:
	void CallDestructors(Vector &state_vector, idx_t count);
};

} // namespace duckdb
