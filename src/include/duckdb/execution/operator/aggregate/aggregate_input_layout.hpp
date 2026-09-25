//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/aggregate_input_layout.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_layout.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

struct AggregateObject;
class BoundAggregateExpression;

//! Aggregate arguments are contiguous groups followed by one column for each FILTER.
class AggregateInputLayout {
public:
	explicit AggregateInputLayout(const vector<BoundAggregateExpression *> &bindings);
	AggregateInputLayout(const vector<LogicalType> &types, const vector<AggregateObject> &aggregates);

	const ChunkLayout &Payload() const {
		return *layout;
	}
	ChunkColumnView Arguments(DataChunk &chunk, idx_t aggregate_idx) const {
		return layout->Columns(chunk, arguments[aggregate_idx]);
	}
	bool HasFilter(idx_t aggregate_idx) const {
		return filters[aggregate_idx].IsValid();
	}
	idx_t FilterColumnIndex(idx_t aggregate_idx) const {
		return filters[aggregate_idx].GetIndex();
	}
	Vector &Filter(DataChunk &chunk, idx_t aggregate_idx) const {
		return layout->Column(chunk, layout->AllColumns().Column(FilterColumnIndex(aggregate_idx)));
	}
	ChunkProjection CreateProjection(const vector<LogicalType> &input_types,
	                                 const vector<BoundAggregateExpression *> &bindings) const;

private:
	void Initialize(const vector<LogicalType> &types, const vector<idx_t> &argument_counts,
	                const vector<bool> &has_filters);

	unique_ptr<ChunkLayout> layout;
	vector<ChunkColumnGroup> arguments;
	vector<optional_idx> filters;
};

} // namespace duckdb
