//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/relation_statistics/relation_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

enum class DistinctCountSource : uint8_t { CARDINALITY, MIN_MAX, HLL, EXACT };

struct DistinctCount {
public:
	DistinctCount(idx_t distinct_count, DistinctCountSource source);

public:
	idx_t distinct_count;
	DistinctCountSource source;
};

struct RelationColumnStats {
public:
	RelationColumnStats(ColumnBinding binding, DistinctCount distinct_count, Identifier name);

public:
	ColumnBinding binding;
	DistinctCount distinct_count;
	Identifier name;
};

struct RelationStats {
public:
	RelationStats();

public:
	optional_idx FindColumn(ColumnBinding binding) const;
	optional_ptr<const RelationColumnStats> GetColumnStats(ColumnBinding binding) const;
	bool MatchesBindings(const vector<ColumnBinding> &bindings) const;
	void Verify(const vector<ColumnBinding> &bindings) const;

public:
	vector<RelationColumnStats> columns;
	idx_t cardinality;
	double filter_strength = 1;
	bool stats_initialized = false;
	Identifier table_name;
};

} // namespace duckdb
