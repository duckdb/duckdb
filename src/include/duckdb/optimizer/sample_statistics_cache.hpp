//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/sample_statistics_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/optimizer/sample_histogram.hpp"

namespace duckdb {

class ClientContext;
class ExpressionFilter;
class ReservoirSample;
class TableFilter;

//! SampleStatisticsCache builds and caches per-column histograms from a table's reservoir sample.
//! It provides selectivity estimation for filter predicates using histograms and direct sample probing.
class SampleStatisticsCache {
public:
	SampleStatisticsCache();

	//! Build histograms for all supported columns from the given sample
	void BuildFromSample(ReservoirSample &sample, const vector<LogicalType> &types);

	//! Get the histogram for a specific column (returns nullptr if not available)
	optional_ptr<SampleHistogram> GetHistogram(idx_t column_index);

	//! Probe the sample with an expression filter to estimate selectivity.
	//! Returns estimated cardinality after applying the filter, or 0 if probing is not possible.
	idx_t ProbeExpressionFilter(ClientContext &context, const ExpressionFilter &filter, idx_t column_index,
	                            idx_t cardinality);

	//! Invalidate all cached histograms
	void Invalidate();

	//! Whether histograms have been built
	bool IsValid() const;

private:
	mutex cache_lock;
	unordered_map<idx_t, unique_ptr<SampleHistogram>> histograms;
	//! Cached copy of the sample data for direct probing
	DataChunk sample_chunk;
	idx_t sample_count = 0;
	bool valid;
};

} // namespace duckdb
