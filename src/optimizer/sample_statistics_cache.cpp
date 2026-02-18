#include "duckdb/optimizer/sample_statistics_cache.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

SampleStatisticsCache::SampleStatisticsCache() : sample_count(0), valid(false) {
}

void SampleStatisticsCache::BuildFromSample(ReservoirSample &sample, const vector<LogicalType> &types) {
	lock_guard<mutex> guard(cache_lock);
	histograms.clear();
	sample_count = 0;
	valid = false;

	// Get the sample chunk - check if the sample has collected any data first
	if (sample.NumSamplesCollected() == 0) {
		return;
	}
	auto &chunk = sample.Chunk();
	if (chunk.size() == 0) {
		return;
	}

	// Store a copy of the sample data for direct expression probing
	sample_chunk.Reset();
	sample_chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes(), chunk.size());
	chunk.Copy(sample_chunk, 0);
	sample_count = chunk.size();

	// Build a histogram for each column that supports it
	for (idx_t col_idx = 0; col_idx < types.size() && col_idx < chunk.ColumnCount(); col_idx++) {
		auto &col_type = types[col_idx];
		// Only build histograms for orderable types (numeric, string, date/time)
		if (!col_type.IsNumeric() && col_type.id() != LogicalTypeId::VARCHAR && col_type.id() != LogicalTypeId::DATE &&
		    col_type.id() != LogicalTypeId::TIMESTAMP && col_type.id() != LogicalTypeId::TIMESTAMP_TZ &&
		    col_type.id() != LogicalTypeId::TIME && col_type.id() != LogicalTypeId::INTERVAL) {
			continue;
		}

		auto histogram = make_uniq<SampleHistogram>(col_type);
		histogram->Build(chunk.data[col_idx], chunk.size());
		if (histogram->IsValid()) {
			histograms[col_idx] = std::move(histogram);
		}
	}

	valid = true;
}

idx_t SampleStatisticsCache::ProbeExpressionFilter(ClientContext &context, const ExpressionFilter &filter,
                                                   idx_t column_index, idx_t cardinality) {
	lock_guard<mutex> guard(cache_lock);
	if (!valid || sample_count == 0) {
		return 0;
	}
	if (column_index >= sample_chunk.ColumnCount()) {
		return 0;
	}

	// Build a single-column DataChunk containing just the relevant column from the sample.
	// ExpressionFilter expressions use BoundReferenceExpression(index=0) to reference the column.
	DataChunk probe_chunk;
	probe_chunk.data.emplace_back(sample_chunk.data[column_index]);
	probe_chunk.SetCardinality(sample_count);

	// Create an executor with the filter expression and evaluate against the sample.
	// Wrap in try-catch because the expression may fail due to type mismatches
	// (e.g., sample column type differs from what the expression expects).
	try {
		ExpressionExecutor executor(context, *filter.expr);
		SelectionVector sel(sample_count);
		idx_t matching = executor.SelectExpression(probe_chunk, sel);

		// Compute selectivity from the sample and extrapolate to full table
		double selectivity = static_cast<double>(matching) / static_cast<double>(sample_count);
		return MaxValue<idx_t>(LossyNumericCast<idx_t>(static_cast<double>(cardinality) * selectivity), 1U);
	} catch (...) {
		return 0;
	}
}

optional_ptr<SampleHistogram> SampleStatisticsCache::GetHistogram(idx_t column_index) {
	lock_guard<mutex> guard(cache_lock);
	auto it = histograms.find(column_index);
	if (it == histograms.end()) {
		return nullptr;
	}
	return it->second.get();
}

void SampleStatisticsCache::Invalidate() {
	lock_guard<mutex> guard(cache_lock);
	histograms.clear();
	valid = false;
}

bool SampleStatisticsCache::IsValid() const {
	return valid;
}

} // namespace duckdb
