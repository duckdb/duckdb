#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

bool ArrowUtil::TryFetchChunk(ChunkScanState &scan_state, ClientProperties options, idx_t batch_size, ArrowArray *out,
                              idx_t &count, ErrorData &error) {
	count = 0;
	ArrowAppender appender(scan_state.Types(), batch_size, std::move(options));
	auto remaining_tuples_in_chunk = scan_state.RemainingInChunk();
	if (remaining_tuples_in_chunk) {
		// We start by scanning the non-finished current chunk
		idx_t cur_consumption = MinValue(remaining_tuples_in_chunk, batch_size);
		count += cur_consumption;
		auto &current_chunk = scan_state.CurrentChunk();
		appender.Append(current_chunk, scan_state.CurrentOffset(), scan_state.CurrentOffset() + cur_consumption,
		                current_chunk.size());
		scan_state.IncreaseOffset(cur_consumption);
	}
	while (count < batch_size) {
		if (!scan_state.LoadNextChunk(error)) {
			if (scan_state.HasError()) {
				error = scan_state.GetError();
			}
			return false;
		}
		if (scan_state.ChunkIsEmpty()) {
			// The scan was successful, but an empty chunk was returned
			break;
		}
		auto &current_chunk = scan_state.CurrentChunk();
		if (scan_state.Finished() || current_chunk.size() == 0) {
			break;
		}
		// The amount we still need to append into this chunk
		auto remaining = batch_size - count;

		// The amount remaining, capped by the amount left in the current chunk
		auto to_append_to_batch = MinValue(remaining, scan_state.RemainingInChunk());
		appender.Append(current_chunk, 0, to_append_to_batch, current_chunk.size());
		count += to_append_to_batch;
		scan_state.IncreaseOffset(to_append_to_batch);
	}
	if (count > 0) {
		*out = appender.Finalize();
	}
	return true;
}

idx_t ArrowUtil::FetchChunk(ChunkScanState &scan_state, ClientProperties options, idx_t chunk_size, ArrowArray *out) {
	ErrorData error;
	idx_t result_count;
	if (!TryFetchChunk(scan_state, std::move(options), chunk_size, out, result_count, error)) {
		error.Throw();
	}
	return result_count;
}

} // namespace duckdb
