#pragma once

#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

namespace duckdb {

class EmptyValidityCompression {
public:
	struct EmptyValidityCompressionState : public CompressionState {
	public:
		explicit EmptyValidityCompressionState(ColumnDataCheckpointData &checkpoint_data, const CompressionInfo &info)
		    : CompressionState(info),
		      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_EMPTY)),
		      checkpoint_data(checkpoint_data) {
		}
		~EmptyValidityCompressionState() override {
		}

	public:
		optional_ptr<CompressionFunction> function;
		ColumnDataCheckpointData &checkpoint_data;
		idx_t count = 0;
		idx_t non_nulls = 0;
	};
	struct EmptyValiditySegmentScanState : public SegmentScanState {
		EmptyValiditySegmentScanState() {
		}
	};

public:
	static CompressionFunction CreateFunction() {
		CompressionFunction result(CompressionType::COMPRESSION_EMPTY, PhysicalType::BIT, nullptr, nullptr, nullptr,
		                           InitCompression, Compress, FinalizeCompress, InitScan, Scan, ScanPartial, FetchRow,
		                           Skip, InitSegment);
		result.filter = Filter;
		result.select = Select;
		return result;
	}

public:
	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
	                                                    unique_ptr<AnalyzeState> state_p) {
		return make_uniq<EmptyValidityCompressionState>(checkpoint_data, state_p->info);
	}
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
		auto &state = state_p.Cast<EmptyValidityCompressionState>();
		UnifiedVectorFormat format;
		scan_vector.ToUnifiedFormat(count, format);
		state.non_nulls += format.validity.CountValid(count);
		state.count += count;
	}
	static void FinalizeCompress(CompressionState &state_p) {
		auto &state = state_p.Cast<EmptyValidityCompressionState>();
		auto &checkpoint_data = state.checkpoint_data;

		auto &db = checkpoint_data.GetDatabase();
		auto &type = checkpoint_data.GetType();

		auto &info = state.info;
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, *state.function, type, info.GetBlockSize(),
		                                                                info.GetBlockManager());
		compressed_segment->count = state.count;
		if (state.non_nulls != state.count) {
			compressed_segment->stats.statistics.SetHasNullFast();
		}
		if (state.non_nulls != 0) {
			compressed_segment->stats.statistics.SetHasNoNullFast();
		}

		auto &buffer_manager = BufferManager::GetBufferManager(checkpoint_data.GetDatabase());
		auto handle = buffer_manager.Pin(compressed_segment->block);

		auto &checkpoint_state = checkpoint_data.GetCheckpointState();
		checkpoint_state.FlushSegment(std::move(compressed_segment), std::move(handle), 0);
	}
	static unique_ptr<SegmentScanState> InitScan(const QueryContext &context, ColumnSegment &segment) {
		return make_uniq<EmptyValiditySegmentScanState>();
	}
	static void ScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                        idx_t result_offset) {
		return;
	}
	static void Scan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
		return;
	}
	static void FetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                     idx_t result_idx) {
		return;
	}
	static void Skip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
		return;
	}
	static unique_ptr<CompressedSegmentState> InitSegment(ColumnSegment &segment, block_id_t block_id,
	                                                      optional_ptr<ColumnSegmentState> segment_state) {
		return nullptr;
	}
	static void Filter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
	                   SelectionVector &sel, idx_t &sel_count, const TableFilter &filter,
	                   TableFilterState &filter_state) {
	}
	static void Select(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
	                   const SelectionVector &sel, idx_t sel_count) {
	}
};

} // namespace duckdb
