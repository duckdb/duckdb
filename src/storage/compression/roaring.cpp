#include "duckdb/storage/compression/roaring.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

/*
Data layout per segment:
+--------------------------------------------+
|            Vector Metadata                 |
|   +------------------------------------+   |
|   |   int64_t  page_id[]               |   |
|   |   uint32_t page_offset[]           |   |
|   |   uint64_t uncompressed_size[]     |   |
|   |   uint64_t compressed_size[]       |   |
|   +------------------------------------+   |
|                                            |
+--------------------------------------------+
|              [Vector Data]+                |
|   +------------------------------------+   |
|   |   uint32_t lengths[]               |   |
|   |   void    *compressed_data         |   |
|   +------------------------------------+   |
|                                            |
+--------------------------------------------+
*/

namespace {

struct ContainerMetadata {
public:
	ContainerMetadata() {
	}
	ContainerMetadata(bool is_run, uint16_t value) {
		if (is_run) {
			data.run_container.is_run = 1;
			data.run_container.number_of_runs = value;
			data.run_container.unused = 0;
		} else {
			data.non_run_container.is_run = 0;
			data.non_run_container.cardinality = value;
			data.non_run_container.unused = 0;
		}
	}

public:
	bool IsRun() const {
		return data.run_container.is_run;
	}

	uint16_t NumberOfRuns() const {
		return data.run_container.number_of_runs;
	}

	uint16_t Cardinality() const {
		return data.non_run_container.cardinality;
	}

private:
	union {
		struct {
			uint16_t unused : 3;          //! Currently unused bits
			uint16_t is_run : 1;          //! Indicate if this is a run container
			uint16_t unused2 : 1;         //! Currently unused bits
			uint16_t number_of_runs : 11; //! The number of runs
		} run_container;

		struct {
			uint16_t unused : 3;       //! Currently unused bits
			uint16_t is_run : 1;       //! Indicate if this is a run container
			uint16_t cardinality : 12; //! How many values are set
		} non_run_container;
	} data;
};

struct RunContainerRLEPair {
	uint16_t start;
	uint16_t length;
};

enum class ContainerType : uint8_t { RUN_CONTAINER, ARRAY_CONTAINER, BITSET_CONTAINER };

struct ContainerCompressionState {
	static constexpr uint16_t MAX_RUN_IDX = 2047;
	static constexpr uint16_t MAX_ARRAY_IDX = 4095;

public:
	struct Result {
		ContainerType container_type;
		//! Whether nulls are being encoded or non-nulls
		bool nulls;
		//! The amount of runs / size of the final array (dependent on the container type)
		idx_t count;
	};

public:
	ContainerCompressionState() {
		Reset();
	}

public:
	void Append(bool null) {
		// Adjust the runs
		auto &current_run_idx = run_idx[null ? 0 : 1];
		auto &last_run_idx = run_idx[last_is_null ? 0 : 1];

		if (count && null != last_is_null && last_run_idx < MAX_RUN_IDX) {
			auto &last_run = runs[last_is_null ? 0 : 1][last_run_idx];
			// End the last run
			last_run.length = (count - last_run.start);
			last_run_idx++;
		}
		if (!count || (null != last_is_null && current_run_idx < MAX_RUN_IDX)) {
			auto &current_run = runs[null ? 0 : 1][current_run_idx];
			// Initialize the new run
			current_run.start = count;
		}

		// Add to the array
		auto &current_array_idx = array_idx[null ? 0 : 1];
		if (current_array_idx < MAX_ARRAY_IDX) {
			arrays[null ? 0 : 1][current_array_idx] = count;
			current_array_idx++;
		}

		last_is_null = null;
		null_count += !null;
		count++;
	}

	void Finalize() {
		D_ASSERT(!finalized);
		auto &last_run_idx = run_idx[last_is_null ? 0 : 1];
		if (count && last_run_idx < MAX_RUN_IDX) {
			auto &last_run = runs[last_is_null ? 0 : 1][last_run_idx];
			// End the last run
			last_run.length = (count - last_run.start);
			last_run_idx++;
		}
		finalized = true;
	}

	Result GetResult() {
		D_ASSERT(finalized);
		const bool can_use_null_array = array_idx[0] < MAX_ARRAY_IDX;
		const bool can_use_non_null_array = array_idx[1] < MAX_ARRAY_IDX;

		const bool can_use_null_run = run_idx[0] < MAX_RUN_IDX;
		const bool can_use_non_null_run = run_idx[1] < MAX_RUN_IDX;

		const bool can_use_array = can_use_null_array || can_use_non_null_array;
		const bool can_use_run = can_use_null_run || can_use_non_null_run;
		if (!can_use_array && !can_use_run) {
			// Can not efficiently encode at all, write it uncompressed
			return Result {ContainerType::BITSET_CONTAINER, true, count};
		}
		uint16_t lowest_array_cost = duckdb::MinValue<uint16_t>(array_idx[0], array_idx[1]);
		uint16_t lowest_run_cost = duckdb::MinValue<uint16_t>(run_idx[0], run_idx[1]) * 2;

		if (lowest_array_cost <= lowest_run_cost) {
			if (array_idx[0] < array_idx[1]) {
				return Result {ContainerType::ARRAY_CONTAINER, true, array_idx[0]};
			} else {
				return Result {ContainerType::ARRAY_CONTAINER, false, array_idx[1]};
			}
		} else {
			if (run_idx[0] < run_idx[1]) {
				return Result {ContainerType::RUN_CONTAINER, true, run_idx[0]};
			} else {
				return Result {ContainerType::RUN_CONTAINER, false, run_idx[1]};
			}
		}
	}

	void Reset() {
		count = 0;
		null_count = 0;
		run_idx[false] = 0;
		run_idx[true] = 0;
		array_idx[false] = 0;
		array_idx[true] = 0;
		finalized = false;
	}

public:
	//! Total amount of values covered by the container
	idx_t count = 0;
	//! How many of the total are null
	idx_t null_count = 0;
	bool last_is_null;

	//! The runs (for sequential nulls | sequential non-nulls)
	RunContainerRLEPair runs[2][MAX_RUN_IDX];
	//! The indices (for nulls | non-nulls)
	uint16_t arrays[2][MAX_ARRAY_IDX];

	uint16_t run_idx[2];
	uint16_t array_idx[2];

	//! Whether the state has been finalized
	bool finalized = false;
};

} // namespace

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T>
struct RoaringAnalyzeState : public AnalyzeState {
public:
	explicit RoaringAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {};

public:
	void Analyze(Vector &input, idx_t count) {
		UnifiedVectorFormat unified;
		input.ToUnifiedFormat(count, unified);
		auto &validity = unified.validity;

		if (validity.AllValid()) {
			// Fast path for all non-null ?
			return;
		}
		for (idx_t i = 0; i < count; i++) {
			validity.RowIsValidUnsafe(i);
		}
	}

public:
	//! The space used by the current segment
	idx_t spaced_used = 0;
	//! The total amount of segments to write
	idx_t segment_count = 0;
	//! The amount of values in the current segment;
	idx_t current_count = 0;
	//! The total amount of data to serialize
	idx_t count = 0;
};

template <class T>
unique_ptr<AnalyzeState> RoaringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto &config = DBConfig::GetConfig(col_data.GetDatabase());

	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	auto state = make_uniq<RoaringAnalyzeState<T>>(info);

	return std::move(state);
}

template <class T>
bool RoaringAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = state.Cast<RoaringAnalyzeState<T>>();

	// TODO: implement
	// During analyze, we want to walk through the data, for every 8kB we determine which container type should be used,
	// we save that in the analyze state this only costs us 16 bits per container, this will simultaneously be used in
	// the compression stage as the container metadata written to disk

	analyze_state.Analyze(input, count);
	return true;
}

template <class T>
idx_t RoaringFinalAnalyze(AnalyzeState &state) {
	auto &roaring_state = state.Cast<RoaringAnalyzeState<T>>();
	// TODO: implement
	return DConstants::INVALID_INDEX;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T>
struct RoaringCompressState : public CompressionState {
public:
	explicit RoaringCompressState(ColumnDataCheckpointer &checkpointer, const CompressionInfo &info)
	    : CompressionState(info), checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ROARING)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths and frame-of-references (growing downwards).
	data_ptr_t metadata_ptr;

public:
	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();

		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, type, row_start, info.GetBlockSize(), info.GetBlockSize());
		compressed_segment->function = function;
		current_segment = std::move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
		metadata_ptr = handle.Ptr() + info.GetBlockSize();
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
		auto base_ptr = handle.Ptr();

		// TODO: implement

		idx_t total_segment_size = 0;
		state.FlushSegment(std::move(current_segment), std::move(handle), total_segment_size);
	}

	void Finalize() {
		// TODO: implement
		FlushSegment();
		current_segment.reset();
	}
};

template <class T>
unique_ptr<CompressionState> RoaringInitCompression(ColumnDataCheckpointer &checkpointer,
                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<RoaringCompressState<T>>(checkpointer, state->info);
}

template <class T>
void RoaringCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<RoaringCompressState<T>>();
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	// TODO: implement
}

template <class T>
void RoaringFinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<RoaringCompressState<T>>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

template <class T>
struct RoaringScanState : public SegmentScanState {
public:
	explicit RoaringScanState(ColumnSegment &segment) : current_segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		auto data_ptr = handle.Ptr();

		// TODO: implement
	}

	BufferHandle handle;
	ColumnSegment &current_segment;

public:
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		// TODO: implement
	}
};

template <class T>
unique_ptr<SegmentScanState> RoaringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<RoaringScanState<T>>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void RoaringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                        idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<RoaringScanState<T>>();

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	// TODO: implement
}

template <class T>
void RoaringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	RoaringScanPartial<T>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void RoaringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	RoaringScanState<T> scan_state(segment);

	// TODO: implement
}

template <class T>
void RoaringSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = static_cast<RoaringScanState<T> &>(*state.scan_state);
	scan_state.Skip(segment, skip_count);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction GetCompressionFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_ROARING, data_type, RoaringInitAnalyze<T>,
	                           RoaringAnalyze<T>, RoaringFinalAnalyze<T>, RoaringInitCompression<T>, RoaringCompress<T>,
	                           RoaringFinalizeCompress<T>, RoaringInitScan<T>, RoaringScan<T>, RoaringScanPartial<T>,
	                           RoaringFetchRow<T>, RoaringSkip<T>);
}

CompressionFunction RoaringCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return GetCompressionFunction<validity_t>(type);
	default:
		throw InternalException("Unsupported type for Roaring");
	}
}

bool RoaringCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BIT:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
