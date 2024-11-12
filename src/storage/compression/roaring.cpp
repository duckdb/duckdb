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

//! The amount of values that are encoded per container
static constexpr idx_t ROARING_CONTAINER_SIZE = 65536;
static constexpr bool NULLS = true;
static constexpr bool NON_NULLS = false;

namespace {

struct ContainerMetadata {
public:
	ContainerMetadata() {
	}
	ContainerMetadata(bool is_inverted, bool is_run, uint16_t value) {
		data.run_container.is_inverted = is_inverted;
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

	bool IsInverted() const {
		return data.run_container.is_inverted;
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
			uint16_t unused : 2;          //! Currently unused bits
			uint16_t is_inverted : 1;     //! Indicate if this maps nulls or non-nulls
			uint16_t is_run : 1;          //! Indicate if this is a run container
			uint16_t unused2 : 1;         //! Currently unused bits
			uint16_t number_of_runs : 11; //! The number of runs
		} run_container;

		struct {
			uint16_t unused : 2;       //! Currently unused bits
			uint16_t is_inverted : 1;  //! Indicate if this maps nulls or non-nulls
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
	public:
		static Result RunContainer(idx_t runs, bool nulls) {
			auto res = Result();
			res.container_type = ContainerType::RUN_CONTAINER;
			res.nulls = nulls;
			res.count = runs;
			return res;
		}

		static Result ArrayContainer(idx_t array_size, bool nulls) {
			auto res = Result();
			res.container_type = ContainerType::ARRAY_CONTAINER;
			res.nulls = nulls;
			res.count = array_size;
			return res;
		}

		static Result BitsetContainer(idx_t container_size) {
			auto res = Result();
			res.container_type = ContainerType::BITSET_CONTAINER;
			res.count = container_size;
			return res;
		}

	public:
		idx_t GetByteSize() const {
			idx_t res = 0;
			res += sizeof(ContainerMetadata);
			switch (container_type) {
			case ContainerType::BITSET_CONTAINER:
				res += duckdb::AlignValue<idx_t, 8>(count) / 8;
				break;
			case ContainerType::RUN_CONTAINER:
				res += count * sizeof(RunContainerRLEPair);
				break;
			case ContainerType::ARRAY_CONTAINER:
				res += count * sizeof(uint16_t);
				break;
			}
			return res;
		}

	public:
		ContainerType container_type;
		//! Whether nulls are being encoded or non-nulls
		bool nulls;
		//! The amount (meaning depends on container_type)
		idx_t count;

	private:
		Result() {
		}
	};

public:
	ContainerCompressionState() {
		Reset();
	}

public:
	void Append(bool null, idx_t amount = 1) {
		// Adjust the runs
		auto &current_run_idx = run_idx[null];
		auto &last_run_idx = run_idx[last_is_null];

		if (count && null != last_is_null && last_run_idx < MAX_RUN_IDX) {
			auto &last_run = runs[last_is_null][last_run_idx];
			// End the last run
			last_run.length = (count - last_run.start);
			last_run_idx++;
		}
		if (!count || (null != last_is_null && current_run_idx < MAX_RUN_IDX)) {
			auto &current_run = runs[null][current_run_idx];
			// Initialize the new run
			current_run.start = count;
		}

		// Add to the array
		auto &current_array_idx = array_idx[null];
		if (current_array_idx < MAX_ARRAY_IDX) {
			for (idx_t i = 0; i < amount; i++) {
				arrays[null][current_array_idx + i] = count + i;
			}
			current_array_idx += amount;
		}

		last_is_null = null;
		null_count += null * amount;
		count += amount;
	}

	bool IsFull() const {
		return count == ROARING_CONTAINER_SIZE;
	}

	void Finalize() {
		D_ASSERT(!finalized);
		auto &last_run_idx = run_idx[last_is_null];
		if (count && last_run_idx < MAX_RUN_IDX) {
			auto &last_run = runs[last_is_null][last_run_idx];
			// End the last run
			last_run.length = (count - last_run.start);
			last_run_idx++;
		}
		finalized = true;
	}

	Result GetResult() {
		D_ASSERT(finalized);
		const bool can_use_null_array = array_idx[NON_NULLS] < MAX_ARRAY_IDX;
		const bool can_use_non_null_array = array_idx[NULLS] < MAX_ARRAY_IDX;

		const bool can_use_null_run = run_idx[NON_NULLS] < MAX_RUN_IDX;
		const bool can_use_non_null_run = run_idx[NULLS] < MAX_RUN_IDX;

		const bool can_use_array = can_use_null_array || can_use_non_null_array;
		const bool can_use_run = can_use_null_run || can_use_non_null_run;
		if (!can_use_array && !can_use_run) {
			// Can not efficiently encode at all, write it uncompressed
			return Result::BitsetContainer(count);
		}
		uint16_t lowest_array_cost = duckdb::MinValue<uint16_t>(array_idx[NON_NULLS], array_idx[NULLS]);
		uint16_t lowest_run_cost = duckdb::MinValue<uint16_t>(run_idx[NON_NULLS], run_idx[NULLS]) * 2;
		if (duckdb::MinValue<uint16_t>(lowest_array_cost, lowest_run_cost) >
		    duckdb::AlignValue<uint16_t, 8>(count) / 8) {
			// The amount of values is too small, better off using uncompressed
			// we can detect this at decompression because we know how many values are left
			return Result::BitsetContainer(count);
		}

		if (lowest_array_cost <= lowest_run_cost) {
			if (array_idx[NULLS] < array_idx[NON_NULLS]) {
				return Result::ArrayContainer(array_idx[NULLS], NULLS);
			} else {
				return Result::ArrayContainer(array_idx[NON_NULLS], NON_NULLS);
			}
		} else {
			if (run_idx[NULLS] < run_idx[NON_NULLS]) {
				return Result::RunContainer(run_idx[NULLS], NULLS);
			} else {
				return Result::RunContainer(run_idx[NON_NULLS], NON_NULLS);
			}
		}
	}

	void Reset() {
		count = 0;
		null_count = 0;
		run_idx[NON_NULLS] = 0;
		run_idx[NULLS] = 0;
		array_idx[NON_NULLS] = 0;
		array_idx[NULLS] = 0;
		finalized = false;
		last_is_null = false;
	}

public:
	//! Total amount of values covered by the container
	idx_t count = 0;
	//! How many of the total are null
	idx_t null_count = 0;
	bool last_is_null = false;

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
	bool HasEnoughSpaceInSegment(idx_t required_space) {
		D_ASSERT(space_used <= info.GetBlockSize());
		idx_t remaining_space = info.GetBlockSize() - space_used;
		if (required_space > remaining_space) {
			return false;
		}
		return true;
	}

	void FlushSegment() {
		if (!current_count) {
			D_ASSERT(!space_used);
			return;
		}
		total_size += space_used;
		space_used = 0;
		current_count = 0;
		segment_count++;
	}

	void FlushContainer() {
		if (!container_state.count) {
			return;
		}
		container_state.Finalize();
		auto res = container_state.GetResult();
		auto required_space = res.GetByteSize();
		if (!HasEnoughSpaceInSegment(required_space)) {
			FlushSegment();
		}
		space_used += required_space;
		current_count += container_state.count;
		container_state.Reset();
	}

	void Analyze(Vector &input, idx_t count) {
		UnifiedVectorFormat unified;
		input.ToUnifiedFormat(count, unified);
		auto &validity = unified.validity;

		if (validity.AllValid()) {
			idx_t appended = 0;
			while (appended < count) {
				idx_t to_append = MinValue<idx_t>(ROARING_CONTAINER_SIZE - container_state.count, count - appended);
				container_state.Append(false, to_append);
				if (container_state.IsFull()) {
					FlushContainer();
				}
				appended += to_append;
			}
		} else {
			idx_t appended = 0;
			while (appended < count) {
				idx_t to_append = MinValue<idx_t>(ROARING_CONTAINER_SIZE - container_state.count, count - appended);
				for (idx_t i = 0; i < to_append; i++) {
					auto is_null = validity.RowIsValidUnsafe(appended + i);
					container_state.Append(!is_null);
				}
				if (container_state.IsFull()) {
					FlushContainer();
				}
				appended += to_append;
			}
		}
		this->count += count;
	}

public:
	ContainerCompressionState container_state;
	//! The space used by the current segment
	idx_t space_used = 0;
	//! The total amount of segments to write
	idx_t segment_count = 0;
	//! The amount of values in the current segment;
	idx_t current_count = 0;
	//! The total amount of data to serialize
	idx_t count = 0;

	//! The total amount of bytes used to compress the whole segment
	idx_t total_size = 0;
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
	roaring_state.FlushContainer();
	roaring_state.FlushSegment();
	return roaring_state.total_size;
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
