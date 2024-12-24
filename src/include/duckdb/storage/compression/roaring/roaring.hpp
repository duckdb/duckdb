//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/roaring/roaring.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {
namespace roaring {

//! Used for compressed runs/arrays
static constexpr uint16_t COMPRESSED_SEGMENT_SIZE = 256;
//! compresed segment size is 256, instead of division we can make use of shifting
static constexpr uint16_t COMPRESSED_SEGMENT_SHIFT_AMOUNT = 8;
//! The amount of values that are encoded per container
static constexpr idx_t ROARING_CONTAINER_SIZE = 2048;
static constexpr bool NULLS = true;
static constexpr bool NON_NULLS = false;
static constexpr uint16_t UNCOMPRESSED_SIZE = (ROARING_CONTAINER_SIZE / sizeof(validity_t));
static constexpr uint16_t COMPRESSED_SEGMENT_COUNT = (ROARING_CONTAINER_SIZE / COMPRESSED_SEGMENT_SIZE);

static constexpr uint16_t MAX_RUN_IDX = (UNCOMPRESSED_SIZE - COMPRESSED_SEGMENT_COUNT) / (sizeof(uint8_t) * 2);
static constexpr uint16_t MAX_ARRAY_IDX = (UNCOMPRESSED_SIZE - COMPRESSED_SEGMENT_COUNT) / (sizeof(uint8_t) * 1);
//! The value used to indicate that a container is a bitset container
static constexpr uint16_t BITSET_CONTAINER_SENTINEL_VALUE = MAX_ARRAY_IDX + 1;
static constexpr uint16_t COMPRESSED_ARRAY_THRESHOLD = 8;
static constexpr uint16_t COMPRESSED_RUN_THRESHOLD = 4;

static constexpr uint16_t CONTAINER_TYPE_BITWIDTH = 2;
static constexpr uint16_t RUN_CONTAINER_SIZE_BITWIDTH = 7;
static constexpr uint16_t ARRAY_CONTAINER_SIZE_BITWIDTH = 8;

//! This can be increased, but requires additional changes beyond just changing the constant
static_assert(MAX_RUN_IDX < NumericLimits<uint8_t>::Maximum(), "The run size is encoded in a maximum of 8 bits");

//! This can be increased, but requires additional changes beyond just changing the constant
static_assert(BITSET_CONTAINER_SENTINEL_VALUE < NumericLimits<uint8_t>::Maximum(),
              "The array/bitset size is encoded in a maximum of 8 bits");

static_assert(ROARING_CONTAINER_SIZE % COMPRESSED_SEGMENT_SIZE == 0,
              "The (maximum) container size has to be cleanly divisable by the segment size");

static_assert((1 << RUN_CONTAINER_SIZE_BITWIDTH) - 1 >= MAX_RUN_IDX,
              "The bitwidth used to store the size of a run container has to be big enough to store the maximum size");
static_assert(
    (1 << ARRAY_CONTAINER_SIZE_BITWIDTH) - 1 >= MAX_ARRAY_IDX + 1,
    "The bitwidth used to store the size of an array/bitset container has to be big enough to store the maximum size");

void SetInvalidRange(ValidityMask &result, idx_t start, idx_t end);

struct RunContainerRLEPair {
	uint16_t start;
	uint16_t length;
};

enum class ContainerType : uint8_t { RUN_CONTAINER, ARRAY_CONTAINER, BITSET_CONTAINER };

struct ContainerMetadata {
public:
	bool operator==(const ContainerMetadata &other) const {
		if (container_type != other.container_type) {
			return false;
		}
		if (count != other.count) {
			return false;
		}
		if (nulls != other.nulls) {
			return false;
		}
		return true;
	}

	static ContainerMetadata CreateMetadata(uint16_t count, uint16_t array_null, uint16_t array_non_null,
	                                        uint16_t runs);

	static ContainerMetadata RunContainer(uint16_t runs) {
		auto res = ContainerMetadata();
		res.container_type = ContainerType::RUN_CONTAINER;
		res.nulls = true;
		res.count = runs;
		return res;
	}

	static ContainerMetadata ArrayContainer(uint16_t array_size, bool nulls) {
		auto res = ContainerMetadata();
		res.container_type = ContainerType::ARRAY_CONTAINER;
		res.nulls = nulls;
		res.count = array_size;
		return res;
	}

	static ContainerMetadata BitsetContainer(uint16_t container_size) {
		auto res = ContainerMetadata();
		res.container_type = ContainerType::BITSET_CONTAINER;
		res.nulls = true;
		res.count = container_size;
		return res;
	}

public:
	idx_t GetDataSizeInBytes(idx_t container_size) const;
	bool IsUncompressed() const {
		return container_type == ContainerType::BITSET_CONTAINER;
	}
	bool IsRun() const {
		return container_type == ContainerType::RUN_CONTAINER;
	}
	bool IsInverted() const {
		return nulls;
	}
	bool IsArray() const {
		return container_type == ContainerType::ARRAY_CONTAINER;
	}
	idx_t NumberOfRuns() const {
		D_ASSERT(IsRun());
		return count;
	}
	idx_t Cardinality() const {
		D_ASSERT(IsArray());
		return count;
	}

public:
	ContainerType container_type;
	//! Whether nulls are being encoded or non-nulls
	bool nulls;
	//! The amount (meaning depends on container_type)
	uint16_t count;

private:
	ContainerMetadata() {
	}
};

struct ContainerMetadataCollection {
	static constexpr uint8_t IS_RUN_FLAG = 1 << 1;
	static constexpr uint8_t IS_INVERTED_FLAG = 1 << 0;

public:
	ContainerMetadataCollection();

public:
	void AddMetadata(ContainerMetadata metadata);
	idx_t GetMetadataSizeForSegment() const;
	idx_t GetMetadataSize(idx_t container_count, idx_t run_containers, idx_t array_containers) const;
	idx_t GetRunContainerCount() const;
	idx_t GetArrayAndBitsetContainerCount() const;
	void FlushSegment();
	void Reset();
	// Write the metadata for the current segment
	idx_t Serialize(data_ptr_t dest) const;
	void Deserialize(data_ptr_t src, idx_t container_count);

private:
	void AddBitsetContainer();
	void AddArrayContainer(idx_t amount, bool is_inverted);
	void AddRunContainer(idx_t amount, bool is_inverted);
	void AddContainerType(bool is_run, bool is_inverted);

public:
	//! Encode for each container in the lower 2 bits if the container 'is_run' and 'is_inverted'
	vector<uint8_t> container_type;
	//! Encode for each run container the length
	vector<uint8_t> number_of_runs;
	//! Encode for each array/bitset container the length
	vector<uint8_t> cardinality;

	idx_t count_in_segment = 0;
	idx_t runs_in_segment = 0;
	idx_t arrays_in_segment = 0;
};

struct ContainerMetadataCollectionScanner {
public:
	explicit ContainerMetadataCollectionScanner(ContainerMetadataCollection &collection);

public:
	ContainerMetadata GetNext();

public:
	const ContainerMetadataCollection &collection;
	idx_t array_idx = 0;
	idx_t run_idx = 0;
	idx_t idx = 0;
};

struct BitmaskTableEntry {
	uint8_t first_bit_set : 1;
	uint8_t last_bit_set : 1;
	uint8_t valid_count : 6;
	uint8_t run_count;
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct RoaringAnalyzeState : public AnalyzeState {
public:
	explicit RoaringAnalyzeState(const CompressionInfo &info);

public:
	// RoaringStateAppender interface:

	static void HandleByte(RoaringAnalyzeState &state, uint8_t array_index);
	static void HandleRaggedByte(RoaringAnalyzeState &state, uint8_t array_index, idx_t relevant_bits);
	static void HandleAllValid(RoaringAnalyzeState &state, idx_t amount);
	static void HandleNoneValid(RoaringAnalyzeState &state, idx_t amount);
	static idx_t Count(RoaringAnalyzeState &state);
	static void Flush(RoaringAnalyzeState &state);

public:
	ContainerMetadata GetResult();
	bool HasEnoughSpaceInSegment(idx_t required_space);
	void FlushSegment();
	void FlushContainer();
	void Analyze(Vector &input, idx_t count);

public:
	unsafe_unique_array<BitmaskTableEntry> bitmask_table;

	//! Analyze phase

	//! The amount of set bits found
	uint16_t one_count = 0;
	//! The amount of unset bits found
	uint16_t zero_count = 0;
	//! The amount of runs (of 0's) so far
	uint16_t run_count = 0;
	//! Whether the last bit was set or not
	bool last_bit_set;
	//! The total amount of bits covered (one_count + zero_count)
	uint16_t count = 0;

	//! Flushed analyze data

	//! The space used by the current segment
	idx_t space_used = 0;
	//! The total amount of segments to write
	idx_t segment_count = 0;
	//! The amount of values in the current segment;
	idx_t current_count = 0;
	//! The total amount of data to serialize
	idx_t total_count = 0;

	//! The total amount of bytes used to compress the whole segment
	idx_t total_size = 0;
	//! The container metadata, determining the type of each container to use during compression
	ContainerMetadataCollection metadata_collection;
	vector<ContainerMetadata> container_metadata;
};

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//

struct ContainerCompressionState {
	using append_func_t = void (*)(ContainerCompressionState &, bool, uint16_t);

public:
	ContainerCompressionState();

public:
	void Append(bool null, uint16_t amount = 1);
	void OverrideArray(data_ptr_t destination, bool nulls, idx_t count);
	void OverrideRun(data_ptr_t destination, idx_t count);
	void OverrideUncompressed(data_ptr_t destination);
	void Finalize();
	ContainerMetadata GetResult();
	void Reset();

public:
	//! Buffered append state (we don't want to append every bit separately)
	uint16_t length = 0;
	bool last_bit_set;

	//! Total amount of values covered by the container
	uint16_t appended_count = 0;
	//! How many of the total are null
	uint16_t null_count = 0;
	bool last_is_null = false;

	RunContainerRLEPair *runs;
	uint8_t *compressed_runs;
	uint8_t *compressed_arrays[2];
	uint16_t *arrays[2];

	//! The runs (for sequential nulls)
	RunContainerRLEPair base_runs[COMPRESSED_RUN_THRESHOLD];
	//! The indices (for nulls | non-nulls)
	uint16_t base_arrays[2][COMPRESSED_ARRAY_THRESHOLD];

	uint16_t run_idx;
	uint16_t array_idx[2];

	uint8_t *array_counts[2];
	uint8_t *run_counts;

	uint8_t base_compressed_arrays[2][MAX_ARRAY_IDX];
	uint8_t base_compressed_runs[MAX_RUN_IDX * 2];
	uint8_t base_array_counts[2][COMPRESSED_SEGMENT_COUNT];
	uint8_t base_run_counts[COMPRESSED_SEGMENT_COUNT];

	validity_t *uncompressed = nullptr;
	//! Whether the state has been finalized
	bool finalized = false;
	//! The function called to append to the state
	append_func_t append_function;
};

struct RoaringCompressState : public CompressionState {
public:
	explicit RoaringCompressState(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> analyze_state_p);

public:
	//! RoaringStateAppender interface
	static void HandleByte(RoaringCompressState &state, uint8_t array_index);
	static void HandleRaggedByte(RoaringCompressState &state, uint8_t array_index, idx_t relevant_bits);
	static void HandleAllValid(RoaringCompressState &state, idx_t amount);
	static void HandleNoneValid(RoaringCompressState &state, idx_t amount);
	static idx_t Count(RoaringCompressState &state);
	static void Flush(RoaringCompressState &state);

public:
	idx_t GetContainerIndex();
	idx_t GetRemainingSpace();
	bool CanStore(idx_t container_size, const ContainerMetadata &metadata);
	void InitializeContainer();
	void CreateEmptySegment(idx_t row_start);
	void FlushSegment();
	void Finalize();
	void FlushContainer();
	void NextContainer();
	void Compress(Vector &input, idx_t count);

public:
	unique_ptr<AnalyzeState> owned_analyze_state;
	RoaringAnalyzeState &analyze_state;

	ContainerCompressionState container_state;
	ContainerMetadataCollection metadata_collection;
	vector<ContainerMetadata> &container_metadata;

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing
	data_ptr_t metadata_ptr;
	//! The amount of values already compressed
	idx_t total_count = 0;
};

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

struct ContainerScanState {
public:
	ContainerScanState(idx_t container_index_p, idx_t container_size)
	    : container_index(container_index_p), container_size(container_size) {
	}
	virtual ~ContainerScanState() {
	}

public:
	virtual void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) = 0;
	virtual void Skip(idx_t count) = 0;
	virtual void Verify() const = 0;

public:
	//! The index of the container
	idx_t container_index;
	//! The size of the container (how many values does it hold)
	idx_t container_size;
	//! How much of the container is already consumed
	idx_t scanned_count = 0;
};

struct ContainerSegmentScan {
public:
	explicit ContainerSegmentScan(data_ptr_t data);
	ContainerSegmentScan(const ContainerSegmentScan &other) = delete;
	ContainerSegmentScan(ContainerSegmentScan &&other) = delete;
	ContainerSegmentScan &operator=(const ContainerSegmentScan &other) = delete;
	ContainerSegmentScan &operator=(ContainerSegmentScan &&other) = delete;

public:
	// Returns the base of the current segment, forwarding the index if the segment is depleted of values
	uint16_t operator++(int);

private:
	//! The COMPRESSED_SEGMENT_COUNT unsigned bytes indicating for each segment (256 bytes) of the container how many
	//! values are in the segment
	uint8_t *segments;
	uint8_t index;
	uint8_t count;
};

//! RUN Container

struct RunContainerScanState : public ContainerScanState {
public:
	RunContainerScanState(idx_t container_index, idx_t container_size, idx_t count, data_ptr_t data_p);

public:
	void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) override;
	void Skip(idx_t to_skip) override;
	void Verify() const override;

protected:
	virtual void LoadNextRun();

protected:
	RunContainerRLEPair run;
	bool finished = false;
	idx_t run_index = 0;
	idx_t count;
	data_ptr_t data;
};

struct CompressedRunContainerScanState : public RunContainerScanState {
public:
	CompressedRunContainerScanState(idx_t container_index, idx_t container_size, idx_t count, data_ptr_t segments,
	                                data_ptr_t data);

protected:
	void LoadNextRun() override;
	void Verify() const override;

private:
	data_ptr_t segments;
	ContainerSegmentScan segment;
};

//! ARRAY Container

template <bool INVERTED>
struct ArrayContainerScanState : public ContainerScanState {
public:
	ArrayContainerScanState(idx_t container_index, idx_t container_size, idx_t count, data_ptr_t data_p)
	    : ContainerScanState(container_index, container_size), data(data_p), count(count) {
	}

public:
	virtual void LoadNextValue() {
		if (array_index >= count) {
			finished = true;
			return;
		}
		value = reinterpret_cast<uint16_t *>(data)[array_index];
		array_index++;
	}

public:
	void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) override {
		auto &result_mask = FlatVector::Validity(result);

		// This method assumes that the validity mask starts off as having all bits set for the entries that are being
		// scanned.

		if (!INVERTED) {
			// If we are mapping valid entries, that means the majority of the bits are invalid
			// so we set everything to invalid and only flip the bits that are present in the array
			SetInvalidRange(result_mask, result_offset, result_offset + to_scan);
		}

		if (!array_index) {
			LoadNextValue();
		}
		// At least one of the entries to scan is set
		while (!finished) {
			if (value >= scanned_count + to_scan) {
				break;
			}
			if (value < scanned_count) {
				LoadNextValue();
				continue;
			}
			auto index = value - scanned_count;
			if (INVERTED) {
				result_mask.SetInvalid(result_offset + index);
			} else {
				result_mask.SetValid(result_offset + index);
			}
			LoadNextValue();
		}
		scanned_count += to_scan;
	}

	void Skip(idx_t to_skip) override {
		idx_t end = scanned_count + to_skip;
		if (!array_index) {
			LoadNextValue();
		}
		while (!finished && value < end) {
			LoadNextValue();
		}
		// In case array_index has already reached count
		scanned_count = end;
	}

	void Verify() const override {
#ifdef DEBUG
		uint16_t index = 0;
		auto array = reinterpret_cast<uint16_t *>(data);
		for (uint16_t i = 0; i < count; i++) {
			D_ASSERT(!i || array[i] > index);
			index = array[i];
		}
#endif
	}

protected:
	uint16_t value;
	data_ptr_t data;
	bool finished = false;
	const idx_t count;
	idx_t array_index = 0;
};

template <bool INVERTED>
struct CompressedArrayContainerScanState : public ArrayContainerScanState<INVERTED> {
public:
	CompressedArrayContainerScanState(idx_t container_index, idx_t container_size, idx_t count, data_ptr_t segments,
	                                  data_ptr_t data)
	    : ArrayContainerScanState<INVERTED>(container_index, container_size, count, data), segments(segments),
	      segment(segments) {
		D_ASSERT(count >= COMPRESSED_ARRAY_THRESHOLD);
	}

public:
	void LoadNextValue() override {
		if (this->array_index >= this->count) {
			this->finished = true;
			return;
		}
		this->value = segment++;
		this->value += reinterpret_cast<uint8_t *>(this->data)[this->array_index];
		this->array_index++;
	}
	void Verify() const override {
#ifdef DEBUG
		uint16_t index = 0;
		ContainerSegmentScan verify_segment(segments);
		for (uint16_t i = 0; i < this->count; i++) {
			// Get the value
			uint16_t new_index = verify_segment++;
			new_index += reinterpret_cast<uint8_t *>(this->data)[i];

			D_ASSERT(!i || new_index > index);
			index = new_index;
		}
#endif
	}

private:
	data_ptr_t segments;
	ContainerSegmentScan segment;
};

//! BITSET Container

struct BitsetContainerScanState : public ContainerScanState {
public:
	BitsetContainerScanState(idx_t container_index, idx_t count, validity_t *bitset);

public:
	void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) override;
	void Skip(idx_t to_skip) override;
	void Verify() const override;

public:
	validity_t *bitset;
};

struct RoaringScanState : public SegmentScanState {
public:
	explicit RoaringScanState(ColumnSegment &segment);

public:
	idx_t SkipVector(const ContainerMetadata &metadata);
	bool UseContainerStateCache(idx_t container_index, idx_t internal_offset);
	ContainerMetadata GetContainerMetadata(idx_t container_index);
	data_ptr_t GetStartOfContainerData(idx_t container_index);
	ContainerScanState &LoadContainer(idx_t container_index, idx_t internal_offset);
	void ScanInternal(ContainerScanState &scan_state, idx_t to_scan, Vector &result, idx_t offset);
	idx_t GetContainerIndex(idx_t start_index, idx_t &offset);
	void ScanPartial(idx_t start_idx, Vector &result, idx_t offset, idx_t count);
	void Skip(ContainerScanState &scan_state, idx_t skip_count);

public:
	BufferHandle handle;
	ColumnSegment &segment;
	unique_ptr<ContainerScanState> current_container;
	data_ptr_t data_ptr;
	ContainerMetadataCollection metadata_collection;
	vector<ContainerMetadata> container_metadata;
	vector<idx_t> data_start_position;
};

} // namespace roaring

} // namespace duckdb
