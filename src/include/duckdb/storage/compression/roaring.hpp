//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/roaring.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

namespace roaring {

//! The amount of values that are encoded per container
static constexpr idx_t ROARING_CONTAINER_SIZE = 2048;
static constexpr bool NULLS = true;
static constexpr bool NON_NULLS = false;
static constexpr uint16_t UNCOMPRESSED_SIZE = (ROARING_CONTAINER_SIZE / sizeof(validity_t));
static constexpr uint16_t COMPRESSED_SEGMENT_SIZE = 256;
static constexpr uint16_t COMPRESSED_SEGMENT_COUNT = (ROARING_CONTAINER_SIZE / COMPRESSED_SEGMENT_SIZE);

static constexpr uint16_t MAX_RUN_IDX = (UNCOMPRESSED_SIZE - COMPRESSED_SEGMENT_COUNT) / (sizeof(uint8_t) * 2);
static constexpr uint16_t MAX_ARRAY_IDX = (UNCOMPRESSED_SIZE - COMPRESSED_SEGMENT_COUNT) / (sizeof(uint8_t) * 1);
static constexpr uint16_t COMPRESSED_ARRAY_THRESHOLD = 60;
static constexpr uint16_t COMPRESSED_RUN_THRESHOLD = 30;

static constexpr uint16_t CONTAINER_TYPE_BITWIDTH = 2;
static constexpr uint16_t RUN_CONTAINER_SIZE_BITWIDTH = 7;
static constexpr uint16_t ARRAY_CONTAINER_SIZE_BITWIDTH = 8;

static_assert((1 << RUN_CONTAINER_SIZE_BITWIDTH) - 1 >= MAX_RUN_IDX,
              "The bitwidth used to store the size of a run container has to be big enough to store the maximum size");
static_assert(
    (1 << ARRAY_CONTAINER_SIZE_BITWIDTH) - 1 >= MAX_ARRAY_IDX + 1,
    "The bitwidth used to store the size of an array/bitset container has to be big enough to store the maximum size");

static void SetInvalidRange(ValidityMask &result, idx_t start, idx_t end);

struct RunContainerRLEPair {
	uint16_t start;
	uint16_t length;
};

struct ContainerMetadata {
public:
	ContainerMetadata();
	ContainerMetadata(bool is_inverted, bool is_run, uint16_t value);

public:
	bool operator==(const ContainerMetadata &other) {
		return memcmp(&data, &other.data, sizeof(uint16_t)) == 0;
	}
	bool IsRun() const;
	bool IsUncompressed() const;
	bool IsInverted() const;
	uint16_t NumberOfRuns() const;
	uint16_t Cardinality() const;
	idx_t GetDataSizeInBytes(idx_t container_size) const;

public:
	union {
		struct {
			uint16_t unused : 1;          //! Currently unused bits
			uint16_t is_inverted : 1;     //! Indicate if this maps nulls or non-nulls
			uint16_t is_run : 1;          //! Indicate if this is a run container
			uint16_t unused2 : 2;         //! Currently unused bits
			uint16_t number_of_runs : 11; //! The number of runs
		} run_container;

		struct {
			uint16_t unused : 1;       //! Currently unused bits
			uint16_t is_inverted : 1;  //! Indicate if this maps nulls or non-nulls
			uint16_t is_run : 1;       //! Indicate if this is a run container
			uint16_t cardinality : 13; //! How many values are set (4096)
		} non_run_container;
	} data;
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
	RunContainerScanState(idx_t container_index, idx_t container_size, idx_t count, data_ptr_t data_p)
	    : ContainerScanState(container_index, container_size), count(count), data(data_p) {
	}

public:
	void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) override {
		auto &result_mask = FlatVector::Validity(result);

		// This method assumes that the validity mask starts off as having all bits set for the entries that are being
		// scanned.

		idx_t result_idx = 0;
		if (!run_index) {
			LoadNextRun();
		}
		while (!finished && result_idx < to_scan) {
			// Either we are already inside a run, then 'start_of_run' will be scanned_count
			// or we're skipping values until the run begins
			auto start_of_run =
			    MaxValue<idx_t>(MinValue<idx_t>(run.start, scanned_count + to_scan), scanned_count + result_idx);
			result_idx = start_of_run - scanned_count;

			// How much of the run are we covering?
			idx_t run_end = run.start + 1 + run.length;
			auto run_or_scan_end = MinValue<idx_t>(run_end, scanned_count + to_scan);

			// Process the run
			D_ASSERT(run_or_scan_end >= start_of_run);
			if (run_or_scan_end > start_of_run) {
				idx_t amount = run_or_scan_end - start_of_run;
				idx_t start = result_offset + result_idx;
				idx_t end = start + amount;
				SetInvalidRange(result_mask, start, end);
			}

			result_idx += run_or_scan_end - start_of_run;
			if (scanned_count + result_idx == run_end) {
				// Fully processed the current run
				LoadNextRun();
			}
		}
		scanned_count += to_scan;
	}
	void Skip(idx_t to_skip) override {
		idx_t end = scanned_count + to_skip;
		if (!run_index) {
			LoadNextRun();
		}
		while (scanned_count < end && !finished) {
			idx_t run_end = run.start + 1 + run.length;
			scanned_count = MinValue<idx_t>(run_end, end);
			if (scanned_count == run_end) {
				LoadNextRun();
			}
		}
		// In case run_index has already reached count
		scanned_count = end;
	}
	virtual void Verify() const override {
#ifdef DEBUG
		uint16_t index = 0;
		for (idx_t i = 0; i < count; i++) {
			auto run = reinterpret_cast<RunContainerRLEPair *>(data)[i];
			D_ASSERT(run.start >= index);
			index = run.start + 1 + run.length;
		}
#endif
	}

protected:
	virtual void LoadNextRun() {
		if (run_index >= count) {
			finished = true;
			return;
		}
		run = reinterpret_cast<RunContainerRLEPair *>(data)[run_index];
		run_index++;
	}

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
	                                data_ptr_t data)
	    : RunContainerScanState(container_index, container_size, count, data), segments(segments), segment(segments) {
		D_ASSERT(count >= COMPRESSED_RUN_THRESHOLD);
	}

public:
	void LoadNextRun() override {
		if (run_index >= count) {
			finished = true;
			return;
		}
		uint16_t start = segment++;
		start += reinterpret_cast<uint8_t *>(data)[(run_index * 2) + 0];

		uint16_t end = segment++;
		end += reinterpret_cast<uint8_t *>(data)[(run_index * 2) + 1];

		D_ASSERT(end > start);
		run = RunContainerRLEPair {start, static_cast<uint16_t>(end - 1 - start)};
		run_index++;
	}

	void Verify() const override {
#ifdef DEBUG
		uint16_t index = 0;
		ContainerSegmentScan verify_segment(segments);
		for (idx_t i = 0; i < count; i++) {
			// Get the start index of the run
			uint16_t start = verify_segment++;
			start += reinterpret_cast<uint8_t *>(data)[(i * 2) + 0];

			// Get the end index of the run
			uint16_t end = verify_segment++;
			end += reinterpret_cast<uint8_t *>(data)[(i * 2) + 1];

			D_ASSERT(!i || start >= index);
			D_ASSERT(end > start);
			index = end;
		}
#endif
	}

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

	virtual void Verify() const override {
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

} // namespace roaring

} // namespace duckdb
