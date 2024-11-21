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

struct ContainerMetadata {
public:
	ContainerMetadata();
	ContainerMetadata(bool is_inverted, bool is_run, uint16_t value);

public:
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

} // namespace roaring

} // namespace duckdb
