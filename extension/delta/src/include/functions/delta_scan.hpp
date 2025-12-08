//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/delta_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "delta_utils.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Delta V2 Feature Flags
//===----------------------------------------------------------------------===//

enum class DeltaV2Feature : uint32_t {
	NONE = 0,
	DELETION_VECTORS = 1 << 0,
	ROW_TRACKING = 1 << 1,
	LIQUID_CLUSTERING = 1 << 2,
	V2_CHECKPOINTS = 1 << 3,
	TIMESTAMP_NTZ = 1 << 4,
	COLUMN_MAPPING = 1 << 5
};

inline DeltaV2Feature operator|(DeltaV2Feature a, DeltaV2Feature b) {
	return static_cast<DeltaV2Feature>(static_cast<uint32_t>(a) | static_cast<uint32_t>(b));
}

inline DeltaV2Feature operator&(DeltaV2Feature a, DeltaV2Feature b) {
	return static_cast<DeltaV2Feature>(static_cast<uint32_t>(a) & static_cast<uint32_t>(b));
}

inline bool HasFeature(DeltaV2Feature flags, DeltaV2Feature feature) {
	return (static_cast<uint32_t>(flags) & static_cast<uint32_t>(feature)) != 0;
}

//===----------------------------------------------------------------------===//
// Row Tracking Information
//===----------------------------------------------------------------------===//

struct DeltaRowTrackingInfo {
	//! Base row ID for this file (from fileConstantValues.baseRowId)
	//! Set to -1 if not available
	int64_t base_row_id = -1;
	//! Default row commit version (from fileConstantValues.defaultRowCommitVersion)
	//! Set to -1 if not available
	int64_t default_row_commit_version = -1;
	
	bool HasRowTracking() const {
		return base_row_id >= 0 || default_row_commit_version >= 0;
	}
};

//===----------------------------------------------------------------------===//
// Deletion Vector Information
//===----------------------------------------------------------------------===//

struct DeltaDeletionVectorInfo {
	//! Number of deleted rows in this file
	int64_t cardinality = 0;
	//! Storage type: 'u' (UUID), 'i' (inline), 'p' (path)
	char storage_type = '\0';
	//! Size of the deletion vector in bytes
	int32_t size_in_bytes = 0;
	
	bool HasDeletionVector() const {
		return cardinality > 0;
	}
};

//===----------------------------------------------------------------------===//
// Delta File Metadata (V2 Enhanced)
//===----------------------------------------------------------------------===//

struct DeltaFileMetaData {
	DeltaFileMetaData() {};

	// No copying pls
	DeltaFileMetaData(const DeltaFileMetaData &) = delete;
	DeltaFileMetaData &operator=(const DeltaFileMetaData &) = delete;

	~DeltaFileMetaData() {
		if (selection_vector.ptr) {
			ffi::drop_bool_slice(selection_vector);
		}
	}

	//! Snapshot version this file belongs to
	idx_t delta_snapshot_version = DConstants::INVALID_INDEX;
	//! File number/index within the snapshot
	idx_t file_number = DConstants::INVALID_INDEX;
	//! Selection vector for deletion vector filtering (rows NOT deleted are true)
	ffi::KernelBoolSlice selection_vector = {nullptr, 0};
	//! Partition column values for this file
	case_insensitive_map_t<string> partition_map;
	
	//===----------------------------------------------------------------------===//
	// V2 Features
	//===----------------------------------------------------------------------===//
	
	//! Row tracking information
	DeltaRowTrackingInfo row_tracking;
	//! Deletion vector information
	DeltaDeletionVectorInfo deletion_vector_info;
	//! File size in bytes
	int64_t file_size = -1;
};

//===----------------------------------------------------------------------===//
// Liquid Clustering Configuration
//===----------------------------------------------------------------------===//

struct DeltaLiquidClusteringConfig {
	//! Whether liquid clustering is enabled
	bool is_enabled = false;
	//! Clustering column names
	vector<string> clustering_columns;
	
	//! Check if a column is used for clustering
	bool IsClusteringColumn(const string &column_name) const {
		for (const auto &col : clustering_columns) {
			if (StringUtil::CIEquals(col, column_name)) {
				return true;
			}
		}
		return false;
	}
};

//===----------------------------------------------------------------------===//
// Table Protocol V2 Information
//===----------------------------------------------------------------------===//

struct DeltaTableProtocol {
	//! Minimum reader version required
	int32_t min_reader_version = 1;
	//! Minimum writer version required
	int32_t min_writer_version = 1;
	//! Enabled V2 features
	DeltaV2Feature enabled_features = DeltaV2Feature::NONE;
	
	bool HasDeletionVectors() const { return HasFeature(enabled_features, DeltaV2Feature::DELETION_VECTORS); }
	bool HasRowTracking() const { return HasFeature(enabled_features, DeltaV2Feature::ROW_TRACKING); }
	bool HasLiquidClustering() const { return HasFeature(enabled_features, DeltaV2Feature::LIQUID_CLUSTERING); }
	bool HasV2Checkpoints() const { return HasFeature(enabled_features, DeltaV2Feature::V2_CHECKPOINTS); }
};

//===----------------------------------------------------------------------===//
// Delta Snapshot (V2 Enhanced)
//===----------------------------------------------------------------------===//

//! The DeltaSnapshot implements the MultiFileList API to allow injecting it into the regular DuckDB parquet scan
struct DeltaSnapshot : public MultiFileList {
	DeltaSnapshot(ClientContext &context, const string &path);
	string GetPath();
	static string ToDuckDBPath(const string &raw_path);
	static string ToDeltaPath(const string &raw_path);

	//! MultiFileList API
public:
	void Bind(vector<LogicalType> &return_types, vector<string> &names);
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                LogicalGet &get, vector<unique_ptr<Expression>> &filters) override;
	vector<string> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;

protected:
	//! Get the i-th expanded file
	string GetFile(idx_t i) override;

protected:
	// TODO: How to guarantee we only call this after the filter pushdown?
	void InitializeFiles();
	
	//! Initialize V2 feature metadata from the snapshot
	void InitializeV2Features();

	template <class T>
	T TryUnpackKernelResult(ffi::ExternResult<T> result) {
		return KernelUtils::UnpackResult<T>(
		    result, StringUtil::Format("While trying to read from delta table: '%s'", paths[0]));
	}

	// TODO: change back to protected
public:
	idx_t version;

	//! Delta Kernel Structures
	KernelSnapshot snapshot;
	KernelExternEngine extern_engine;
	KernelScan scan;
	KernelGlobalScanState global_state;
	KernelScanDataIterator scan_data_iterator;

	//! Names
	vector<string> names;

	//! Metadata map for files
	vector<unique_ptr<DeltaFileMetaData>> metadata;

	//! Current file list resolution state
	bool initialized = false;
	bool files_exhausted = false;
	vector<string> resolved_files;
	TableFilterSet table_filters;

	ClientContext &context;
	
	//===----------------------------------------------------------------------===//
	// V2 Features - Table Level
	//===----------------------------------------------------------------------===//
	
	//! Table protocol information
	DeltaTableProtocol protocol;
	//! Liquid clustering configuration
	DeltaLiquidClusteringConfig liquid_clustering;
	//! Whether V2 features have been initialized
	bool v2_features_initialized = false;
	//! Total deleted rows across all files (for statistics)
	int64_t total_deleted_rows = 0;
	//! Number of files with deletion vectors
	idx_t files_with_deletion_vectors = 0;
};

struct DeltaMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
	DeltaMultiFileReaderGlobalState(vector<LogicalType> extra_columns_p, optional_ptr<const MultiFileList> file_list_p)
	    : MultiFileReaderGlobalState(extra_columns_p, file_list_p) {
	}
	//! The idx of the file number column in the result chunk
	idx_t delta_file_number_idx = DConstants::INVALID_INDEX;
	//! The idx of the file_row_number column in the result chunk
	idx_t file_row_number_idx = DConstants::INVALID_INDEX;
	
	//===----------------------------------------------------------------------===//
	// V2 Row Tracking Column Indices
	//===----------------------------------------------------------------------===//
	
	//! The idx of the base_row_id column in the result chunk (V2 row tracking)
	idx_t base_row_id_idx = DConstants::INVALID_INDEX;
	//! The idx of the default_row_commit_version column in the result chunk (V2 row tracking)
	idx_t row_commit_version_idx = DConstants::INVALID_INDEX;
	//! The idx of the deletion_vector_cardinality column in the result chunk (V2 deletion vectors)
	idx_t dv_cardinality_idx = DConstants::INVALID_INDEX;

	void SetColumnIdx(const string &column, idx_t idx);
};

struct DeltaMultiFileReader : public MultiFileReader {
	static unique_ptr<MultiFileReader> CreateInstance();
	//! Return a DeltaSnapshot
	unique_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         FileGlobOptions options) override;

	//! Override the regular parquet bind using the MultiFileReader Bind. The bind from these are what DuckDB's file
	//! readers will try read
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types, vector<string> &names,
	          MultiFileReaderBindData &bind_data) override;

	//! Override the Options bind
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<string> &names, MultiFileReaderBindData &bind_data) override;

	void CreateNameMapping(const string &file_name, const vector<LogicalType> &local_types,
	                       const vector<string> &local_names, const vector<LogicalType> &global_types,
	                       const vector<string> &global_names, const vector<column_t> &global_column_ids,
	                       MultiFileReaderData &reader_data, const string &initial_file,
	                       optional_ptr<MultiFileReaderGlobalState> global_state) override;

	unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
	                      const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
	                      const vector<LogicalType> &global_types, const vector<string> &global_names,
	                      const vector<column_t> &global_column_ids) override;

	void FinalizeBind(const MultiFileOptions &file_options, const MultiFileReaderBindData &options,
	                  const string &filename, const vector<string> &local_names,
	                  const vector<LogicalType> &global_types, const vector<string> &global_names,
	                  const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data,
	                  ClientContext &context, optional_ptr<MultiFileReaderGlobalState> global_state) override;

	//! Override the FinalizeChunk method
	void FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
	                   const MultiFileReaderData &reader_data, DataChunk &chunk,
	                   optional_ptr<MultiFileReaderGlobalState> global_state) override;

	//! Override the ParseOption call to parse delta_scan specific options
	bool ParseOption(const string &key, const Value &val, MultiFileOptions &options, ClientContext &context) override;
};

} // namespace duckdb
