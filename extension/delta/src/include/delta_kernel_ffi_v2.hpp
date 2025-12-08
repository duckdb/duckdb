//===----------------------------------------------------------------------===//
//                         DuckDB Delta Extension
//
// delta_kernel_ffi_v2.hpp
//
// FFI declarations for Delta Lake V2 features
// These functions should be implemented in delta-kernel-rs and exposed through the FFI
//
//===----------------------------------------------------------------------===//

#pragma once

#include "delta_kernel_ffi.hpp"

namespace ffi {

//===----------------------------------------------------------------------===//
// Deletion Vector V2 APIs
//===----------------------------------------------------------------------===//

/// Extended DvInfo with additional V2 metadata
/// Note: This extends the existing DvInfo from delta-kernel-rs
struct DvInfoV2 {
	/// The original DvInfo
	const DvInfo *base_dv_info;
	/// Storage type: 'u' (UUID), 'i' (inline), 'p' (path)
	char storage_type;
	/// Size in bytes (for UUID/path storage)
	int32_t size_in_bytes;
	/// Cardinality (number of deleted rows)
	int64_t cardinality;
	/// Offset into the file (for UUID storage)
	int32_t offset;
};

/// Check if a snapshot has any files with deletion vectors
/// Returns: true if any file in the snapshot has a deletion vector
extern "C" bool snapshot_has_deletion_vectors(SharedSnapshot *snapshot);

/// Get deletion vector cardinality (number of deleted rows) for a DvInfo
/// Returns: Number of deleted rows, or 0 if no deletion vector
extern "C" int64_t get_dv_cardinality(const DvInfo *dv_info);

/// Check if a specific row index is deleted
/// Returns: true if the row at row_index is deleted
extern "C" bool dv_contains_row(const KernelBoolSlice *selection_vector, uint64_t row_index);

/// Get total rows affected by deletion vectors in a scan
extern "C" int64_t scan_total_deleted_rows(SharedScan *scan);

//===----------------------------------------------------------------------===//
// Row Tracking V2 APIs
//===----------------------------------------------------------------------===//

/// Row tracking information for a file
struct RowTrackingInfo {
	/// Base row ID for this file (from fileConstantValues.baseRowId)
	/// -1 if not available
	int64_t base_row_id;
	/// Default row commit version (from fileConstantValues.defaultRowCommitVersion)
	/// -1 if not available  
	int64_t default_row_commit_version;
	/// Whether row tracking is enabled for this table
	bool is_enabled;
};

/// Get row tracking information from DvInfo
/// The DvInfo contains the file metadata including row tracking fields
extern "C" ExternResult<RowTrackingInfo> get_row_tracking_info(
    const DvInfo *dv_info,
    SharedExternEngine *engine,
    SharedGlobalScanState *state);

/// Check if row tracking is enabled for the table
extern "C" bool snapshot_has_row_tracking(SharedSnapshot *snapshot);

/// Callback type for visiting row tracking info during scan
typedef void (*RowTrackingCallback)(
    NullableCvoid engine_context,
    KernelStringSlice path,
    int64_t base_row_id,
    int64_t default_row_commit_version);

/// Extended scan data visitor that includes row tracking
extern "C" void visit_scan_data_with_row_tracking(
    EngineData *engine_data,
    KernelBoolSlice selection_vec,
    NullableCvoid engine_context,
    void (*callback)(NullableCvoid engine_context,
                     KernelStringSlice path,
                     int64_t size,
                     const DvInfo *dv_info,
                     const CStringMap *partition_values,
                     int64_t base_row_id,
                     int64_t default_row_commit_version));

//===----------------------------------------------------------------------===//
// Liquid Clustering V2 APIs
//===----------------------------------------------------------------------===//

/// Liquid clustering configuration for a table
struct LiquidClusteringConfig {
	/// Number of clustering columns
	uintptr_t num_columns;
	/// Whether liquid clustering is enabled
	bool is_enabled;
};

/// Check if liquid clustering is enabled for the table
extern "C" bool snapshot_has_liquid_clustering(SharedSnapshot *snapshot);

/// Get liquid clustering configuration
extern "C" ExternResult<LiquidClusteringConfig> get_liquid_clustering_config(SharedSnapshot *snapshot);

/// Get clustering column name by index
/// Returns a KernelStringSlice with the column name
extern "C" ExternResult<KernelStringSlice> get_clustering_column_name(
    SharedSnapshot *snapshot,
    uintptr_t column_index);

/// Check if a column is used for clustering
extern "C" bool is_clustering_column(
    SharedSnapshot *snapshot,
    KernelStringSlice column_name);

/// Iterator for clustering columns
struct ClusteringColumnIterator;

/// Create an iterator over clustering column names
extern "C" ExternResult<ClusteringColumnIterator*> get_clustering_columns_iterator(SharedSnapshot *snapshot);

/// Get next clustering column name from iterator
/// Returns empty slice when exhausted
extern "C" KernelStringSlice clustering_columns_next(ClusteringColumnIterator *iter);

/// Free the clustering column iterator
extern "C" void drop_clustering_columns_iterator(ClusteringColumnIterator *iter);

//===----------------------------------------------------------------------===//
// Table Protocol V2 APIs
//===----------------------------------------------------------------------===//

/// Protocol features supported by the table
struct TableProtocolV2 {
	/// Minimum reader version
	int32_t min_reader_version;
	/// Minimum writer version
	int32_t min_writer_version;
	/// Reader features (bitmask)
	/// Bit 0: deletionVectors
	/// Bit 1: columnMapping
	/// Bit 2: timestampNtz
	/// Bit 3: v2Checkpoint
	/// Bit 4: vacuumProtocolCheck
	/// Bit 5: variantType
	uint32_t reader_features;
	/// Writer features (bitmask)
	uint32_t writer_features;
};

/// Feature flag constants for reader_features/writer_features
constexpr uint32_t FEATURE_DELETION_VECTORS = 1 << 0;
constexpr uint32_t FEATURE_COLUMN_MAPPING = 1 << 1;
constexpr uint32_t FEATURE_TIMESTAMP_NTZ = 1 << 2;
constexpr uint32_t FEATURE_V2_CHECKPOINT = 1 << 3;
constexpr uint32_t FEATURE_VACUUM_PROTOCOL_CHECK = 1 << 4;
constexpr uint32_t FEATURE_VARIANT_TYPE = 1 << 5;
constexpr uint32_t FEATURE_ROW_TRACKING = 1 << 6;
constexpr uint32_t FEATURE_LIQUID_CLUSTERING = 1 << 7;

/// Get table protocol information
extern "C" ExternResult<TableProtocolV2> get_table_protocol_v2(SharedSnapshot *snapshot);

/// Check if a specific reader feature is enabled
extern "C" bool has_reader_feature(SharedSnapshot *snapshot, uint32_t feature_flag);

/// Check if a specific writer feature is enabled  
extern "C" bool has_writer_feature(SharedSnapshot *snapshot, uint32_t feature_flag);

//===----------------------------------------------------------------------===//
// Domain Metadata APIs (for Liquid Clustering)
//===----------------------------------------------------------------------===//

/// Get domain metadata by domain name
extern "C" ExternResult<KernelStringSlice> get_domain_metadata(
    SharedSnapshot *snapshot,
    KernelStringSlice domain_name);

/// Check if domain metadata exists
extern "C" bool has_domain_metadata(
    SharedSnapshot *snapshot, 
    KernelStringSlice domain_name);

} // namespace ffi


