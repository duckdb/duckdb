//===----------------------------------------------------------------------===//
//                         DuckDB Delta Extension
//
// functions/delta_table_info.cpp
//
// delta_table_info and delta_file_stats table functions for V2 feature metadata
//
//===----------------------------------------------------------------------===//

#include "delta_functions.hpp"
#include "functions/delta_scan.hpp"
#include "delta_kernel_ffi_v2.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// delta_table_info - Table-level V2 metadata
//===----------------------------------------------------------------------===//

struct DeltaTableInfoBindData : public TableFunctionData {
	string path;
	unique_ptr<DeltaSnapshot> snapshot;
	bool finished = false;
};

static unique_ptr<FunctionData> DeltaTableInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DeltaTableInfoBindData>();
	result->path = input.inputs[0].GetValue<string>();
	
	// Create snapshot to read metadata
	result->snapshot = make_uniq<DeltaSnapshot>(context, result->path);
	
	// Define output schema
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("version");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("min_reader_version");
	return_types.emplace_back(LogicalType::INTEGER);
	
	names.emplace_back("min_writer_version");
	return_types.emplace_back(LogicalType::INTEGER);
	
	// V2 Features
	names.emplace_back("has_deletion_vectors");
	return_types.emplace_back(LogicalType::BOOLEAN);
	
	names.emplace_back("has_row_tracking");
	return_types.emplace_back(LogicalType::BOOLEAN);
	
	names.emplace_back("has_liquid_clustering");
	return_types.emplace_back(LogicalType::BOOLEAN);
	
	names.emplace_back("has_v2_checkpoints");
	return_types.emplace_back(LogicalType::BOOLEAN);
	
	names.emplace_back("has_timestamp_ntz");
	return_types.emplace_back(LogicalType::BOOLEAN);
	
	names.emplace_back("has_column_mapping");
	return_types.emplace_back(LogicalType::BOOLEAN);
	
	// Liquid clustering columns (as array)
	names.emplace_back("clustering_columns");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
	
	// Statistics
	names.emplace_back("total_files");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("files_with_deletion_vectors");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("total_deleted_rows");
	return_types.emplace_back(LogicalType::BIGINT);
	
	return std::move(result);
}

static void DeltaTableInfoExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<DeltaTableInfoBindData>();
	
	if (bind_data.finished) {
		return;
	}
	
	// Initialize the snapshot to get all metadata
	bind_data.snapshot->GetTotalFileCount();
	
	idx_t col = 0;
	
	// path
	output.SetValue(col++, 0, Value(bind_data.path));
	
	// version
	output.SetValue(col++, 0, Value::BIGINT(bind_data.snapshot->version));
	
	// min_reader_version
	output.SetValue(col++, 0, Value::INTEGER(bind_data.snapshot->protocol.min_reader_version));
	
	// min_writer_version
	output.SetValue(col++, 0, Value::INTEGER(bind_data.snapshot->protocol.min_writer_version));
	
	// V2 Feature flags
	output.SetValue(col++, 0, Value::BOOLEAN(bind_data.snapshot->protocol.HasDeletionVectors()));
	output.SetValue(col++, 0, Value::BOOLEAN(bind_data.snapshot->protocol.HasRowTracking()));
	output.SetValue(col++, 0, Value::BOOLEAN(bind_data.snapshot->protocol.HasLiquidClustering()));
	output.SetValue(col++, 0, Value::BOOLEAN(bind_data.snapshot->protocol.HasV2Checkpoints()));
	output.SetValue(col++, 0, Value::BOOLEAN(HasFeature(bind_data.snapshot->protocol.enabled_features, DeltaV2Feature::TIMESTAMP_NTZ)));
	output.SetValue(col++, 0, Value::BOOLEAN(HasFeature(bind_data.snapshot->protocol.enabled_features, DeltaV2Feature::COLUMN_MAPPING)));
	
	// Clustering columns
	if (bind_data.snapshot->liquid_clustering.is_enabled && !bind_data.snapshot->liquid_clustering.clustering_columns.empty()) {
		vector<Value> clustering_cols;
		for (const auto &col_name : bind_data.snapshot->liquid_clustering.clustering_columns) {
			clustering_cols.push_back(Value(col_name));
		}
		output.SetValue(col++, 0, Value::LIST(LogicalType::VARCHAR, clustering_cols));
	} else {
		output.SetValue(col++, 0, Value::EMPTYLIST(LogicalType::VARCHAR));
	}
	
	// Statistics
	output.SetValue(col++, 0, Value::BIGINT(bind_data.snapshot->GetTotalFileCount()));
	output.SetValue(col++, 0, Value::BIGINT(bind_data.snapshot->files_with_deletion_vectors));
	output.SetValue(col++, 0, Value::BIGINT(bind_data.snapshot->total_deleted_rows));
	
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunctionSet DeltaFunctions::GetDeltaTableInfoFunction(DatabaseInstance &instance) {
	TableFunctionSet function_set("delta_table_info");
	
	TableFunction table_info({LogicalType::VARCHAR}, DeltaTableInfoExecute, DeltaTableInfoBind);
	table_info.name = "delta_table_info";
	
	function_set.AddFunction(table_info);
	return function_set;
}

//===----------------------------------------------------------------------===//
// delta_file_stats - Per-file V2 metadata
//===----------------------------------------------------------------------===//

struct DeltaFileStatsBindData : public TableFunctionData {
	string path;
	unique_ptr<DeltaSnapshot> snapshot;
	idx_t current_file = 0;
};

static unique_ptr<FunctionData> DeltaFileStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DeltaFileStatsBindData>();
	result->path = input.inputs[0].GetValue<string>();
	
	// Create snapshot to read metadata
	result->snapshot = make_uniq<DeltaSnapshot>(context, result->path);
	
	// Define output schema
	names.emplace_back("file_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("file_number");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("file_size");
	return_types.emplace_back(LogicalType::BIGINT);
	
	// Deletion vector info
	names.emplace_back("has_deletion_vector");
	return_types.emplace_back(LogicalType::BOOLEAN);
	
	names.emplace_back("deleted_row_count");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("dv_storage_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("dv_size_bytes");
	return_types.emplace_back(LogicalType::INTEGER);
	
	// Row tracking info
	names.emplace_back("base_row_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("default_row_commit_version");
	return_types.emplace_back(LogicalType::BIGINT);
	
	// Partition info
	names.emplace_back("partition_values");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	
	return std::move(result);
}

static void DeltaFileStatsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<DeltaFileStatsBindData>();
	
	// Initialize if needed
	bind_data.snapshot->GetTotalFileCount();
	
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && bind_data.current_file < bind_data.snapshot->resolved_files.size()) {
		idx_t file_idx = bind_data.current_file++;
		auto &file_path = bind_data.snapshot->resolved_files[file_idx];
		auto &metadata = bind_data.snapshot->metadata[file_idx];
		
		idx_t col = 0;
		
		// file_path
		output.SetValue(col++, count, Value(file_path));
		
		// file_number
		output.SetValue(col++, count, Value::BIGINT(metadata->file_number));
		
		// file_size
		output.SetValue(col++, count, Value::BIGINT(metadata->file_size));
		
		// Deletion vector info
		bool has_dv = metadata->selection_vector.ptr != nullptr;
		output.SetValue(col++, count, Value::BOOLEAN(has_dv));
		output.SetValue(col++, count, Value::BIGINT(metadata->deletion_vector_info.cardinality));
		
		// DV storage type
		if (metadata->deletion_vector_info.storage_type != '\0') {
			string storage_type_str;
			switch (metadata->deletion_vector_info.storage_type) {
				case 'u': storage_type_str = "uuid"; break;
				case 'i': storage_type_str = "inline"; break;
				case 'p': storage_type_str = "path"; break;
				default: storage_type_str = "unknown"; break;
			}
			output.SetValue(col++, count, Value(storage_type_str));
		} else {
			output.SetValue(col++, count, Value());
		}
		
		// DV size bytes
		output.SetValue(col++, count, Value::INTEGER(metadata->deletion_vector_info.size_in_bytes));
		
		// Row tracking info
		if (metadata->row_tracking.base_row_id >= 0) {
			output.SetValue(col++, count, Value::BIGINT(metadata->row_tracking.base_row_id));
		} else {
			output.SetValue(col++, count, Value());
		}
		
		if (metadata->row_tracking.default_row_commit_version >= 0) {
			output.SetValue(col++, count, Value::BIGINT(metadata->row_tracking.default_row_commit_version));
		} else {
			output.SetValue(col++, count, Value());
		}
		
		// Partition values as MAP
		if (!metadata->partition_map.empty()) {
			vector<Value> keys;
			vector<Value> values;
			for (const auto &kv : metadata->partition_map) {
				keys.push_back(Value(kv.first));
				values.push_back(Value(kv.second));
			}
			output.SetValue(col++, count, Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, keys, values));
		} else {
			output.SetValue(col++, count, Value::EMPTYMAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
		}
		
		count++;
	}
	
	output.SetCardinality(count);
}

TableFunctionSet DeltaFunctions::GetDeltaFileStatsFunction(DatabaseInstance &instance) {
	TableFunctionSet function_set("delta_file_stats");
	
	TableFunction file_stats({LogicalType::VARCHAR}, DeltaFileStatsExecute, DeltaFileStatsBind);
	file_stats.name = "delta_file_stats";
	
	function_set.AddFunction(file_stats);
	return function_set;
}

} // namespace duckdb


