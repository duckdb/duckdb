//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi_v2/capi_v2_data_source_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb::capiv2 {

//! The definition of a data source: what scans a source of rows like a physical operator source does. It holds the
//! callbacks that set up the scan, produce its batches of rows, and describe how it can be partitioned and optimized,
//! none of which depend on how the call that scans it was bound. A table function takes a data source as its scan.
class CV2DataSourceInfo {
public:
	duckdb_v2_table_function_init_global_callback_fn init_global_cb = nullptr;
	duckdb_v2_table_function_init_local_callback_fn init_local_cb = nullptr;
	duckdb_v2_table_function_exec_callback_fn exec_cb = nullptr;
	duckdb_v2_table_function_progress_callback_fn progress_cb = nullptr;
	duckdb_v2_table_function_filter_pushdown_callback_fn filter_pushdown_cb = nullptr;
	duckdb_v2_table_function_partition_data_callback_fn partition_data_cb = nullptr;
	duckdb_v2_table_function_partitioning_callback_fn partitioning_cb = nullptr;
	duckdb_v2_table_function_claim_batch_callback_fn claim_batch_cb = nullptr;
	duckdb_v2_table_function_metrics_callback_fn metrics_cb = nullptr;
	//! What the callbacks of the data source receive as their user data
	shared_ptr<CV2UserData> user_data;
	bool projection_pushdown = false;
	//! Whether the data source can produce the file_row_number virtual column
	bool file_row_number = false;
	//! The name of the function the data source scans for, which error messages refer to
	Identifier name;

	//! Whether the data source sees the filters of the scan: it has a filter pushdown callback and projection pushdown
	bool ReadsFilters() const {
		return filter_pushdown_cb && projection_pushdown;
	}

	//! Throws unless the configuration is complete and consistent
	void Verify() const;

	//! Makes the function scan with this data source. The data source must outlive every call of the function, as the
	//! bind data of a call refers to it.
	void ConfigureScan(TableFunction &function) const;
};

//! The bind data of a call whose scan is a data source. Besides the user's own bind data it carries a borrowed pointer
//! to the data source, which the scan hooks that only receive the bind data (e.g. progress) reach their callbacks
//! through.
class CV2DataSourceBindData : public FunctionData {
public:
	shared_ptr<CV2UserData> handle;
	optional_ptr<const CV2DataSourceInfo> source;
	//! The declared columns the data source produces
	vector<LogicalType> column_types;

	auto Equals(const FunctionData &other) const -> bool override {
		const auto &other_data = other.Cast<CV2DataSourceBindData>();
		return handle && other_data.handle && handle->Equals(*other_data.handle);
	}

protected:
	void CopyTo(CV2DataSourceBindData &other) const {
		other.handle = handle;
		other.source = source;
		other.column_types = column_types;
	}
};

//! A data source being built, created through a factory
class CV2DataSource {
public:
	explicit CV2DataSource(DatabaseInstance &db) : db(db) {
	}

public:
	//! The database it was created for: the only one it can be used on.
	DatabaseInstance &db;
	CV2DataSourceInfo info;
};

inline auto Convert(duckdb_v2_data_source_handle source) -> CV2DataSource * {
	return reinterpret_cast<CV2DataSource *>(source);
}
inline auto Convert(CV2DataSource *source) -> duckdb_v2_data_source_handle {
	return reinterpret_cast<duckdb_v2_data_source_handle>(source);
}

//! The scan hooks of a table function that scans with a data source
auto CV2DataSourceInitGlobal(ClientContext &context, TableFunctionInitInput &input)
    -> unique_ptr<GlobalTableFunctionState>;
auto CV2DataSourceInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                            GlobalTableFunctionState *global_state) -> unique_ptr<LocalTableFunctionState>;
auto CV2DataSourceExec(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void;
//! Claims the next batch for the scanning thread, for a multi-file scan that claims the batches itself
auto CV2DataSourceClaimBatch(ClientContext &context, TableFunctionInput &input) -> bool;

} // namespace duckdb::capiv2
