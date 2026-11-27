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
#include "duckdb/planner/expression.hpp"

namespace duckdb::capiv2 {

//! The definition of a data source: the physical operator source of a scan. It holds the callbacks that set up the
//! scan, hand out its work to the scanning threads and produce its batches of rows. Everything that is decided at plan
//! time (binding, filter and projection pushdown, statistics, partitioning of the plan) belongs to the table function
//! that scans with it.
class CV2DataSourceInfo {
public:
	duckdb_v2_data_source_init_global_callback_fn init_global_cb = nullptr;
	duckdb_v2_data_source_init_local_callback_fn init_local_cb = nullptr;
	duckdb_v2_data_source_exec_callback_fn exec_cb = nullptr;
	duckdb_v2_data_source_progress_callback_fn progress_cb = nullptr;
	duckdb_v2_data_source_partition_data_callback_fn partition_data_cb = nullptr;
	duckdb_v2_data_source_claim_batch_callback_fn claim_batch_cb = nullptr;
	duckdb_v2_data_source_metrics_callback_fn metrics_cb = nullptr;
	//! What the callbacks of the data source receive as their user data
	shared_ptr<CV2UserData> user_data;
	//! The name of the function the data source scans for, which error messages refer to
	Identifier name;

	//! Throws unless the configuration is complete and consistent
	void Verify() const;
	//! Takes `fallback` as its user data unless it has some: user data with a null pointer counts as none
	void InheritUserData(const shared_ptr<CV2UserData> &fallback) {
		if (!user_data || !user_data->GetData()) {
			user_data = fallback;
		}
	}

	//! Makes the function scan with this data source. The data source must outlive every call of the function, as the
	//! bind data of a call refers to it.
	void ConfigureScan(TableFunction &function) const;
};

//! What the table function decided at plan time that changes how its scan runs
class CV2ScanOptions {
public:
	//! Whether the scan only produces the columns the query reads
	bool projection_pushdown = false;
	//! The callback that is offered the filters of the query, and the user data it receives
	duckdb_v2_table_function_filter_pushdown_callback_fn filter_pushdown_cb = nullptr;
	shared_ptr<CV2UserData> user_data;

	//! Whether the scan sees the filters of the query: there is a filter pushdown callback and projection pushdown
	bool ReadsFilters() const {
		return filter_pushdown_cb && projection_pushdown;
	}
};

//! The bind data of a call whose scan is a data source. Besides the user's own bind data it carries borrowed pointers
//! to the data source and the scan options, which the scan hooks that only receive the bind data (e.g. progress) reach
//! their callbacks through.
class CV2DataSourceBindData : public FunctionData {
public:
	shared_ptr<CV2UserData> handle;
	optional_ptr<const CV2DataSourceInfo> source;
	optional_ptr<const CV2ScanOptions> scan;
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
		other.scan = scan;
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

//! What a filter pushdown callback is offered: at plan time by the optimizer, and when a scan starts
class CV2TableFilterPushdownInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	// The predicates the optimizer offers, borrowed from the engine for the duration of the callback.
	const vector<unique_ptr<Expression>> *in_filters = nullptr;
	// What the column references inside the predicates index: the scan's columns, as the function sees them.
	vector<column_t> in_column_ids;

	vector<bool> out_handled;
};

inline auto Convert(duckdb_v2_table_function_filter_pushdown_info_handle info) -> CV2TableFilterPushdownInfo * {
	return reinterpret_cast<CV2TableFilterPushdownInfo *>(info);
}
inline auto Convert(CV2TableFilterPushdownInfo *info) -> duckdb_v2_table_function_filter_pushdown_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(info);
}

//! The index the function sees for a column of the scan. A virtual column of a function wrapped in a multi-file
//! function has an index of its own past the declared columns, and the wrapper tells which virtual column that is.
auto CV2ScanColumnIndex(column_t column, optional_ptr<const unordered_map<column_t, column_t>> virtual_columns)
    -> column_t;

//! The scan hooks of a table function that scans with a data source
auto CV2DataSourceInitGlobal(ClientContext &context, TableFunctionInitInput &input)
    -> unique_ptr<GlobalTableFunctionState>;
auto CV2DataSourceInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                            GlobalTableFunctionState *global_state) -> unique_ptr<LocalTableFunctionState>;
auto CV2DataSourceExec(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void;
//! Claims the next batch for the scanning thread, for a multi-file scan that claims the batches itself
auto CV2DataSourceClaimBatch(ClientContext &context, TableFunctionInput &input) -> bool;

} // namespace duckdb::capiv2
