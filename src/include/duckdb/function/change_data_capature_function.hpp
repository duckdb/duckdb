//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/change_data_capture_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"

namespace duckdb {

struct ChangeDataCapture {
	duckdb_change_data_capture_callback_t function = nullptr;
	bool IsEnabled() const { return function != nullptr; }
	void EmitChange(
		cdc_event_type type,
		idx_t transactionId,
		idx_t column_count,
		idx_t table_version,
		idx_t *updated_column_index,
		const char *table_name,
		const char **column_names,
		idx_t *column_versions,
		duckdb_data_chunk values,
		duckdb_data_chunk previous_values) const;
};

} // namespace duckdb
