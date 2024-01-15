//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/scanner/column_count_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/buffer_manager/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/state_machine/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/string_value_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/base_scanner.hpp"

namespace duckdb {

class ColumnCountResult : public ScannerResult {
public:
	ColumnCountResult(CSVStates &states, CSVStateMachine &state_machine);
	inline idx_t &operator[](size_t index) {
		return column_counts[index];
	}

	idx_t column_counts[STANDARD_VECTOR_SIZE];
	idx_t current_column_count = 0;
	bool error = false;
	bool last_value_always_empty = true;

	//! Adds a Value to the result
	static inline void AddValue(ColumnCountResult &result, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(ColumnCountResult &result, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline void InvalidState(ColumnCountResult &result);
	//! Handles EmptyLine states
	static inline bool EmptyLine(ColumnCountResult &result, const idx_t buffer_pos);
	inline void InternalAddRow();
};

//! Scanner that goes over the CSV and figures out how many columns each row has. Used for dialect sniffing
class ColumnCountScanner : public BaseScanner {
public:
	ColumnCountScanner(shared_ptr<CSVBufferManager> buffer_manager, const shared_ptr<CSVStateMachine> &state_machine,
	                   shared_ptr<CSVErrorHandler> error_handler);

	~ColumnCountScanner() {
	}

	ColumnCountResult &ParseChunk() override;

	ColumnCountResult &GetResult() override;

	unique_ptr<StringValueScanner> UpgradeToStringValueScanner();

private:
	void Initialize() override;

	void Process() override;

	void FinalizeChunkProcess() override;

	ColumnCountResult result;

	idx_t column_count;
};

} // namespace duckdb
