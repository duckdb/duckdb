//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/scanner/column_count_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/execution/operator/scan/csv/parser/scanner_boundary.hpp"
#include "duckdb/execution/operator/scan/csv/scanner/string_value_scanner.hpp"
#include "duckdb/execution/operator/scan/csv/scanner/base_scanner.hpp"

namespace duckdb {

class ColumnCountResult : public ScannerResult {
public:
	ColumnCountResult(CSVStates &states, CSVStateMachine &state_machine);
	idx_t &operator[](size_t index);

	idx_t column_counts[STANDARD_VECTOR_SIZE];
	idx_t current_column_count = 0;

	//! Adds a Value to the result
	static inline void AddValue(ColumnCountResult &result, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(ColumnCountResult &result, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline void Kaput(ColumnCountResult &result);
	inline void InternalAddRow();
};

//! Scanner that goes over the CSV and figures out how many columns each row has. Used for dialect sniffing
class ColumnCountScanner : public BaseScanner {
public:
	ColumnCountScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine);

	ColumnCountResult *ParseChunk() override;

	ColumnCountResult *GetResult() override;

	unique_ptr<StringValueScanner> UpgradeToStringValueScanner();

private:
	void Process() override;

	void FinalizeChunkProcess() override;

	ColumnCountResult result;

	idx_t column_count;
};

} // namespace duckdb
