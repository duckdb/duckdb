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

namespace duckdb {

class ColumnCountResult : public ScannerResult {
public:
	idx_t column_counts[STANDARD_VECTOR_SIZE];

	CSVStateMachine *state_machine;
	//! Adds a Value to the result
	static inline void AddValue(ColumnCountResult &result, const char current_char, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(ColumnCountResult &result, const char current_char, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline void Kaput(ColumnCountResult &result);

	idx_t &operator[](size_t index);
};

//! Scanner that goes over the CSV and figures out how many columns each row has. Used for dialect sniffing
class ColumnCountScanner : public BaseScanner {
public:
	ColumnCountScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine);

	ColumnCountResult *ParseChunk() override;

	ColumnCountResult *GetResult() override;

	StringValueScanner UpgradeToStringValueScanner();

private:
	void Process() override;

	void FinalizeChunkProcess() override;

	ColumnCountResult result;

	idx_t column_count;
};

} // namespace duckdb
