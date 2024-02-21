//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/skip_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/string_value_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/base_scanner.hpp"

namespace duckdb {

class SkipResult : public ScannerResult {
public:
	SkipResult(CSVStates &states, CSVStateMachine &state_machine, idx_t rows_to_skip);

	idx_t row_count = 0;
	idx_t rows_to_skip;

	//! Adds a Value to the result
	static inline void AddValue(SkipResult &result, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(SkipResult &result, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline void InvalidState(SkipResult &result);
	//! Handles EmptyLine states
	static inline bool EmptyLine(SkipResult &result, const idx_t buffer_pos);
	//! Handles QuotedNewline State
	static inline void QuotedNewLine(SkipResult &result);
	inline void InternalAddRow();
};

//! Scanner used to skip lines in a CSV File
class SkipScanner : public BaseScanner {
public:
	SkipScanner(shared_ptr<CSVBufferManager> buffer_manager, const shared_ptr<CSVStateMachine> &state_machine,
	            shared_ptr<CSVErrorHandler> error_handler, idx_t rows_to_skip);

	~SkipScanner() {
	}

	SkipResult &ParseChunk() override;

	SkipResult &GetResult() override;

private:
	void Initialize() override;

	void FinalizeChunkProcess() override;

	SkipResult result;
};

} // namespace duckdb
