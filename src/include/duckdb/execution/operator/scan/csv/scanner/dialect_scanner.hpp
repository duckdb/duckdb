//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/scanner/base_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/execution/operator/scan/csv/parser/scanner_boundary.hpp"

namespace duckdb {

class DialectResult : public ScannerResult {
public:
	idx_t sniffed_column_counts[STANDARD_VECTOR_SIZE];
	idx_t cur_rows;
};

//! Our dialect scanner basically goes over the CSV and figures out how many columns they have
class DialectScanner : public BaseScanner {
public:
	DialectScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine);

	DialectResult *ParseChunk() override;

private:
	void Initialize() override;

	void Process() override;

	inline bool ProcessInternal(char current_char);

	void FinalizeChunkProcess() override;

	DialectResult result;

	CSVStates states;

	idx_t column_count;
};

} // namespace duckdb
