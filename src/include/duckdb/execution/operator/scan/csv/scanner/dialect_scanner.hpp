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
	idx_t result[STANDARD_VECTOR_SIZE];
	idx_t current_rows;
};

//! Our dialect scanner basically goes over the CSV and figures out how many columns they have
class DialectScanner : public BaseScanner {
public:
	DialectScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	               ScannerBoundary boundary);

	DialectResult *ParseChunk() override;

private:
	virtual void Initialize() override;

	virtual void Process() override;

	virtual void FinalizeChunkProcess() override;

	DialectResult result;

	CSVStates states;

	idx_t column_count;
};

} // namespace duckdb
