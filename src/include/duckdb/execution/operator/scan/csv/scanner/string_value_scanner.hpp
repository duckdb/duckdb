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

class TypeResult : public ScannerResult {
public:
	unique_ptr<Vector> vector;
	string_t *vector_ptr;
	idx_t vector_size;

	idx_t last_position;
	idx_t cur_value_idx;

	char *buffer_ptr;

	//! Adds a Value to the result
	static inline void AddValue(TypeResult &result, const char current_char, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(TypeResult &result, const char current_char, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline void Kaput(TypeResult &result);
};

//! Our dialect scanner basically goes over the CSV and figures out the tuples as Values for type sniffing.
class TypeScanner : public BaseScanner {
public:
	TypeScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine);

	TypeResult *ParseChunk() override;

private:
	void Process() override;

	void FinalizeChunkProcess() override;

	void ProcessOverbufferValue();

	TypeResult result;

	//! Pointer to the previous buffer handle, necessary for overbuffer values
	unique_ptr<CSVBufferHandle> previous_buffer_handle;
};

} // namespace duckdb
