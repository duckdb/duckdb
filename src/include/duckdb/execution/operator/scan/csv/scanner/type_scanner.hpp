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

struct TupleOfValues {
	idx_t line_number;
	idx_t position;
	bool set = false;
	vector<Value> values;
	void Print() {
		for (auto &val : values) {
			val.Print();
		}
	}
};

class TypeResult : public ScannerResult {
public:
	TupleOfValues values[STANDARD_VECTOR_SIZE];
	idx_t cur_rows;
};

//! Our dialect scanner basically goes over the CSV and figures out the tuples as Values for type sniffing.
class TypeScanner : public BaseScanner {
public:
	TypeScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine);

	TypeResult *ParseChunk() override;

private:
	void Initialize() override;

	void Process() override;

	inline bool ProcessInternal(char current_char);

	void FinalizeChunkProcess() override;

	TypeResult result;
};

} // namespace duckdb
