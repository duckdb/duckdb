//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"

namespace duckdb {

//! The CSV Scanner is what iterates over CSV Buffers
class CSVScanner {
public:
	explicit CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p);

	//! This functions templates an operation over the CSV File
	template <class OP, class T>
	inline bool Process(CSVScanner &machine, T &result) {
		OP::Initialize(machine);
		//! If current buffer is not set we try to get a new one
		if (!cur_buffer_handle) {
			cur_pos = 0;
			if (cur_buffer_idx == 0) {
				cur_pos = buffer_manager->GetStartPos();
			}
			cur_buffer_handle = buffer_manager->GetBuffer(cur_buffer_idx++);
			D_ASSERT(cur_buffer_handle);
		}
		while (cur_buffer_handle) {
			char *buffer_handle_ptr = cur_buffer_handle->Ptr();
			while (cur_pos < cur_buffer_handle->actual_size) {
				if (OP::Process(machine, result, buffer_handle_ptr[cur_pos], cur_pos)) {
					//! Not-Done Processing the File, but the Operator is happy!
					OP::Finalize(machine, result);
					return false;
				}
				cur_pos++;
			}
			cur_buffer_handle = buffer_manager->GetBuffer(cur_buffer_idx++);
			cur_pos = 0;
		}
		//! Done Processing the File
		OP::Finalize(machine, result);
		return true;
	}
	//! Returns true if the iterator is finished
	bool Finished();
	//! Resets the iterator
	void Reset();

	CSVStateMachineSniffing &GetStateMachine();

	//! Current Numbers of Rows
	idx_t cur_rows = 0;
	//! Current Number of Columns
	idx_t column_count = 1;

	//! Current, previous, and state before the previous
	CSVState state;
	CSVState previous_state;
	CSVState pre_previous_state;

	//! String Value
	string value;
	idx_t rows_read = 0;
	idx_t line_start_pos = 0;

	//! Verifies if value is UTF8
	void VerifyUTF8();

private:
	idx_t cur_pos = 0;
	idx_t cur_buffer_idx = 0;
	shared_ptr<CSVBufferManager> buffer_manager;
	unique_ptr<CSVBufferHandle> cur_buffer_handle;
	unique_ptr<CSVStateMachine> state_machine;
};

//! The CSV Scanner with special objects to perform sniffing
// class CSVScannerSniffer: public CSVScanner {
// public:
//	explicit CSVScannerSniffer(shared_ptr<CSVBufferManager> buffer_manager_p)
//	    : CSVScanner(std::move(buffer_manager_p)) {
//	};
//
// private:
//	 int x = 0;
//};

} // namespace duckdb
