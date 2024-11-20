//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/base_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_error.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

class CSVFileScan;
class ScannerResult {
public:
	ScannerResult(CSVStates &states, CSVStateMachine &state_machine, idx_t result_size);

	//! Adds a Value to the result
	static inline void SetQuoted(ScannerResult &result, idx_t quoted_position) {
		if (!result.quoted) {
			result.quoted_position = quoted_position;
		}
		result.quoted = true;
	}
	static inline void SetEscaped(ScannerResult &result) {
		result.escaped = true;
	}
	static inline void SetComment(ScannerResult &result, idx_t buffer_pos) {
		result.comment = true;
	}
	static inline bool UnsetComment(ScannerResult &result, idx_t buffer_pos) {
		result.comment = false;
		return false;
	}
	static inline bool IsCommentSet(const ScannerResult &result) {
		return result.comment == true;
	}

	//! Variable to keep information regarding quoted and escaped values
	bool quoted = false;
	bool escaped = false;
	//! Variable to keep track if we are in a comment row. Hence won't add it
	bool comment = false;
	idx_t quoted_position = 0;

	//! Size of the result
	const idx_t result_size;

	CSVStateMachine &state_machine;

	void Print() const {
		state_machine.Print();
	}

protected:
	CSVStates &states;
};

//! This is the base of our CSV scanners.
//! Scanners differ on what they are used for, and consequently have different performance benefits.
class BaseScanner {
public:
	explicit BaseScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	                     shared_ptr<CSVErrorHandler> error_handler, bool sniffing = false,
	                     shared_ptr<CSVFileScan> csv_file_scan = nullptr, CSVIterator iterator = {});

	virtual ~BaseScanner() = default;

	//! Returns true if the scanner is finished
	bool FinishedFile() const;

	//! Parses data into a output_chunk
	virtual ScannerResult &ParseChunk();

	//! Returns the result from the last Parse call. Shouts at you if you call it wrong
	virtual ScannerResult &GetResult();

	CSVIterator &GetIterator();

	void SetIterator(const CSVIterator &it);

	idx_t GetBoundaryIndex() const {
		return iterator.GetBoundaryIdx();
	}

	idx_t GetLinesRead() const {
		return lines_read;
	}

	CSVPosition GetIteratorPosition() const {
		return iterator.pos;
	}

	CSVStateMachine &GetStateMachine() const;

	shared_ptr<CSVFileScan> csv_file_scan;

	//! If this scanner is being used for sniffing
	bool sniffing = false;
	//! The guy that handles errors
	shared_ptr<CSVErrorHandler> error_handler;

	//! Shared pointer to the state machine, this is used across multiple scanners
	shared_ptr<CSVStateMachine> state_machine;

	//! States
	CSVStates states;

	bool ever_quoted = false;

	bool ever_escaped = false;

	//! Shared pointer to the buffer_manager, this is shared across multiple scanners
	shared_ptr<CSVBufferManager> buffer_manager;

	//! Skips Notes and/or parts of the data, starting from the top.
	//! notes are dirty lines on top of the file, before the actual data
	static CSVIterator SkipCSVRows(shared_ptr<CSVBufferManager> buffer_manager,
	                               const shared_ptr<CSVStateMachine> &state_machine, idx_t rows_to_skip);

	inline static bool ContainsZeroByte(uint64_t v) {
		return (v - UINT64_C(0x0101010101010101)) & ~(v)&UINT64_C(0x8080808080808080);
	}

protected:
	//! Boundaries of this scanner
	CSVIterator iterator;

	//! Unique pointer to the buffer_handle, this is unique per scanner, since it also contains the necessary counters
	//! To offload buffers to disk if necessary
	shared_ptr<CSVBufferHandle> cur_buffer_handle;

	//! Hold the current buffer ptr
	char *buffer_handle_ptr = nullptr;

	//! If this scanner has been initialized
	bool initialized = false;
	//! How many lines were read by this scanner
	idx_t lines_read = 0;
	idx_t bytes_read = 0;
	//! Internal Functions used to perform the parsing
	//! Initializes the scanner
	virtual void Initialize();

	//! Process one chunk
	template <class T>
	void Process(T &result) {
		idx_t to_pos;
		const idx_t start_pos = iterator.pos.buffer_pos;
		if (iterator.IsBoundarySet()) {
			to_pos = iterator.GetEndPos();
			if (to_pos > cur_buffer_handle->actual_size) {
				to_pos = cur_buffer_handle->actual_size;
			}
		} else {
			to_pos = cur_buffer_handle->actual_size;
		}
		while (iterator.pos.buffer_pos < to_pos) {
			state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
			switch (states.states[1]) {
			case CSVState::INVALID:
				T::InvalidState(result);
				iterator.pos.buffer_pos++;
				bytes_read = iterator.pos.buffer_pos - start_pos;
				return;
			case CSVState::RECORD_SEPARATOR:
				if (states.states[0] == CSVState::RECORD_SEPARATOR || states.states[0] == CSVState::NOT_SET) {
					if (T::EmptyLine(result, iterator.pos.buffer_pos)) {
						iterator.pos.buffer_pos++;
						bytes_read = iterator.pos.buffer_pos - start_pos;
						lines_read++;
						return;
					}
					lines_read++;

				} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
					if (T::IsCommentSet(result)) {
						if (T::UnsetComment(result, iterator.pos.buffer_pos)) {
							iterator.pos.buffer_pos++;
							bytes_read = iterator.pos.buffer_pos - start_pos;
							lines_read++;
							return;
						}
					} else {
						if (T::AddRow(result, iterator.pos.buffer_pos)) {
							iterator.pos.buffer_pos++;
							bytes_read = iterator.pos.buffer_pos - start_pos;
							lines_read++;
							return;
						}
					}
					lines_read++;
				}
				iterator.pos.buffer_pos++;
				break;
			case CSVState::CARRIAGE_RETURN:
				if (states.states[0] == CSVState::RECORD_SEPARATOR || states.states[0] == CSVState::NOT_SET) {
					if (T::EmptyLine(result, iterator.pos.buffer_pos)) {
						iterator.pos.buffer_pos++;
						bytes_read = iterator.pos.buffer_pos - start_pos;
						lines_read++;
						return;
					}
				} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
					if (T::IsCommentSet(result)) {
						if (T::UnsetComment(result, iterator.pos.buffer_pos)) {
							iterator.pos.buffer_pos++;
							bytes_read = iterator.pos.buffer_pos - start_pos;
							lines_read++;
							return;
						}
					} else {
						if (T::AddRow(result, iterator.pos.buffer_pos)) {
							iterator.pos.buffer_pos++;
							bytes_read = iterator.pos.buffer_pos - start_pos;
							lines_read++;
							return;
						}
					}
				}
				iterator.pos.buffer_pos++;
				lines_read++;
				break;
			case CSVState::DELIMITER:
				T::AddValue(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				break;
			case CSVState::QUOTED: {
				if (states.states[0] == CSVState::UNQUOTED) {
					T::SetEscaped(result);
				}
				ever_quoted = true;
				T::SetQuoted(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				while (iterator.pos.buffer_pos + 8 < to_pos) {
					uint64_t value =
					    Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(&buffer_handle_ptr[iterator.pos.buffer_pos]));
					if (ContainsZeroByte((value ^ state_machine->transition_array.quote) &
					                     (value ^ state_machine->transition_array.escape))) {
						break;
					}
					iterator.pos.buffer_pos += 8;
				}

				while (state_machine->transition_array
				           .skip_quoted[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
				       iterator.pos.buffer_pos < to_pos - 1) {
					iterator.pos.buffer_pos++;
				}
			} break;
			case CSVState::ESCAPE:
			case CSVState::UNQUOTED_ESCAPE:
			case CSVState::ESCAPED_RETURN:
				T::SetEscaped(result);
				ever_escaped = true;
				iterator.pos.buffer_pos++;
				break;
			case CSVState::STANDARD: {
				iterator.pos.buffer_pos++;
				while (iterator.pos.buffer_pos + 8 < to_pos) {
					uint64_t value =
					    Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(&buffer_handle_ptr[iterator.pos.buffer_pos]));
					if (ContainsZeroByte((value ^ state_machine->transition_array.delimiter) &
					                     (value ^ state_machine->transition_array.new_line) &
					                     (value ^ state_machine->transition_array.carriage_return) &
					                     (value ^ state_machine->transition_array.escape) &
					                     (value ^ state_machine->transition_array.comment))) {
						break;
					}
					iterator.pos.buffer_pos += 8;
				}
				while (state_machine->transition_array
				           .skip_standard[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
				       iterator.pos.buffer_pos < to_pos - 1) {
					iterator.pos.buffer_pos++;
				}
				break;
			}
			case CSVState::QUOTED_NEW_LINE:
				T::QuotedNewLine(result);
				iterator.pos.buffer_pos++;
				break;
			case CSVState::COMMENT: {
				T::SetComment(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				while (iterator.pos.buffer_pos + 8 < to_pos) {
					uint64_t value =
					    Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(&buffer_handle_ptr[iterator.pos.buffer_pos]));
					if (ContainsZeroByte((value ^ state_machine->transition_array.new_line) &
					                     (value ^ state_machine->transition_array.carriage_return))) {
						break;
					}
					iterator.pos.buffer_pos += 8;
				}
				while (state_machine->transition_array
				           .skip_comment[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
				       iterator.pos.buffer_pos < to_pos - 1) {
					iterator.pos.buffer_pos++;
				}
				break;
			}
			default:
				iterator.pos.buffer_pos++;
				break;
			}
		}
		bytes_read = iterator.pos.buffer_pos - start_pos;
	}

	//! Finalizes the process of the chunk
	virtual void FinalizeChunkProcess();

	//! Internal function for parse chunk
	template <class T>
	void ParseChunkInternal(T &result) {
		if (iterator.done) {
			return;
		}
		if (!initialized) {
			Initialize();
			initialized = true;
		}
		if (!iterator.done && cur_buffer_handle) {
			Process(result);
		}
		FinalizeChunkProcess();
	}
};

} // namespace duckdb
