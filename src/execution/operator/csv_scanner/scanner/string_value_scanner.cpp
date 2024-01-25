#include "duckdb/execution/operator/csv_scanner/scanner/string_value_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/util/csv_casting.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/table_function/csv_file_scanner.hpp"
#include "duckdb/main/client_data.hpp"

#include <algorithm>

namespace duckdb {

StringValueResult::StringValueResult(CSVStates &states, CSVStateMachine &state_machine, CSVBufferHandle &buffer_handle,
                                     Allocator &buffer_allocator, idx_t result_size_p, idx_t buffer_position,
                                     CSVErrorHandler &error_hander_p, CSVIterator &iterator_p, bool store_line_size_p)
    : ScannerResult(states, state_machine), number_of_columns(state_machine.dialect_options.num_cols),
      null_padding(state_machine.options.null_padding), ignore_errors(state_machine.options.ignore_errors),
      null_str(state_machine.options.null_str), result_size(result_size_p), error_handler(error_hander_p),
      iterator(iterator_p), store_line_size(store_line_size_p) {
	// Vector information
	D_ASSERT(number_of_columns > 0);
	vector_size = number_of_columns * result_size;
	vector = make_uniq<Vector>(LogicalType::VARCHAR, vector_size);
	vector_ptr = FlatVector::GetData<string_t>(*vector);
	validity_mask = &FlatVector::Validity(*vector);

	// Buffer Information
	buffer_ptr = buffer_handle.Ptr();
	buffer_size = buffer_handle.actual_size;
	last_position = buffer_position;

	// Current Result information
	result_position = 0;
	previous_line_start = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, buffer_handle.actual_size};
	pre_previous_line_start = previous_line_start;
	// Initialize Parse Chunk
	parse_chunk.Initialize(buffer_allocator, {number_of_columns, LogicalType::VARCHAR}, result_size);
}

Value StringValueResult::GetValue(idx_t row_idx, idx_t col_idx) {
	idx_t vector_idx = row_idx * number_of_columns + col_idx;
	if (validity_mask->AllValid()) {
		return Value(vector_ptr[vector_idx]);
	} else {
		if (validity_mask->RowIsValid(vector_idx)) {
			return Value(vector_ptr[vector_idx]);
		} else {
			return Value();
		}
	}
}
DataChunk &StringValueResult::ToChunk() {
	idx_t number_of_rows = NumberOfRows();
	parse_chunk.Reset();
	const auto &selection_vectors = state_machine.GetSelectionVector();
	for (idx_t col_idx = 0; col_idx < parse_chunk.ColumnCount(); col_idx++) {
		parse_chunk.data[col_idx].Slice(*vector, selection_vectors[col_idx], number_of_rows);
	}
	parse_chunk.SetCardinality(number_of_rows);
	return parse_chunk;
}

void StringValueResult::AddQuotedValue(StringValueResult &result, const idx_t buffer_pos) {
	if (result.escaped) {
		// If it's an escaped value we have to remove all the escapes, this is not really great
		auto value = StringValueScanner::RemoveEscape(result.buffer_ptr + result.last_position + 1,
		                                              buffer_pos - result.last_position - 2,
		                                              result.state_machine.options.GetEscape()[0], *result.vector);

		result.AddValueToVector(value);
	} else {
		if (buffer_pos < result.last_position + 2) {
			// empty value
			auto value = string_t();
			result.AddValueToVector(value);
		} else {
			auto value = string_t(result.buffer_ptr + result.last_position + 1, buffer_pos - result.last_position - 2);
			result.AddValueToVector(value);
		}
	}
	result.quoted = false;
	result.escaped = false;
}

void StringValueResult::AddValue(StringValueResult &result, const idx_t buffer_pos) {
	if (result.last_position > buffer_pos) {
		return;
	}

	D_ASSERT(result.result_position < result.vector_size);
	if (result.result_position == result.vector_size) {
		result.HandleOverLimitRows();
	}
	if (result.quoted) {
		StringValueResult::AddQuotedValue(result, buffer_pos);
	} else {
		auto value = string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
		result.AddValueToVector(value);
	}
	result.last_position = buffer_pos + 1;
}

void StringValueResult::HandleOverLimitRows() {
	auto csv_error = CSVError::IncorrectColumnAmountError(state_machine.options, vector_ptr, number_of_columns,
	                                                      result_position - last_row_pos);
	LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), result_position / number_of_columns + 1);
	error_handler.Error(lines_per_batch, csv_error);
	result_position -= result_position % number_of_columns + number_of_columns;
}

void StringValueResult::AddValueToVector(string_t &value, bool allocate) {
	if (((quoted && state_machine.options.allow_quoted_nulls) || !quoted) && value == null_str) {
		bool empty = false;
		idx_t cur_pos = result_position % number_of_columns;
		if (cur_pos < state_machine.options.force_not_null.size()) {
			empty = state_machine.options.force_not_null[cur_pos];
		}
		if (empty) {
			vector_ptr[result_position++] = string_t();
		} else {
			validity_mask->SetInvalid(result_position++);
		}
	} else {
		if (allocate) {
			vector_ptr[result_position++] = StringVector::AddStringOrBlob(*vector, value);
		} else {
			vector_ptr[result_position++] = value;
		}
	}
}
void StringValueResult::QuotedNewLine(StringValueResult &result) {
	result.quoted_new_line = true;
}

void StringValueResult::NullPaddingQuotedNewlineCheck() {
	// We do some checks for null_padding correctness
	if (state_machine.options.null_padding && iterator.IsBoundarySet() && quoted_new_line && iterator.done) {
		// If we have null_padding set, we found a quoted new line, we are scanning the file in parallel and it's the
		// last row of this thread.
		auto csv_error = CSVError::NullPaddingFail(state_machine.options);
		LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), result_position / number_of_columns + 1);
		error_handler.Error(lines_per_batch, csv_error, true);
	}
}

bool StringValueResult::AddRowInternal() {
	NullPaddingQuotedNewlineCheck();
	quoted_new_line = false;
	// We need to check if we are getting the correct number of columns here.
	// If columns are correct, we add it, and that's it.
	idx_t total_columns_in_row = result_position - last_row_pos;
	if (total_columns_in_row > number_of_columns) {
		// If the columns are incorrect:
		// Maybe we have too many columns:
		if (total_columns_in_row == number_of_columns + 1 && !validity_mask->RowIsValid(result_position - 1)) {
			// This is a weird case, where we ignore an extra value, if it is a null value
			result_position--;
			validity_mask->SetValid(result_position);
		} else {
			HandleOverLimitRows();
		}
	}
	if (result_position % number_of_columns != 0) {
		// Maybe we have too few columns:
		// 1) if null_padding is on we null pad it
		if (null_padding) {
			while (result_position % number_of_columns != 0) {
				bool empty = false;
				idx_t cur_pos = result_position % number_of_columns;
				if (cur_pos < state_machine.options.force_not_null.size()) {
					empty = state_machine.options.force_not_null[cur_pos];
				}
				if (empty) {
					vector_ptr[result_position++] = string_t();
				} else {
					validity_mask->SetInvalid(result_position++);
				}
			}
		} else {
			auto csv_error = CSVError::IncorrectColumnAmountError(state_machine.options, vector_ptr, number_of_columns,
			                                                      result_position % number_of_columns);
			LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), result_position / number_of_columns + 1);
			error_handler.Error(lines_per_batch, csv_error);
			result_position -= result_position % number_of_columns;
			D_ASSERT(result_position % number_of_columns == 0);
		}
	}
	last_row_pos = result_position;
	if (result_position / number_of_columns >= result_size) {
		// We have a full chunk
		return true;
	}
	return false;
}

void StringValueResult::Print() {
	for (idx_t i = 0; i < result_position; i++) {
		std::cout << vector_ptr[i].GetString();
		if ((i + 1) % number_of_columns == 0) {
			std::cout << std::endl;
		} else {
			std::cout << ",";
		}
	}
}

bool StringValueResult::AddRow(StringValueResult &result, const idx_t buffer_pos) {
	if (result.last_position <= buffer_pos) {
		LinePosition current_line_start = {result.iterator.pos.buffer_idx, result.iterator.pos.buffer_pos,
		                                   result.buffer_size};
		idx_t current_line_size = current_line_start - result.previous_line_start;
		if (result.store_line_size) {
			result.error_handler.NewMaxLineSize(current_line_size);
		}
		if (current_line_size > result.state_machine.options.maximum_line_size) {
			auto csv_error = CSVError::LineSizeError(result.state_machine.options, current_line_size);
			LinesPerBoundary lines_per_batch(result.iterator.GetBoundaryIdx(),
			                                 result.result_position / result.number_of_columns);
			result.error_handler.Error(lines_per_batch, csv_error);
		}
		result.pre_previous_line_start = result.previous_line_start;
		result.previous_line_start = current_line_start;
		if (result.result_position == result.vector_size) {
			result.HandleOverLimitRows();
		}
		// We add the value
		if (result.quoted) {
			StringValueResult::AddQuotedValue(result, buffer_pos);
		} else {
			auto value = string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
			result.AddValueToVector(value);
		}
		if (result.state_machine.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
			if (result.states.states[1] == CSVState::RECORD_SEPARATOR) {
				// Even though this is marked as a carry on, this is a hippie mixie
				result.last_position = buffer_pos + 1;
			} else {
				result.last_position = buffer_pos + 2;
			}
		} else {
			result.last_position = buffer_pos + 1;
		}
	}

	// We add the value
	return result.AddRowInternal();
}

void StringValueResult::InvalidState(StringValueResult &result) {
	// FIXME: How do we recover from an invalid state? Can we restart the state machine and jump to the next row?
	auto csv_error =
	    CSVError::UnterminatedQuotesError(result.state_machine.options, result.vector_ptr, result.result_position,
	                                      result.result_position % result.number_of_columns);
	LinesPerBoundary lines_per_batch(result.iterator.GetBoundaryIdx(),
	                                 result.result_position / result.number_of_columns);
	result.error_handler.Error(lines_per_batch, csv_error);
}

bool StringValueResult::EmptyLine(StringValueResult &result, const idx_t buffer_pos) {
	// We care about empty lines if this is a single column csv file
	result.last_position = buffer_pos + 1;
	if (result.states.IsCarriageReturn() &&
	    result.state_machine.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		result.last_position++;
	}
	if (result.parse_chunk.ColumnCount() == 1) {
		if (result.null_str.Empty()) {
			bool empty = false;
			if (!result.state_machine.options.force_not_null.empty()) {
				empty = result.state_machine.options.force_not_null[result.result_position % result.number_of_columns];
			}
			if (empty) {
				result.vector_ptr[result.result_position++] = string_t();
			} else {
				result.validity_mask->SetInvalid(result.result_position++);
			}
		}
		if (result.result_position / result.number_of_columns >= result.result_size) {
			// We have a full chunk
			return true;
		}
		result.last_row_pos = result.result_position;
	}
	return false;
}

idx_t StringValueResult::NumberOfRows() {
	if (result_position % number_of_columns != 0) {
		// Maybe we have too few columns:
		// 1) if null_padding is on we null pad it
		if (null_padding) {
			while (result_position % number_of_columns != 0) {
				bool empty = false;
				idx_t cur_pos = result_position % number_of_columns;
				if (cur_pos < state_machine.options.force_not_null.size()) {
					empty = state_machine.options.force_not_null[cur_pos];
				}
				if (empty) {
					vector_ptr[result_position++] = string_t();
				} else {
					validity_mask->SetInvalid(result_position++);
				}
			}
		} else {
			auto csv_error = CSVError::IncorrectColumnAmountError(state_machine.options, vector_ptr, number_of_columns,
			                                                      result_position % number_of_columns);
			LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), result_position / number_of_columns + 1);
			error_handler.Error(lines_per_batch, csv_error);
			result_position -= result_position % number_of_columns;
			D_ASSERT(result_position % number_of_columns == 0);
		}
	}
	return result_position / number_of_columns;
}

StringValueScanner::StringValueScanner(idx_t scanner_idx_p, const shared_ptr<CSVBufferManager> &buffer_manager,
                                       const shared_ptr<CSVStateMachine> &state_machine,
                                       const shared_ptr<CSVErrorHandler> &error_handler, CSVIterator boundary,
                                       idx_t result_size)
    : BaseScanner(buffer_manager, state_machine, error_handler, boundary), scanner_idx(scanner_idx_p),
      result(states, *state_machine, *cur_buffer_handle, BufferAllocator::Get(buffer_manager->context), result_size,
             iterator.pos.buffer_pos, *error_handler, iterator,
             buffer_manager->context.client_data->debug_set_max_line_length) {
}

unique_ptr<StringValueScanner> StringValueScanner::GetCSVScanner(ClientContext &context, CSVReaderOptions &options) {
	auto state_machine = make_shared<CSVStateMachine>(options, options.dialect_options.state_machine_options,
	                                                  CSVStateMachineCache::Get(context));

	state_machine->dialect_options.num_cols = options.dialect_options.num_cols;
	state_machine->dialect_options.header = options.dialect_options.header;
	auto buffer_manager = make_shared<CSVBufferManager>(context, options, options.file_path, 0);
	auto scanner = make_uniq<StringValueScanner>(0, buffer_manager, state_machine, make_shared<CSVErrorHandler>());
	scanner->csv_file_scan = make_shared<CSVFileScan>(context, options.file_path, options);
	scanner->csv_file_scan->InitializeProjection();
	return scanner;
}

bool StringValueScanner::FinishedIterator() {
	return iterator.done;
}

StringValueResult &StringValueScanner::ParseChunk() {
	result.result_position = 0;
	result.last_row_pos = 0;
	result.validity_mask->SetAllValid(result.vector_size);
	ParseChunkInternal(result);
	return result;
}

void StringValueScanner::Flush(DataChunk &insert_chunk) {
	auto &process_result = ParseChunk();
	// First Get Parsed Chunk
	auto &parse_chunk = process_result.ToChunk();

	if (parse_chunk.size() == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);

	// We keep track of the borked lines, in case we are ignoring errors
	unordered_set<idx_t> borked_lines;
	D_ASSERT(csv_file_scan);

	auto &reader_data = csv_file_scan->reader_data;
	// Now Do the cast-aroo
	for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
		auto col_idx = reader_data.column_ids[c];
		auto result_idx = reader_data.column_mapping[c];
		if (col_idx >= parse_chunk.ColumnCount()) {
			throw InvalidInputException("Mismatch between the schema of different files");
		}
		auto &parse_vector = parse_chunk.data[col_idx];
		auto &result_vector = insert_chunk.data[result_idx];
		auto &type = result_vector.GetType();
		if (type.id() == LogicalTypeId::VARCHAR) {
			// target type is varchar: no need to convert
			// reinterpret rather than reference, so we can deal with user-defined types
			result_vector.Reinterpret(parse_vector);
		} else {
			string error_message;
			bool success;
			idx_t line_error = 0;
			bool line_error_set = true;

			if (!state_machine->options.dialect_options.date_format.at(LogicalTypeId::DATE).GetValue().Empty() &&
			    type.id() == LogicalTypeId::DATE) {
				// use the date format to cast the chunk
				success = CSVCast::TryCastDateVector(state_machine->options.dialect_options.date_format, parse_vector,
				                                     result_vector, parse_chunk.size(), error_message, line_error);
			} else if (!state_machine->options.dialect_options.date_format.at(LogicalTypeId::TIMESTAMP)
			                .GetValue()
			                .Empty() &&
			           type.id() == LogicalTypeId::TIMESTAMP) {
				// use the date format to cast the chunk
				success =
				    CSVCast::TryCastTimestampVector(state_machine->options.dialect_options.date_format, parse_vector,
				                                    result_vector, parse_chunk.size(), error_message);
			} else if (state_machine->options.decimal_separator != "." &&
			           (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE)) {
				success =
				    CSVCast::TryCastFloatingVectorCommaSeparated(state_machine->options, parse_vector, result_vector,
				                                                 parse_chunk.size(), error_message, type, line_error);
			} else if (state_machine->options.decimal_separator != "." && type.id() == LogicalTypeId::DECIMAL) {
				success = CSVCast::TryCastDecimalVectorCommaSeparated(
				    state_machine->options, parse_vector, result_vector, parse_chunk.size(), error_message, type);
			} else {
				// target type is not varchar: perform a cast
				success = VectorOperations::TryCast(buffer_manager->context, parse_vector, result_vector,
				                                    parse_chunk.size(), &error_message);
				line_error_set = false;
			}
			if (success) {
				continue;
			}
			// An error happened, to propagate it we need to figure out the exact line where the casting failed.
			UnifiedVectorFormat inserted_column_data;
			result_vector.ToUnifiedFormat(parse_chunk.size(), inserted_column_data);
			UnifiedVectorFormat parse_column_data;
			parse_vector.ToUnifiedFormat(parse_chunk.size(), parse_column_data);
			if (!line_error_set) {
				for (; line_error < parse_chunk.size(); line_error++) {
					if (!inserted_column_data.validity.RowIsValid(line_error) &&
					    parse_column_data.validity.RowIsValid(line_error)) {
						break;
					}
				}
			}
			{
				vector<Value> row;
				for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
					row.push_back(parse_chunk.GetValue(col, line_error));
				}
				auto csv_error = CSVError::CastError(state_machine->options, parse_chunk, line_error,
				                                     csv_file_scan->names[col_idx], error_message, col_idx, row);
				LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(),
				                                 lines_read - parse_chunk.size() + line_error);
				error_handler->Error(lines_per_batch, csv_error);
			}
			borked_lines.insert(line_error++);
			D_ASSERT(state_machine->options.ignore_errors);
			// We are ignoring errors. We must continue but ignoring borked rows
			for (; line_error < parse_chunk.size(); line_error++) {
				if (!inserted_column_data.validity.RowIsValid(line_error) &&
				    parse_column_data.validity.RowIsValid(line_error)) {
					borked_lines.insert(line_error);
					vector<Value> row;
					for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
						row.push_back(parse_chunk.GetValue(col, line_error));
					}
					auto csv_error = CSVError::CastError(state_machine->options, parse_chunk, line_error,
					                                     csv_file_scan->names[col_idx], error_message, col_idx, row);
					LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(),
					                                 lines_read - parse_chunk.size() + line_error);
					error_handler->Error(lines_per_batch, csv_error);
				}
			}
		}
	}
	if (!borked_lines.empty()) {
		// We must remove the borked lines from our chunk
		SelectionVector succesful_rows(parse_chunk.size() - borked_lines.size());
		idx_t sel_idx = 0;
		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {
			if (borked_lines.find(row_idx) == borked_lines.end()) {
				succesful_rows.set_index(sel_idx++, row_idx);
			}
		}
		// Now we slice the result
		insert_chunk.Slice(succesful_rows, sel_idx);
	}
}

void StringValueScanner::Initialize() {
	states.Initialize();

	if (result.result_size != 1 && !(sniffing && state_machine->options.null_padding &&
	                                 !state_machine->options.dialect_options.skip_rows.IsSetByUser())) {
		SetStart();
	}
	result.last_position = iterator.pos.buffer_pos;
	result.previous_line_start = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, cur_buffer_handle->actual_size};

	result.pre_previous_line_start = result.previous_line_start;
}

void StringValueScanner::ProcessExtraRow() {
	result.NullPaddingQuotedNewlineCheck();
	idx_t to_pos = cur_buffer_handle->actual_size;
	while (iterator.pos.buffer_pos < to_pos) {
		state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
		switch (states.states[1]) {
		case CSVState::INVALID:
			result.InvalidState(result);
			iterator.pos.buffer_pos++;
			break;
		case CSVState::RECORD_SEPARATOR:
			if (states.states[0] == CSVState::RECORD_SEPARATOR) {
				lines_read++;
				result.EmptyLine(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				return;
			} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
				lines_read++;
				result.AddRow(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				return;
			}
			iterator.pos.buffer_pos++;
			break;
		case CSVState::CARRIAGE_RETURN:
			lines_read++;
			if (states.states[0] != CSVState::RECORD_SEPARATOR) {
				result.AddRow(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				return;
			} else {
				result.EmptyLine(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				return;
			}
		case CSVState::DELIMITER:
			result.AddValue(result, iterator.pos.buffer_pos);
			iterator.pos.buffer_pos++;
			break;
		case CSVState::QUOTED:
			if (states.states[0] == CSVState::UNQUOTED) {
				result.SetEscaped(result);
			}
			result.SetQuoted(result);
			iterator.pos.buffer_pos++;
			while (state_machine->transition_array
			           .skip_quoted[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
			       iterator.pos.buffer_pos < to_pos - 1) {
				iterator.pos.buffer_pos++;
			}
			break;
		case CSVState::ESCAPE:
			result.SetEscaped(result);
			iterator.pos.buffer_pos++;
			break;
		case CSVState::STANDARD:
			iterator.pos.buffer_pos++;
			while (state_machine->transition_array
			           .skip_standard[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
			       iterator.pos.buffer_pos < to_pos - 1) {
				iterator.pos.buffer_pos++;
			}
			break;
		case CSVState::QUOTED_NEW_LINE:
			result.quoted_new_line = true;
			result.NullPaddingQuotedNewlineCheck();
			iterator.pos.buffer_pos++;
			break;
		default:
			iterator.pos.buffer_pos++;
			break;
		}
	}
}

string_t StringValueScanner::RemoveEscape(const char *str_ptr, idx_t end, char escape, Vector &vector) {
	// Figure out the exact size
	idx_t str_pos = 0;
	bool just_escaped = false;
	for (idx_t cur_pos = 0; cur_pos < end; cur_pos++) {
		if (str_ptr[cur_pos] == escape && !just_escaped) {
			just_escaped = true;
		} else {
			just_escaped = false;
			str_pos++;
		}
	}

	auto removed_escapes = StringVector::EmptyString(vector, str_pos);
	auto removed_escapes_ptr = removed_escapes.GetDataWriteable();
	// Allocate string and copy it
	str_pos = 0;
	just_escaped = false;
	for (idx_t cur_pos = 0; cur_pos < end; cur_pos++) {
		char c = str_ptr[cur_pos];
		if (c == escape && !just_escaped) {
			just_escaped = true;
		} else {
			just_escaped = false;
			removed_escapes_ptr[str_pos++] = c;
		}
	}
	removed_escapes.Finalize();
	return removed_escapes;
}

void StringValueScanner::ProcessOverbufferValue() {
	// Process first string
	states.Initialize();
	string overbuffer_string;
	auto previous_buffer = previous_buffer_handle->Ptr();
	if (result.last_position == previous_buffer_handle->actual_size) {
		state_machine->Transition(states, previous_buffer[result.last_position - 1]);
	}
	for (idx_t i = result.last_position; i < previous_buffer_handle->actual_size; i++) {
		state_machine->Transition(states, previous_buffer[i]);
		if (states.EmptyLine() || states.IsCurrentNewRow()) {
			continue;
		}
		if (states.NewRow() || states.NewValue()) {
			break;
		} else {
			overbuffer_string += previous_buffer[i];
		}
		if (states.IsQuoted()) {
			result.quoted = true;
		}
		if (states.IsEscaped()) {
			result.escaped = true;
		}
	}
	if (overbuffer_string.empty() &&
	    state_machine->dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n') {
			iterator.pos.buffer_pos++;
		}
	}
	// second buffer
	for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
		state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
		if (states.EmptyLine()) {
			if (state_machine->dialect_options.num_cols == 1) {
				break;
			} else {
				continue;
			}
		}
		if (states.NewRow() || states.NewValue()) {
			break;
		} else {
			overbuffer_string += buffer_handle_ptr[iterator.pos.buffer_pos];
		}
		if (states.IsQuoted()) {
			result.quoted = true;
		}
		if (states.IsEscaped()) {
			result.escaped = true;
		}
	}
	string_t value;
	if (result.quoted) {
		value = string_t(overbuffer_string.c_str() + result.quoted, overbuffer_string.size() - 2);
		if (result.escaped) {
			const auto str_ptr = static_cast<const char *>(overbuffer_string.c_str() + result.quoted);
			value = StringValueScanner::RemoveEscape(
			    str_ptr, overbuffer_string.size() - 2,
			    state_machine->dialect_options.state_machine_options.escape.GetValue(), *result.vector);
		}
	} else {
		value = string_t(overbuffer_string.c_str(), overbuffer_string.size());
	}
	if (!states.IsNotSet()) {
		result.AddValueToVector(value, true);
	}
	if (states.NewRow() && !states.IsNotSet()) {
		result.AddRowInternal();
		lines_read++;
	}
	if (states.EmptyLine() && state_machine->dialect_options.num_cols == 1) {
		result.last_row_pos = result.result_position;
	}
	if (iterator.pos.buffer_pos >= cur_buffer_handle->actual_size && cur_buffer_handle->is_last_buffer) {
		result.added_last_line = true;
	}
	if (states.IsCarriageReturn() &&
	    state_machine->dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		result.last_position = ++iterator.pos.buffer_pos + 1;
	} else {
		result.last_position = ++iterator.pos.buffer_pos;
	}
	// Be sure to reset the quoted and escaped variables
	result.quoted = false;
	result.escaped = false;
}

bool StringValueScanner::MoveToNextBuffer() {
	if (iterator.pos.buffer_pos >= cur_buffer_handle->actual_size) {
		previous_buffer_handle = std::move(cur_buffer_handle);
		cur_buffer_handle = buffer_manager->GetBuffer(++iterator.pos.buffer_idx);
		if (!cur_buffer_handle) {
			iterator.pos.buffer_idx--;
			buffer_handle_ptr = nullptr;
			// We do not care if it's a quoted new line on the last row of our file.
			result.quoted_new_line = false;
			// This means we reached the end of the file, we must add a last line if there is any to be added
			if (states.EmptyLine() || states.NewRow() || result.added_last_line || states.IsCurrentNewRow() ||
			    states.IsNotSet()) {
				while (result.result_position % result.number_of_columns != 0) {
					result.result_position--;
				}
				return false;
			} else if (states.NewValue()) {
				lines_read++;
				// we add the value
				result.AddValue(result, previous_buffer_handle->actual_size);
				// And an extra empty value to represent what comes after the delimiter
				result.AddRow(result, previous_buffer_handle->actual_size);
			} else if (states.IsQuotedCurrent()) {
				// Unterminated quote
				result.InvalidState(result);
			} else {
				lines_read++;
				result.AddRow(result, previous_buffer_handle->actual_size);
			}
			return false;
		}
		iterator.pos.buffer_pos = 0;
		buffer_handle_ptr = cur_buffer_handle->Ptr();
		// Handle overbuffer value
		ProcessOverbufferValue();
		result.buffer_ptr = buffer_handle_ptr;
		result.buffer_size = cur_buffer_handle->actual_size;
		return true;
	}
	return false;
}

void StringValueScanner::SkipBOM() {
	if (cur_buffer_handle->actual_size >= 3 && result.buffer_ptr[0] == '\xEF' && result.buffer_ptr[1] == '\xBB' &&
	    result.buffer_ptr[2] == '\xBF') {
		iterator.pos.buffer_pos = 3;
	}
}

void StringValueScanner::SkipCSVRows() {
	idx_t rows_to_skip =
	    state_machine->dialect_options.skip_rows.GetValue() + state_machine->dialect_options.header.GetValue();
	if (rows_to_skip == 0) {
		return;
	}
	SkipScanner row_skipper(buffer_manager, state_machine, error_handler, rows_to_skip);
	row_skipper.ParseChunk();
	iterator.pos.buffer_pos = row_skipper.GetIteratorPosition();
	if (row_skipper.state_machine->options.dialect_options.state_machine_options.new_line ==
	        NewLineIdentifier::CARRY_ON &&
	    row_skipper.states.states[1] == CSVState::CARRIAGE_RETURN) {
		iterator.pos.buffer_pos++;
	}
	if (result.store_line_size) {
		result.error_handler.NewMaxLineSize(iterator.pos.buffer_pos);
	}
	lines_read += row_skipper.GetLinesRead();
}

void StringValueScanner::SkipUntilNewLine() {
	// Now skip until next newline
	if (state_machine->options.dialect_options.state_machine_options.new_line.GetValue() ==
	    NewLineIdentifier::CARRY_ON) {
		bool carriage_return = false;
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\r') {
				carriage_return = true;
			}
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n') {
				if (carriage_return) {
					iterator.pos.buffer_pos++;
					return;
				}
			}
		}
	} else {
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n' ||
			    buffer_handle_ptr[iterator.pos.buffer_pos] == '\r') {
				iterator.pos.buffer_pos++;
				return;
			}
		}
	}
}

void StringValueScanner::SetStart() {
	if (iterator.pos.buffer_idx == 0 && iterator.pos.buffer_pos == 0) {
		// This means this is the very first buffer
		// This CSV is not from auto-detect, so we don't know where exactly it starts
		// Hence we potentially have to skip empty lines and headers.
		SkipBOM();
		SkipCSVRows();
		return;
	}
	// We have to look for a new line that fits our schema
	// 1. We walk until the next new line
	SkipUntilNewLine();
	if (state_machine->options.null_padding) {
		// When Null Padding, we assume we start from the correct new-line
		return;
	}
	StringValueScanner scan_finder(0, buffer_manager, state_machine, make_shared<CSVErrorHandler>(true), iterator, 1);
	auto &tuples = scan_finder.ParseChunk();
	if (tuples.Empty() || tuples.Size() != state_machine->options.dialect_options.num_cols) {
		// If no tuples were parsed, this is not the correct start, we need to skip until the next new line
		// Or if columns don't match, this is not the correct start, we need to skip until the next new line
		if (scan_finder.iterator.pos.buffer_pos >= cur_buffer_handle->actual_size &&
		    cur_buffer_handle->is_last_buffer) {
			iterator.pos.buffer_idx = scan_finder.iterator.pos.buffer_idx;
			iterator.pos.buffer_pos = scan_finder.iterator.pos.buffer_pos;
			result.last_position = iterator.pos.buffer_pos;
			return;
		}
	}
	iterator.pos.buffer_idx = scan_finder.result.pre_previous_line_start.buffer_idx;
	iterator.pos.buffer_pos = scan_finder.result.pre_previous_line_start.buffer_pos;
	result.last_position = iterator.pos.buffer_pos;
}

void StringValueScanner::FinalizeChunkProcess() {
	if (result.result_position >= result.vector_size || iterator.done) {
		// We are done
		return;
	}
	// If we are not done we have two options.
	// 1) If a boundary is set.
	if (iterator.IsBoundarySet()) {
		iterator.done = true;
		// We read until the next line or until we have nothing else to read.
		// Move to next buffer
		if (!cur_buffer_handle) {
			return;
		}
		bool moved = MoveToNextBuffer();
		if (cur_buffer_handle) {
			if (moved && result.result_position % result.number_of_columns != 0) {
				ProcessExtraRow();
			} else if (!moved) {
				ProcessExtraRow();
			}
			if (cur_buffer_handle->is_last_buffer && iterator.pos.buffer_pos >= cur_buffer_handle->actual_size) {
				MoveToNextBuffer();
			}
		}
	} else {
		// 2) If a boundary is not set
		// We read until the chunk is complete, or we have nothing else to read.
		while (!FinishedFile() && result.result_position < result.vector_size) {
			MoveToNextBuffer();
			if (result.result_position >= result.vector_size) {
				return;
			}
			if (cur_buffer_handle) {
				Process(result);
			}
		}
		iterator.done = FinishedFile();
		if (result.null_padding) {
			while (result.result_position % result.number_of_columns != 0) {
				result.validity_mask->SetInvalid(result.result_position++);
			}
		}
	}
}
} // namespace duckdb
