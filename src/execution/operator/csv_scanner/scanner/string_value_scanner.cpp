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
      result_size(result_size_p), error_handler(error_hander_p), iterator(iterator_p),
      store_line_size(store_line_size_p) {
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
		// fixme: has to do some extra checks for null padding
		parse_chunk.data[col_idx].Slice(*vector, selection_vectors[col_idx], number_of_rows);
	}
	parse_chunk.SetCardinality(number_of_rows);
	return parse_chunk;
}

void StringValueResult::AddQuotedValue(StringValueResult &result, const idx_t buffer_pos) {
	if (result.escaped) {
		// If it's an escaped value we have to remove all the escapes, this is not really great
		string removed_escapes;
		StringValueScanner::RemoveEscape(result.buffer_ptr + result.last_position + 1,
		                                 buffer_pos - result.last_position - 2,
		                                 result.state_machine.options.GetEscape()[0], removed_escapes);
		result.vector_ptr[result.result_position++] =
		    StringVector::AddStringOrBlob(*result.vector, string_t(removed_escapes));
	} else {
		auto value = string_t(result.buffer_ptr + result.last_position + 1, buffer_pos - result.last_position - 2);
		result.AddValueToVector(value);
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
	if (result.result_position % result.number_of_columns == 0) {
		// This means this value reached the number of columns in a line. This is fine, if this is the last buffer, and
		// the last buffer position However, if that's not the case, this means we might be reading too many columns.
		result.maybe_too_many_columns = true;
	}
}

void StringValueResult::HandleOverLimitRows() {
	auto csv_error = CSVError::IncorrectColumnAmountError(state_machine.options, vector_ptr, number_of_columns,
	                                                      number_of_columns + result_position % number_of_columns);
	LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), result_position / number_of_columns + 1);
	error_handler.Error(lines_per_batch, csv_error);
	result_position -= result_position % number_of_columns + number_of_columns;
}

void StringValueResult::AddValueToVector(string_t &value) {
	if (((quoted && state_machine.options.allow_quoted_nulls) || !quoted) && value == state_machine.options.null_str) {
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
		vector_ptr[result_position++] = value;
	}
}

void StringValueResult::AddRowInternal(idx_t buffer_pos) {
	if (last_position > buffer_pos) {
		return;
	}
	LinePosition current_line_start = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, buffer_size};
	idx_t current_line_size = current_line_start - previous_line_start;
	if (store_line_size) {
		error_handler.NewMaxLineSize(current_line_size);
	}
	if (current_line_size > state_machine.options.maximum_line_size) {
		auto csv_error = CSVError::LineSizeError(state_machine.options, current_line_size);
		LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), result_position / number_of_columns);
		error_handler.Error(lines_per_batch, csv_error);
	}
	pre_previous_line_start = previous_line_start;
	previous_line_start = current_line_start;
	if (result_position == vector_size) {
		HandleOverLimitRows();
	}
	// We add the value
	if (quoted) {
		StringValueResult::AddQuotedValue(*this, buffer_pos);
	} else {
		auto value = string_t(buffer_ptr + last_position, buffer_pos - last_position);
		AddValueToVector(value);
	}
	if (state_machine.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		if (states.current_state == CSVState::RECORD_SEPARATOR) {
			// Even though this is marked as a carry on, this is a hippie mixie
			last_position = buffer_pos + 1;
		} else {
			last_position = buffer_pos + 2;
		}
	} else {
		last_position = buffer_pos + 1;
	}
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
	// We add the value
	result.AddRowInternal(buffer_pos);

	// We need to check if we are getting the correct number of columns here.
	// If columns are correct, we add it, and that's it.
	if (result.result_position % result.number_of_columns != 0) {
		// If the columns are incorrect:
		// Maybe we have too many columns:
		if (result.maybe_too_many_columns) {
			if (result.result_position % result.number_of_columns == 1 &&
			    !result.validity_mask->RowIsValid(result.result_position - 1)) {
				// This is a weird case, where we ignore an extra value, if it is a null value
				result.result_position--;
				result.validity_mask->SetValid(result.result_position);
			} else {
				result.HandleOverLimitRows();
			}
			D_ASSERT(result.result_position % result.number_of_columns == 0);
			result.maybe_too_many_columns = false;
		}
		// Maybe we have too few columns:
		// 1) if null_padding is on we null pad it
		else if (result.null_padding) {
			while (result.result_position % result.number_of_columns != 0) {
				bool empty = false;
				idx_t cur_pos = result.result_position % result.number_of_columns;
				if (cur_pos < result.state_machine.options.force_not_null.size()) {
					empty = result.state_machine.options.force_not_null[cur_pos];
				}
				if (empty) {
					result.vector_ptr[result.result_position++] = string_t();
				} else {
					result.validity_mask->SetInvalid(result.result_position++);
				}
			}
		} else {
			auto csv_error = CSVError::IncorrectColumnAmountError(result.state_machine.options, result.vector_ptr,
			                                                      result.result_position,
			                                                      result.result_position % result.number_of_columns);
			LinesPerBoundary lines_per_batch(result.iterator.GetBoundaryIdx(),
			                                 result.result_position / result.number_of_columns + 1);
			result.error_handler.Error(lines_per_batch, csv_error);
			result.result_position -= result.result_position % result.number_of_columns;
			D_ASSERT(result.result_position % result.number_of_columns == 0);
		}
	}

	if (result.result_position / result.number_of_columns >= result.result_size) {
		// We have a full chunk
		return true;
	}
	return false;
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
	if (result.parse_chunk.ColumnCount() == 1) {
		if (result.state_machine.options.null_str.empty()) {
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
	}
	return false;
}

idx_t StringValueResult::NumberOfRows() {
	if (result_position % number_of_columns != 0) {
		throw InternalException("Something went wrong when reading the CSV file, more positions than columns. Open an "
		                        "issue on the issue tracker.");
	}
	return result_position / number_of_columns;
}

StringValueScanner::StringValueScanner(idx_t scanner_idx_p, shared_ptr<CSVBufferManager> buffer_manager,
                                       shared_ptr<CSVStateMachine> state_machine,
                                       shared_ptr<CSVErrorHandler> error_handler, CSVIterator boundary,
                                       idx_t result_size)
    : BaseScanner(buffer_manager, state_machine, error_handler, boundary), scanner_idx(scanner_idx_p),
      result(states, *state_machine, *cur_buffer_handle, BufferAllocator::Get(buffer_manager->context), result_size,
             iterator.pos.buffer_pos, *error_handler, iterator,
             buffer_manager->context.client_data->debug_set_max_line_length) {
}

unique_ptr<StringValueScanner> StringValueScanner::GetCSVScanner(ClientContext &context, CSVReaderOptions &options) {
	auto state_machine = make_shared<CSVStateMachine>(options, options.dialect_options.state_machine_options,
	                                                  *CSVStateMachineCache::Get(context));

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

StringValueResult *StringValueScanner::ParseChunk() {
	result.result_position = 0;
	result.validity_mask->SetAllValid(result.vector_size);
	ParseChunkInternal();
	return &result;
}

void StringValueScanner::Flush(DataChunk &insert_chunk) {
	auto process_result = ParseChunk();
	// First Get Parsed Chunk
	auto &parse_chunk = process_result->ToChunk();

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
	states.Initialize(CSVState::RECORD_SEPARATOR);
	if (result.result_size != 1 && !(sniffing && state_machine->options.null_padding &&
	                                 !state_machine->options.dialect_options.skip_rows.IsSetByUser())) {
		SetStart();
	}
	result.last_position = iterator.pos.buffer_pos;
	result.previous_line_start = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, cur_buffer_handle->actual_size};

	result.pre_previous_line_start = result.previous_line_start;
}

void StringValueScanner::Process() {
	idx_t to_pos;
	if (iterator.IsSet()) {
		to_pos = iterator.GetEndPos();
		if (to_pos > cur_buffer_handle->actual_size) {
			to_pos = cur_buffer_handle->actual_size;
		}
	} else {
		to_pos = cur_buffer_handle->actual_size;
	}
	for (; iterator.pos.buffer_pos < to_pos; iterator.pos.buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.pos.buffer_pos], iterator.pos.buffer_pos, result)) {
			iterator.pos.buffer_pos++;
			return;
		}
	}
}

void StringValueScanner::ProcessExtraRow() {
	idx_t to_pos = cur_buffer_handle->actual_size;
	idx_t cur_result_pos = result.result_position;
	if (result.last_position != iterator.pos.buffer_pos - 1) {
		cur_result_pos++;
	}
	if (states.IsCurrentNewRow()) {
		cur_result_pos++;
	}
	for (; iterator.pos.buffer_pos < to_pos; iterator.pos.buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.pos.buffer_pos], iterator.pos.buffer_pos, result) ||
		    (result.result_position >= cur_result_pos &&
		     result.result_position % state_machine->dialect_options.num_cols == 0)) {
			return;
		}
	}
}

void StringValueScanner::RemoveEscape(char *str_ptr, idx_t end, char escape, string &removed_escapes) {
	bool just_escaped = false;
	for (idx_t cur_pos = 0; cur_pos < end; cur_pos++) {
		char c = str_ptr[cur_pos];
		if (c == escape && !just_escaped) {
			just_escaped = true;
		} else {
			just_escaped = false;
			removed_escapes += c;
		}
	}
}

void StringValueScanner::ProcessOverbufferValue() {
	// Process the next value
	idx_t first_buffer_pos = result.last_position;
	while (iterator.pos.buffer_pos < cur_buffer_handle->actual_size) {
		state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
		iterator.pos.buffer_pos++;
		if (states.NewRow() || states.NewValue() || states.EmptyLine()) {
			break;
		}
		if (states.IsQuoted()) {
			result.quoted = true;
		}
		if (states.IsEscaped()) {
			result.escaped = true;
		}
	}
	if (states.NewRow()) {
		lines_read++;
	}
	if (iterator.pos.buffer_pos >= cur_buffer_handle->actual_size && cur_buffer_handle->is_last_buffer) {
		result.added_last_line = true;
	}
	idx_t first_buffer_length = 0;
	if (previous_buffer_handle->actual_size > first_buffer_pos) {
		first_buffer_length = previous_buffer_handle->actual_size - first_buffer_pos - result.quoted;
	}
	if (states.EmptyValue()) {
		auto value = string_t();
		result.AddValueToVector(value);
	} else if (iterator.pos.buffer_pos == 0 || iterator.pos.buffer_pos == 1) {
		// Value is only on the first buffer
		if (result.escaped) {
			string removed_escapes;
			StringValueScanner::RemoveEscape(previous_buffer_handle->Ptr() + first_buffer_pos + result.quoted,
			                                 first_buffer_length - 1, state_machine->options.GetEscape()[0],
			                                 removed_escapes);
			result.vector_ptr[result.result_position++] =
			    StringVector::AddStringOrBlob(*result.vector, string_t(removed_escapes));
		} else {
			auto value = string_t(previous_buffer_handle->Ptr() + first_buffer_pos + result.quoted,
			                      first_buffer_length - result.quoted);
			result.AddValueToVector(value);
		}
	} else {
		// Figure out the max string length
		idx_t string_length;
		if (states.NewRow() || states.NewValue()) {
			string_length = first_buffer_length + iterator.pos.buffer_pos - 1 - result.quoted;
		} else {
			string_length = first_buffer_length + iterator.pos.buffer_pos - result.quoted;
		}
		// Our value is actually over two buffers, we must do copying of the proper parts os the value
		if (result.escaped) {
			string removed_escapes;
			// First Buffer
			StringValueScanner::RemoveEscape(previous_buffer_handle->Ptr() + first_buffer_pos + result.quoted,
			                                 first_buffer_length, state_machine->options.GetEscape()[0],
			                                 removed_escapes);
			// Second Buffer
			if (string_length > first_buffer_length) {
				StringValueScanner::RemoveEscape(cur_buffer_handle->Ptr(), string_length - first_buffer_length,
				                                 state_machine->options.GetEscape()[0], removed_escapes);
			}
			result.vector_ptr[result.result_position++] =
			    StringVector::AddStringOrBlob(*result.vector, string_t(removed_escapes));
		} else {
			auto &result_str = result.vector_ptr[result.result_position];
			result_str = StringVector::EmptyString(*result.vector, string_length);
			// Copy the first buffer
			FastMemcpy(result_str.GetDataWriteable(), previous_buffer_handle->Ptr() + first_buffer_pos + result.quoted,
			           first_buffer_length);
			// Copy the second buffer
			if (string_length > first_buffer_length) {
				FastMemcpy(result_str.GetDataWriteable() + first_buffer_length, cur_buffer_handle->Ptr(),
				           string_length - first_buffer_length);
			}
			result_str.Finalize();
			result.AddValueToVector(result_str);
		}
	}
	result.last_position = iterator.pos.buffer_pos;
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
			// This means we reached the end of the file, we must add a last line if there is any to be added
			if (states.EmptyLine() || states.NewRow() || result.added_last_line || states.IsCurrentNewRow()) {
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
		if (previous_buffer_handle->actual_size != result.last_position) {
			ProcessOverbufferValue();
		} else {
			result.last_position = 0;
		}
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
	if (state_machine->options.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		iterator.pos.buffer_pos = row_skipper.GetIteratorPosition() + 2;
	} else {
		iterator.pos.buffer_pos = row_skipper.GetIteratorPosition() + 1;
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
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n') {
				iterator.pos.buffer_pos++;
				return;
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
	StringValueScanner scan_finder(0, buffer_manager, state_machine, make_shared<CSVErrorHandler>(true), iterator, 1);
	auto &tuples = *scan_finder.ParseChunk();
	if (tuples.Empty() || tuples.Size() != state_machine->options.dialect_options.num_cols) {
		// If no tuples were parsed, this is not the correct start, we need to skip until the next new line
		// Or if columns don't match, this is not the correct start, we need to skip until the next new line
		if (iterator.pos.buffer_pos >= cur_buffer_handle->actual_size && cur_buffer_handle->is_last_buffer) {
			return;
		}
	}
	iterator.pos.buffer_idx = scan_finder.result.pre_previous_line_start.buffer_idx;
	iterator.pos.buffer_pos = scan_finder.result.pre_previous_line_start.buffer_pos;
}

void StringValueScanner::FinalizeChunkProcess() {
	if (result.result_position >= result.vector_size || iterator.done) {
		// We are done
		return;
	}
	// If we are not done we have two options.
	// 1) If a boundary is set.
	if (iterator.IsSet()) {
		iterator.done = true;
		// We read until the next line or until we have nothing else to read.
		// Move to next buffer
		if (!cur_buffer_handle) {
			return;
		}
		auto moved = MoveToNextBuffer();
		if (moved && result.result_position % result.number_of_columns == 0 && result.iterator.pos.buffer_pos != 0) {
			// nothing more to process
			return;
		}
		if (cur_buffer_handle) {
			ProcessExtraRow();
			if (cur_buffer_handle->is_last_buffer && iterator.pos.buffer_pos >= cur_buffer_handle->actual_size) {
				MoveToNextBuffer();
			}
		}
	} else {
		// 2) If a boundary is not set
		// We read until the chunk is complete, or we have nothing else to read.
		while (!FinishedFile() && result.result_position < result.vector_size) {
			MoveToNextBuffer();
			if (result.result_position >= result.vector_size){
				return;
			}
			if (cur_buffer_handle) {
				Process();
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
