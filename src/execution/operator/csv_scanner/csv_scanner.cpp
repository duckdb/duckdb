//#include "duckdb/execution/operator/scan/csv/csv_scanner.hpp"
//
//#include "duckdb/common/operator/cast_operators.hpp"
//#include "duckdb/common/operator/decimal_cast_operators.hpp"
//#include "duckdb/execution/operator/scan/csv/parse_chunk.hpp"
//#include "duckdb/execution/operator/scan/csv/parse_values.hpp"
//#include "duckdb/main/error_manager.hpp"
//#include "utf8proc_wrapper.hpp"
//#include "duckdb/execution/operator/csv_scanner/util/csv_casting.hpp"
//
// namespace duckdb {
//
// CSVScanner::CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
//                       ScannerBoundary boundary_p, idx_t scanner_id_p)
//    : scanner_id(scanner_id_p), boundary(boundary_p), buffer_manager(std::move(buffer_manager_p)),
//      state_machine(state_machine_p) {
//	SetTotalColumns(state_machine->options.dialect_options.num_cols);
//	//	cur_buffer_handle = buffer_manager->GetBuffer(csv_iterator.file_idx, csv_iterator.buffer_idx++);
//	//	vector<LogicalType> varchar_types(total_columns, LogicalType::VARCHAR);
//	//	parse_chunk.Initialize(BufferAllocator::Get(buffer_manager->context), varchar_types);
//}
//
////! Skips all empty lines, until a non-empty line shows up
// struct ProcessSkipEmptyLines {
//	inline static void Initialize(CSVScanner &scanner, idx_t cur_pos) {
//		scanner.states.Initialize(CSVState::STANDARD);
//	}
//	inline static bool Process(CSVScanner &scanner, idx_t &result_pos, char current_char, idx_t current_pos) {
//		auto state_machine = scanner.GetStateMachine();
//		auto &state = scanner.states;
//		state_machine.Transition(scanner.states, current_char);
//		if (state.current_state != CSVState::EMPTY_LINE && state.current_state != CSVState::CARRIAGE_RETURN &&
//		    state.current_state != CSVState::RECORD_SEPARATOR) {
//			result_pos = current_pos;
//			return true;
//		}
//		// Still and empty line so we have to keep going
//		return false;
//	}
//	inline static void Finalize(CSVScanner &scanner, idx_t &result_pos) {
//		// this is a nop
//		return;
//	}
//};
//
// void CSVScanner::SkipEmptyLines() {
//	if (state_machine->options.dialect_options.num_cols == 1) {
//		// If we only have one column, empty lines are null data.
//		return;
//	}
//	Process<ProcessSkipEmptyLines>(*this, csv_iterator.buffer_pos);
//}
//
////! Moves the buffer until the next new line
// struct SkipUntilNewLine {
//	inline static void Initialize(CSVScanner &scanner, idx_t cur_pos) {
//		scanner.states.Initialize(CSVState::STANDARD);
//	}
//	inline static bool Process(CSVScanner &scanner, idx_t &result_pos, char current_char, idx_t current_pos) {
//		auto state_machine = scanner.GetStateMachine();
//		auto &state = scanner.states;
//		if (current_char == '\n') {
//			result_pos = current_pos + 1;
//			return true;
//		}
//		state_machine.Transition(scanner.states, current_char);
//		if (state.NewRow()) {
//			// Next Position is the first line.
//			result_pos = current_pos;
//			return true;
//		}
//		// Still reading the header so we have to keep going
//		return false;
//	}
//	inline static void Finalize(CSVScanner &scanner, idx_t &result_pos) {
//		// this is a nop
//		return;
//	}
//};
//
// void CSVScanner::SkipHeader() {
//	if (!state_machine->options.dialect_options.header.GetValue()) {
//		// No header to skip
//		return;
//	}
//	Process<SkipUntilNewLine>(*this, csv_iterator.buffer_pos);
//}
//
// void CSVScanner::ProcessOverbufferValue() {
//	auto cur_buf = cur_buffer_handle->Ptr();
//	for (; csv_iterator.buffer_pos < cur_buffer_handle->actual_size; csv_iterator.buffer_pos++) {
//		state_machine->Transition(states, cur_buf[csv_iterator.buffer_pos]);
//		if (csv_iterator.bytes_to_read > 0) {
//			csv_iterator.bytes_to_read--;
//		}
//		if (states.NewRow() || states.NewValue()) {
//			break;
//		}
//	}
//	D_ASSERT(states.NewRow() || states.NewValue());
//	if (csv_iterator.buffer_pos == 0) {
//		duck_vector_ptr[current_value_pos] =
//		    string_t(previous_cur_buffer_handle->Ptr() + length, previous_cur_buffer_handle->actual_size - length - 1);
//	} else if (csv_iterator.buffer_pos == 1) {
//		duck_vector_ptr[current_value_pos] =
//		    string_t(previous_cur_buffer_handle->Ptr() + length, previous_cur_buffer_handle->actual_size - length);
//	} else {
//		idx_t first_buffer_size = previous_cur_buffer_handle->actual_size - length;
//		auto string_length = first_buffer_size + csv_iterator.buffer_pos - 1;
//		auto &result_str = duck_vector_ptr[current_value_pos];
//		result_str = StringVector::EmptyString(*duck_vector, string_length);
//		FastMemcpy(result_str.GetDataWriteable(), previous_cur_buffer_handle->Ptr() + length, first_buffer_size);
//		FastMemcpy(result_str.GetDataWriteable() + first_buffer_size, cur_buffer_handle->Ptr(),
//		           string_length - first_buffer_size);
//		result_str.Finalize();
//	}
//	length = csv_iterator.buffer_pos;
//	current_value_pos++;
//}
//

//
// bool CSVScanner::Flush(DataChunk &insert_chunk, idx_t buffer_idx, bool try_add_line) {
//	if (parse_chunk.size() == 0) {
//		return true;
//	}
//
//	//	bool conversion_error_ignored = false;
//
//	// convert the columns in the parsed chunk to the types of the table
//	insert_chunk.SetCardinality(parse_chunk);
//
//	for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
//		auto col_idx = reader_data.column_ids[c];
//		auto result_idx = reader_data.column_mapping[c];
//		auto &parse_vector = parse_chunk.data[col_idx];
//		auto &result_vector = insert_chunk.data[result_idx];
//		auto &type = result_vector.GetType();
//		if (type.id() == LogicalTypeId::VARCHAR) {
//			// target type is varchar: no need to convert
//			// reinterpret rather than reference, so we can deal with user-defined types
//			result_vector.Reinterpret(parse_vector);
//		} else {
//			string error_message;
//			bool success;
//			idx_t line_error = 0;
//			if (!state_machine->options.dialect_options.date_format.at(LogicalTypeId::DATE).GetValue().Empty() &&
//			    type.id() == LogicalTypeId::DATE) {
//				// use the date format to cast the chunk
//				success = CSVCast::TryCastDateVector(state_machine->options.dialect_options.date_format, parse_vector,
//				                                     result_vector, parse_chunk.size(), error_message, line_error);
//			} else if (!state_machine->options.dialect_options.date_format.at(LogicalTypeId::TIMESTAMP)
//			                .GetValue()
//			                .Empty() &&
//			           type.id() == LogicalTypeId::TIMESTAMP) {
//				// use the date format to cast the chunk
//				success =
//				    CSVCast::TryCastTimestampVector(state_machine->options.dialect_options.date_format, parse_vector,
//				                                    result_vector, parse_chunk.size(), error_message);
//			} else if (state_machine->options.decimal_separator != "." &&
//			           (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE)) {
//				success =
//				    CSVCast::TryCastFloatingVectorCommaSeparated(state_machine->options, parse_vector, result_vector,
//				                                                 parse_chunk.size(), error_message, type, line_error);
//			} else if (state_machine->options.decimal_separator != "." && type.id() == LogicalTypeId::DECIMAL) {
//				success = CSVCast::TryCastDecimalVectorCommaSeparated(
//				    state_machine->options, parse_vector, result_vector, parse_chunk.size(), error_message, type);
//			} else {
//				// target type is not varchar: perform a cast
//				success = VectorOperations::TryCast(buffer_manager->context, parse_vector, result_vector,
//				                                    parse_chunk.size(), &error_message);
//			}
//			if (success) {
//				continue;
//			}
//			throw InvalidInputException("error");
//		}
//
//		//			string col_name = to_string(col_idx);
//		//			if (col_idx < names.size()) {
//		//				col_name = "\"" + names[col_idx] + "\"";
//		//			}
//
//		// figure out the exact line number
//		//			if (target_type_not_varchar) {
//		//				UnifiedVectorFormat inserted_column_data;
//		//				result_vector.ToUnifiedFormat(parse_chunk.size(), inserted_column_data);
//		//				for (; line_error < parse_chunk.size(); line_error++) {
//		//					if (!inserted_column_data.validity.RowIsValid(line_error) &&
//		//					    !FlatVector::IsNull(parse_vector, line_error)) {
//		//						break;
//		//					}
//		//				}
//		//			}
//
//		// The line_error must be summed with linenr (All lines emmited from this batch)
//		// But subtracted from the parse_chunk
//		//			D_ASSERT(line_error + linenr >= parse_chunk.size());
//		//			line_error += linenr;
//		//			line_error -= parse_chunk.size();
//		//
//		//			auto error_line = GetLineError(line_error, buffer_idx);
//		//
//		//			if (options.ignore_errors) {
//		//				conversion_error_ignored = true;
//		//
//		//			} else if (options.auto_detect) {
//		//				throw InvalidInputException("%s in column %s, at line %llu.\n\nParser "
//		//				                            "options:\n%s.\n\nConsider either increasing the sample size "
//		//				                            "(SAMPLE_SIZE=X [X rows] or SAMPLE_SIZE=-1 [all rows]), "
//		//				                            "or skipping column conversion (ALL_VARCHAR=1)",
//		//				                            error_message, col_name, error_line, options.ToString());
//		//			} else {
//		//				throw InvalidInputException("%s at line %llu in column %s. Parser options:\n%s ", error_message,
//		//				                            error_line, col_name, options.ToString());
//		//			}
//		//		}
//	}
//	//	if (conversion_error_ignored) {
//	//		D_ASSERT(options.ignore_errors);
//	//
//	//		SelectionVector succesful_rows(parse_chunk.size());
//	//		idx_t sel_size = 0;
//	//
//	//		// Keep track of failed cells
//	//		vector<ErrorLocation> failed_cells;
//	//
//	//		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {
//	//
//	//			auto global_row_idx = row_idx + linenr - parse_chunk.size();
//	//			auto row_line = GetLineError(global_row_idx, buffer_idx, false);
//	//
//	//			bool row_failed = false;
//	//			for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
//	//				auto col_idx = reader_data.column_ids[c];
//	//				auto result_idx = reader_data.column_mapping[c];
//	//
//	//				auto &parse_vector = parse_chunk.data[col_idx];
//	//				auto &result_vector = insert_chunk.data[result_idx];
//	//
//	//				bool was_already_null = FlatVector::IsNull(parse_vector, row_idx);
//	//				if (!was_already_null && FlatVector::IsNull(result_vector, row_idx)) {
//	//					Increment(buffer_idx);
//	//					auto bla = GetLineError(global_row_idx, buffer_idx, false);
//	//					row_idx += bla;
//	//					row_idx -= bla;
//	//					row_failed = true;
//	//					failed_cells.emplace_back(row_idx, col_idx, row_line);
//	//				}
//	//			}
//	//			if (!row_failed) {
//	//				succesful_rows.set_index(sel_size++, row_idx);
//	//			}
//	//		}
//	//
//	//		// Now do a second pass to produce the reject table entries
//	//		if (!failed_cells.empty() && !options.rejects_table_name.empty()) {
//	//			auto limit = options.rejects_limit;
//	//
//	//			auto rejects = CSVRejectsTable::GetOrCreate(context, options.rejects_table_name);
//	//			lock_guard<mutex> lock(rejects->write_lock);
//	//
//	//			// short circuit if we already have too many rejects
//	//			if (limit == 0 || rejects->count < limit) {
//	//				auto &table = rejects->GetTable(context);
//	//				InternalAppender appender(context, table);
//	//				auto file_name = GetFileName();
//	//
//	//				for (auto &cell : failed_cells) {
//	//					if (limit != 0 && rejects->count >= limit) {
//	//						break;
//	//					}
//	//					rejects->count++;
//	//
//	//					auto row_idx = cell.row_idx;
//	//					auto col_idx = cell.col_idx;
//	//					auto row_line = cell.row_line;
//	//
//	//					auto col_name = to_string(col_idx);
//	//					if (col_idx < names.size()) {
//	//						col_name = "\"" + names[col_idx] + "\"";
//	//					}
//	//
//	//					auto &parse_vector = parse_chunk.data[col_idx];
//	//					auto parsed_str = FlatVector::GetData<string_t>(parse_vector)[row_idx];
//	//					auto &type = insert_chunk.data[col_idx].GetType();
//	//					auto row_error_msg = StringUtil::Format("Could not convert string '%s' to '%s'",
//	//					                                        parsed_str.GetString(), type.ToString());
//	//
//	//					// Add the row to the rejects table
//	//					appender.BeginRow();
//	//					appender.Append(string_t(file_name));
//	//					appender.Append(row_line);
//	//					appender.Append(col_idx);
//	//					appender.Append(string_t(col_name));
//	//					appender.Append(parsed_str);
//	//
//	//					if (!options.rejects_recovery_columns.empty()) {
//	//						child_list_t<Value> recovery_key;
//	//						for (auto &key_idx : options.rejects_recovery_column_ids) {
//	//							// Figure out if the recovery key is valid.
//	//							// If not, error out for real.
//	//							auto &component_vector = parse_chunk.data[key_idx];
//	//							if (FlatVector::IsNull(component_vector, row_idx)) {
//	//								throw InvalidInputException("%s at line %llu in column %s. Parser options:\n%s ",
//	//								                            "Could not parse recovery column", row_line, col_name,
//	//								                            options.ToString());
//	//							}
//	//							auto component = Value(FlatVector::GetData<string_t>(component_vector)[row_idx]);
//	//							recovery_key.emplace_back(names[key_idx], component);
//	//						}
//	//						appender.Append(Value::STRUCT(recovery_key));
//	//					}
//	//
//	//					appender.Append(string_t(row_error_msg));
//	//					appender.EndRow();
//	//				}
//	//				appender.Close();
//	//			}
//	//		}
//	//
//	//		// Now slice the insert chunk to only include the succesful rows
//	//		insert_chunk.Slice(succesful_rows, sel_size);
//	//	}
//	parse_chunk.Reset();
//	return true;
//}
//
// void CSVScanner::Process() {
//	Process<ParseChunk>(*this, parse_chunk);
//}
//
// void CSVScanner::Parse(DataChunk &output_chunk, VerificationPositions &verification_positions) {
//	// If necessary we set the start of the buffer, basically where we need to start scanning from
//	bool found_start = SetStart(verification_positions);
//	if (!found_start) {
//		// Nothing to Scan
//		return;
//	}
//	// Now we do the actual parsing
//	// TODO: Check for errors.
//	//	if (mode == ParserMode::SNIFFING) {
//	//		Process<ParseChunk>(*this, output_chunk);
//	//	} else {
//	Process<ParseChunk>(*this, parse_chunk);
//	Flush(output_chunk, 0, false);
//	//	}
//
//	total_rows_emmited += output_chunk.size();
//}
//
// string CSVScanner::ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names)
// { 	for (idx_t i = 0; i < names.size(); i++) { 		auto it = sql_types_per_column.find(names[i]); 		if (it !=
// sql_types_per_column.end()) { 			sql_types_per_column.erase(names[i]); 			continue;
//		}
//	}
//	if (sql_types_per_column.empty()) {
//		return string();
//	}
//	string exception = "COLUMN_TYPES error: Columns with names: ";
//	for (auto &col : sql_types_per_column) {
//		exception += "\"" + col.first + "\",";
//	}
//	exception.pop_back();
//	exception += " do not exist in the CSV File";
//	return exception;
//}
//
// int64_t CSVScanner::GetBufferIndex() {
//	if (cur_buffer_handle) {
//		return cur_buffer_handle->buffer_idx;
//	}
//	return -1;
//}
//
// idx_t CSVScanner::GetTotalRowsEmmited() {
//	return total_rows_emmited;
//}
//
// bool CSVScanner::Finished() {
//	// We consider the scanner done, if there is no buffer handle for a given buffer_idx (i.e., we are done scanning
//	// the file) OR if we exhausted the bytes we were supposed to read
//	return csv_iterator.bytes_to_read == 0;
//}
//
// void CSVScanner::Reset() {
//	if (cur_buffer_handle) {
//		cur_buffer_handle.reset();
//	}
//	csv_iterator.Reset();
//
//	buffer_manager->Initialize();
//}
//
// CSVStateMachineSniffing &CSVScanner::GetStateMachineSniff() {
//	D_ASSERT(state_machine);
//	CSVStateMachineSniffing *sniffing_state_machine = static_cast<CSVStateMachineSniffing *>(state_machine.get());
//	return *sniffing_state_machine;
//}
//
// CSVStateMachine &CSVScanner::GetStateMachine() {
//	D_ASSERT(state_machine);
//	return *state_machine;
//}
//
// bool CSVScanner::Last() {
//	D_ASSERT(cur_buffer_handle);
//	return cur_buffer_handle->is_last_buffer && csv_iterator.buffer_pos + 1 == cur_buffer_handle->actual_size;
//}
//
//} // namespace duckdb
