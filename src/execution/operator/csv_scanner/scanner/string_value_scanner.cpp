#include "duckdb/execution/operator/csv_scanner/string_value_scanner.hpp"

#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/double_cast_operator.hpp"
#include "duckdb/common/operator/integer_cast_operator.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_casting.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/client_data.hpp"
#include "utf8proc_wrapper.hpp"

#include <algorithm>

namespace duckdb {

StringValueResult::StringValueResult(CSVStates &states, CSVStateMachine &state_machine,
                                     const shared_ptr<CSVBufferHandle> &buffer_handle, Allocator &buffer_allocator,
                                     bool figure_out_new_line_p, idx_t buffer_position, CSVErrorHandler &error_hander_p,
                                     CSVIterator &iterator_p, bool store_line_size_p,
                                     shared_ptr<CSVFileScan> csv_file_scan_p, idx_t &lines_read_p, bool sniffing_p,
                                     string path_p)
    : ScannerResult(states, state_machine),
      number_of_columns(NumericCast<uint32_t>(state_machine.dialect_options.num_cols)),
      null_padding(state_machine.options.null_padding), ignore_errors(state_machine.options.ignore_errors.GetValue()),
      figure_out_new_line(figure_out_new_line_p), error_handler(error_hander_p), iterator(iterator_p),
      store_line_size(store_line_size_p), csv_file_scan(std::move(csv_file_scan_p)), lines_read(lines_read_p),
      current_errors(state_machine.options.IgnoreErrors()), sniffing(sniffing_p), path(std::move(path_p)) {
	// Vector information
	D_ASSERT(number_of_columns > 0);
	buffer_handles[buffer_handle->buffer_idx] = buffer_handle;
	// Buffer Information
	buffer_ptr = buffer_handle->Ptr();
	buffer_size = buffer_handle->actual_size;
	last_position = {buffer_handle->buffer_idx, buffer_position, buffer_size};
	requested_size = buffer_handle->requested_size;
	result_size = figure_out_new_line ? 1 : STANDARD_VECTOR_SIZE;

	// Current Result information
	current_line_position.begin = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, buffer_handle->actual_size};
	current_line_position.end = current_line_position.begin;
	// Fill out Parse Types
	vector<LogicalType> logical_types;
	parse_types = make_unsafe_uniq_array<ParseTypeInfo>(number_of_columns);
	LogicalType varchar_type = LogicalType::VARCHAR;
	if (!csv_file_scan) {
		for (idx_t i = 0; i < number_of_columns; i++) {
			parse_types[i] = ParseTypeInfo(varchar_type, true);
			logical_types.emplace_back(LogicalType::VARCHAR);
			string name = "Column_" + to_string(i);
			names.emplace_back(name);
		}
	} else {
		if (csv_file_scan->file_types.size() > number_of_columns) {
			throw InvalidInputException(
			    "Mismatch between the number of columns (%d) in the CSV file and what is expected in the scanner (%d).",
			    number_of_columns, csv_file_scan->file_types.size());
		}
		for (idx_t i = 0; i < csv_file_scan->file_types.size(); i++) {
			auto &type = csv_file_scan->file_types[i];
			if (StringValueScanner::CanDirectlyCast(type)) {
				parse_types[i] = ParseTypeInfo(type, true);
				logical_types.emplace_back(type);
			} else {
				parse_types[i] = ParseTypeInfo(varchar_type, type.id() == LogicalTypeId::VARCHAR || type.IsNested());
				logical_types.emplace_back(LogicalType::VARCHAR);
			}
		}
		names = csv_file_scan->names;
		if (!csv_file_scan->projected_columns.empty()) {
			projecting_columns = false;
			projected_columns = make_unsafe_uniq_array<bool>(number_of_columns);
			for (idx_t col_idx = 0; col_idx < number_of_columns; col_idx++) {
				if (csv_file_scan->projected_columns.find(col_idx) == csv_file_scan->projected_columns.end()) {
					// Column is not projected
					projecting_columns = true;
					projected_columns[col_idx] = false;
				} else {
					projected_columns[col_idx] = true;
				}
			}
		}
		if (!projecting_columns) {
			for (idx_t j = logical_types.size(); j < number_of_columns; j++) {
				// This can happen if we have sneaky null columns at the end that we wish to ignore
				parse_types[j] = ParseTypeInfo(varchar_type, true);
				logical_types.emplace_back(LogicalType::VARCHAR);
			}
		}
	}

	// Initialize Parse Chunk
	parse_chunk.Initialize(buffer_allocator, logical_types, result_size);
	for (auto &col : parse_chunk.data) {
		vector_ptr.push_back(FlatVector::GetData<string_t>(col));
		validity_mask.push_back(&FlatVector::Validity(col));
	}

	// Setup the NullStr information
	null_str_count = state_machine.options.null_str.size();
	null_str_ptr = make_unsafe_uniq_array<const char *>(null_str_count);
	null_str_size = make_unsafe_uniq_array<idx_t>(null_str_count);
	for (idx_t i = 0; i < null_str_count; i++) {
		null_str_ptr[i] = state_machine.options.null_str[i].c_str();
		null_str_size[i] = state_machine.options.null_str[i].size();
	}
	date_format = state_machine.options.dialect_options.date_format.at(LogicalTypeId::DATE).GetValue();
	timestamp_format = state_machine.options.dialect_options.date_format.at(LogicalTypeId::TIMESTAMP).GetValue();
	decimal_separator = state_machine.options.decimal_separator[0];

	if (iterator.first_one) {
		lines_read +=
		    state_machine.dialect_options.skip_rows.GetValue() + state_machine.dialect_options.header.GetValue();
		if (lines_read == 0) {
			SkipBOM();
		}
	}
}

StringValueResult::~StringValueResult() {
	// We have to insert the lines read by this scanner
	error_handler.Insert(iterator.GetBoundaryIdx(), lines_read);
	if (!iterator.done) {
		// Some operators, like Limit, might cause a future error to incorrectly report the wrong error line
		// Better to print nothing to print something wrong
		error_handler.DontPrintErrorLine();
	}
}

inline bool IsValueNull(const char *null_str_ptr, const char *value_ptr, const idx_t size) {
	for (idx_t i = 0; i < size; i++) {
		if (null_str_ptr[i] != value_ptr[i]) {
			return false;
		}
	}
	return true;
}

bool StringValueResult::HandleTooManyColumnsError(const char *value_ptr, const idx_t size) {
	if (cur_col_id >= number_of_columns) {
		bool error = true;
		if (cur_col_id == number_of_columns && ((quoted && state_machine.options.allow_quoted_nulls) || !quoted)) {
			// we make an exception if the first over-value is null
			bool is_value_null = false;
			for (idx_t i = 0; i < null_str_count; i++) {
				is_value_null = is_value_null || IsValueNull(null_str_ptr[i], value_ptr, size);
			}
			error = !is_value_null;
		}
		if (error) {
			// We error pointing to the current value error.
			current_errors.Insert(CSVErrorType::TOO_MANY_COLUMNS, cur_col_id, chunk_col_id, last_position);
			cur_col_id++;
		}
		// We had an error
		return true;
	}
	return false;
}
void StringValueResult::AddValueToVector(const char *value_ptr, const idx_t size, bool allocate) {
	if (HandleTooManyColumnsError(value_ptr, size)) {
		return;
	}
	if (cur_col_id >= number_of_columns) {
		bool error = true;
		if (cur_col_id == number_of_columns && ((quoted && state_machine.options.allow_quoted_nulls) || !quoted)) {
			// we make an exception if the first over-value is null
			bool is_value_null = false;
			for (idx_t i = 0; i < null_str_count; i++) {
				is_value_null = is_value_null || IsValueNull(null_str_ptr[i], value_ptr, size);
			}
			error = !is_value_null;
		}
		if (error) {
			// We error pointing to the current value error.
			current_errors.Insert(CSVErrorType::TOO_MANY_COLUMNS, cur_col_id, chunk_col_id, last_position);
			cur_col_id++;
		}
		return;
	}

	if (projecting_columns) {
		if (!projected_columns[cur_col_id]) {
			cur_col_id++;
			return;
		}
	}
	for (idx_t i = 0; i < null_str_count; i++) {
		if (size == null_str_size[i]) {
			if (((quoted && state_machine.options.allow_quoted_nulls) || !quoted)) {
				if (IsValueNull(null_str_ptr[i], value_ptr, size)) {
					bool empty = false;
					if (chunk_col_id < state_machine.options.force_not_null.size()) {
						empty = state_machine.options.force_not_null[chunk_col_id];
					}
					if (empty) {
						if (parse_types[chunk_col_id].type_id != LogicalTypeId::VARCHAR) {
							// If it is not a varchar, empty values are not accepted, we must error.
							current_errors.Insert(CSVErrorType::CAST_ERROR, cur_col_id, chunk_col_id, last_position);
						}
						static_cast<string_t *>(vector_ptr[chunk_col_id])[number_of_rows] = string_t();
					} else {
						if (chunk_col_id == number_of_columns) {
							// We check for a weird case, where we ignore an extra value, if it is a null value
							return;
						}
						validity_mask[chunk_col_id]->SetInvalid(number_of_rows);
					}
					cur_col_id++;
					chunk_col_id++;
					return;
				}
			}
		}
	}
	bool success = true;
	switch (parse_types[chunk_col_id].type_id) {
	case LogicalTypeId::TINYINT:
		success = TrySimpleIntegerCast(value_ptr, size, static_cast<int8_t *>(vector_ptr[chunk_col_id])[number_of_rows],
		                               false);
		break;
	case LogicalTypeId::SMALLINT:
		success = TrySimpleIntegerCast(value_ptr, size,
		                               static_cast<int16_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::INTEGER:
		success = TrySimpleIntegerCast(value_ptr, size,
		                               static_cast<int32_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::BIGINT:
		success = TrySimpleIntegerCast(value_ptr, size,
		                               static_cast<int64_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::UTINYINT:
		success = TrySimpleIntegerCast<uint8_t, false>(
		    value_ptr, size, static_cast<uint8_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::USMALLINT:
		success = TrySimpleIntegerCast<uint16_t, false>(
		    value_ptr, size, static_cast<uint16_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::UINTEGER:
		success = TrySimpleIntegerCast<uint32_t, false>(
		    value_ptr, size, static_cast<uint32_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::UBIGINT:
		success = TrySimpleIntegerCast<uint64_t, false>(
		    value_ptr, size, static_cast<uint64_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::DOUBLE:
		success =
		    TryDoubleCast<double>(value_ptr, size, static_cast<double *>(vector_ptr[chunk_col_id])[number_of_rows],
		                          false, state_machine.options.decimal_separator[0]);
		break;
	case LogicalTypeId::FLOAT:
		success = TryDoubleCast<float>(value_ptr, size, static_cast<float *>(vector_ptr[chunk_col_id])[number_of_rows],
		                               false, state_machine.options.decimal_separator[0]);
		break;
	case LogicalTypeId::DATE: {
		if (!date_format.Empty()) {
			success = date_format.TryParseDate(value_ptr, size,
			                                   static_cast<date_t *>(vector_ptr[chunk_col_id])[number_of_rows]);
		} else {
			idx_t pos;
			bool special;
			success = Date::TryConvertDate(
			    value_ptr, size, pos, static_cast<date_t *>(vector_ptr[chunk_col_id])[number_of_rows], special, false);
		}
		break;
	}
	case LogicalTypeId::TIME: {
		idx_t pos;
		success = Time::TryConvertTime(value_ptr, size, pos,
		                               static_cast<dtime_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		if (!timestamp_format.Empty()) {
			success = timestamp_format.TryParseTimestamp(
			    value_ptr, size, static_cast<timestamp_t *>(vector_ptr[chunk_col_id])[number_of_rows]);
		} else {
			success = Timestamp::TryConvertTimestamp(
			              value_ptr, size, static_cast<timestamp_t *>(vector_ptr[chunk_col_id])[number_of_rows]) ==
			          TimestampCastResult::SUCCESS;
		}
		break;
	}
	case LogicalTypeId::DECIMAL: {
		if (decimal_separator == ',') {
			switch (parse_types[chunk_col_id].internal_type) {
			case PhysicalType::INT16:
				success = TryDecimalStringCast<int16_t, ','>(
				    value_ptr, size, static_cast<int16_t *>(vector_ptr[chunk_col_id])[number_of_rows],
				    parse_types[chunk_col_id].width, parse_types[chunk_col_id].scale);
				break;
			case PhysicalType::INT32:
				success = TryDecimalStringCast<int32_t, ','>(
				    value_ptr, size, static_cast<int32_t *>(vector_ptr[chunk_col_id])[number_of_rows],
				    parse_types[chunk_col_id].width, parse_types[chunk_col_id].scale);
				break;
			case PhysicalType::INT64:
				success = TryDecimalStringCast<int64_t, ','>(
				    value_ptr, size, static_cast<int64_t *>(vector_ptr[chunk_col_id])[number_of_rows],
				    parse_types[chunk_col_id].width, parse_types[chunk_col_id].scale);
				break;
			case PhysicalType::INT128:
				success = TryDecimalStringCast<hugeint_t, ','>(
				    value_ptr, size, static_cast<hugeint_t *>(vector_ptr[chunk_col_id])[number_of_rows],
				    parse_types[chunk_col_id].width, parse_types[chunk_col_id].scale);
				break;
			default:
				throw InternalException("Invalid Physical Type for Decimal Value. Physical Type: " +
				                        TypeIdToString(parse_types[chunk_col_id].internal_type));
			}

		} else if (decimal_separator == '.') {
			switch (parse_types[chunk_col_id].internal_type) {
			case PhysicalType::INT16:
				success = TryDecimalStringCast(value_ptr, size,
				                               static_cast<int16_t *>(vector_ptr[chunk_col_id])[number_of_rows],
				                               parse_types[chunk_col_id].width, parse_types[chunk_col_id].scale);
				break;
			case PhysicalType::INT32:
				success = TryDecimalStringCast(value_ptr, size,
				                               static_cast<int32_t *>(vector_ptr[chunk_col_id])[number_of_rows],
				                               parse_types[chunk_col_id].width, parse_types[chunk_col_id].scale);
				break;
			case PhysicalType::INT64:
				success = TryDecimalStringCast(value_ptr, size,
				                               static_cast<int64_t *>(vector_ptr[chunk_col_id])[number_of_rows],
				                               parse_types[chunk_col_id].width, parse_types[chunk_col_id].scale);
				break;
			case PhysicalType::INT128:
				success = TryDecimalStringCast(value_ptr, size,
				                               static_cast<hugeint_t *>(vector_ptr[chunk_col_id])[number_of_rows],
				                               parse_types[chunk_col_id].width, parse_types[chunk_col_id].scale);
				break;
			default:
				throw InternalException("Invalid Physical Type for Decimal Value. Physical Type: " +
				                        TypeIdToString(parse_types[chunk_col_id].internal_type));
			}
		} else {
			throw InvalidInputException("Decimals can only have ',' and '.' as decimal separators");
		}
		break;
	}
	default: {
		// By default, we add a string
		// We only evaluate if a string is utf8 valid, if it's actually a varchar
		if (parse_types[chunk_col_id].validate_utf8 &&
		    !Utf8Proc::IsValid(value_ptr, UnsafeNumericCast<uint32_t>(size))) {
			bool force_error = !state_machine.options.ignore_errors.GetValue() && sniffing;
			// Invalid unicode, we must error
			if (force_error) {
				HandleUnicodeError(cur_col_id, last_position);
			}
			// If we got here, we are ingoring errors, hence we must ignore this line.
			current_errors.Insert(CSVErrorType::INVALID_UNICODE, cur_col_id, chunk_col_id, last_position);
			break;
		}
		if (allocate) {
			// If it's a value produced over multiple buffers, we must allocate
			static_cast<string_t *>(vector_ptr[chunk_col_id])[number_of_rows] = StringVector::AddStringOrBlob(
			    parse_chunk.data[chunk_col_id], string_t(value_ptr, UnsafeNumericCast<uint32_t>(size)));
		} else {
			static_cast<string_t *>(vector_ptr[chunk_col_id])[number_of_rows] =
			    string_t(value_ptr, UnsafeNumericCast<uint32_t>(size));
		}
		break;
	}
	}
	if (!success) {
		current_errors.Insert(CSVErrorType::CAST_ERROR, cur_col_id, chunk_col_id, last_position);
		if (!state_machine.options.IgnoreErrors()) {
			// We have to write the cast error message.
			std::ostringstream error;
			// Casting Error Message
			error << "Could not convert string \"" << std::string(value_ptr, size) << "\" to \'"
			      << LogicalTypeIdToString(parse_types[chunk_col_id].type_id) << "\'";
			current_errors.ModifyErrorMessageOfLastError(error.str());
		}
	}
	cur_col_id++;
	chunk_col_id++;
}

DataChunk &StringValueResult::ToChunk() {
	parse_chunk.SetCardinality(number_of_rows);
	return parse_chunk;
}

void StringValueResult::Reset() {
	if (number_of_rows == 0) {
		return;
	}
	number_of_rows = 0;
	cur_col_id = 0;
	chunk_col_id = 0;
	for (auto &v : validity_mask) {
		v->SetAllValid(result_size);
	}
	// We keep a reference to the buffer from our current iteration if it already exists
	shared_ptr<CSVBufferHandle> cur_buffer;
	if (buffer_handles.find(iterator.GetBufferIdx()) != buffer_handles.end()) {
		cur_buffer = buffer_handles[iterator.GetBufferIdx()];
	}
	buffer_handles.clear();
	if (cur_buffer) {
		buffer_handles[cur_buffer->buffer_idx] = cur_buffer;
	}
	current_errors.Reset();
	borked_rows.clear();
}

void StringValueResult::AddQuotedValue(StringValueResult &result, const idx_t buffer_pos) {
	if (result.escaped) {
		if (result.projecting_columns) {
			if (!result.projected_columns[result.cur_col_id]) {
				result.cur_col_id++;
				result.quoted = false;
				result.escaped = false;
				return;
			}
		}
		if (!result.HandleTooManyColumnsError(result.buffer_ptr + result.quoted_position + 1,
		                                      buffer_pos - result.quoted_position - 2)) {
			// If it's an escaped value we have to remove all the escapes, this is not really great
			auto value = StringValueScanner::RemoveEscape(
			    result.buffer_ptr + result.quoted_position + 1, buffer_pos - result.quoted_position - 2,
			    result.state_machine.dialect_options.state_machine_options.escape.GetValue(),
			    result.parse_chunk.data[result.chunk_col_id]);
			result.AddValueToVector(value.GetData(), value.GetSize());
		}
	} else {
		if (buffer_pos < result.last_position.buffer_pos + 2) {
			// empty value
			auto value = string_t();
			result.AddValueToVector(value.GetData(), value.GetSize());
		} else {
			result.AddValueToVector(result.buffer_ptr + result.quoted_position + 1,
			                        buffer_pos - result.quoted_position - 2);
		}
	}
	result.quoted = false;
	result.escaped = false;
}

void StringValueResult::AddValue(StringValueResult &result, const idx_t buffer_pos) {
	if (result.last_position.buffer_pos > buffer_pos) {
		return;
	}
	if (result.quoted) {
		StringValueResult::AddQuotedValue(result, buffer_pos);
	} else {
		result.AddValueToVector(result.buffer_ptr + result.last_position.buffer_pos,
		                        buffer_pos - result.last_position.buffer_pos);
	}
	result.last_position.buffer_pos = buffer_pos + 1;
}

void StringValueResult::HandleUnicodeError(idx_t col_idx, LinePosition &error_position) {

	bool first_nl;
	auto borked_line = current_line_position.ReconstructCurrentLine(first_nl, buffer_handles);
	LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), lines_read);
	if (current_line_position.begin == error_position) {
		auto csv_error = CSVError::InvalidUTF8(state_machine.options, col_idx, lines_per_batch, borked_line,
		                                       current_line_position.begin.GetGlobalPosition(requested_size, first_nl),
		                                       error_position.GetGlobalPosition(requested_size, first_nl), path);
		error_handler.Error(csv_error, true);
	} else {
		auto csv_error = CSVError::InvalidUTF8(state_machine.options, col_idx, lines_per_batch, borked_line,
		                                       current_line_position.begin.GetGlobalPosition(requested_size, first_nl),
		                                       error_position.GetGlobalPosition(requested_size), path);
		error_handler.Error(csv_error, true);
	}
}

bool LineError::HandleErrors(StringValueResult &result) {
	if (ignore_errors && is_error_in_line && !result.figure_out_new_line) {
		result.cur_col_id = 0;
		result.chunk_col_id = 0;
		result.number_of_rows--;
		Reset();
		return true;
	}
	// Reconstruct CSV Line
	for (auto &cur_error : current_errors) {
		LinesPerBoundary lines_per_batch(result.iterator.GetBoundaryIdx(), result.lines_read);
		bool first_nl;
		auto borked_line = result.current_line_position.ReconstructCurrentLine(first_nl, result.buffer_handles);
		CSVError csv_error;
		auto col_idx = cur_error.col_idx;
		auto &line_pos = cur_error.error_position;

		switch (cur_error.type) {
		case CSVErrorType::TOO_MANY_COLUMNS:
		case CSVErrorType::TOO_FEW_COLUMNS:
			if (result.current_line_position.begin == line_pos) {
				csv_error = CSVError::IncorrectColumnAmountError(
				    result.state_machine.options, col_idx, lines_per_batch, borked_line,
				    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl),
				    line_pos.GetGlobalPosition(result.requested_size, first_nl), result.path);
			} else {
				csv_error = CSVError::IncorrectColumnAmountError(
				    result.state_machine.options, col_idx, lines_per_batch, borked_line,
				    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl),
				    line_pos.GetGlobalPosition(result.requested_size), result.path);
			}
			break;
		case CSVErrorType::INVALID_UNICODE: {
			if (result.current_line_position.begin == line_pos) {
				csv_error = CSVError::InvalidUTF8(
				    result.state_machine.options, col_idx, lines_per_batch, borked_line,
				    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl),
				    line_pos.GetGlobalPosition(result.requested_size, first_nl), result.path);
			} else {
				csv_error = CSVError::InvalidUTF8(
				    result.state_machine.options, col_idx, lines_per_batch, borked_line,
				    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl),
				    line_pos.GetGlobalPosition(result.requested_size), result.path);
			}
			break;
		}
		case CSVErrorType::UNTERMINATED_QUOTES:
			if (result.current_line_position.begin == line_pos) {
				csv_error = CSVError::UnterminatedQuotesError(
				    result.state_machine.options, col_idx, lines_per_batch, borked_line,
				    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl),
				    line_pos.GetGlobalPosition(result.requested_size, first_nl), result.path);
			} else {
				csv_error = CSVError::UnterminatedQuotesError(
				    result.state_machine.options, col_idx, lines_per_batch, borked_line,
				    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl),
				    line_pos.GetGlobalPosition(result.requested_size), result.path);
			}
			break;
		case CSVErrorType::CAST_ERROR:
			if (result.current_line_position.begin == line_pos) {
				csv_error = CSVError::CastError(
				    result.state_machine.options, result.names[cur_error.col_idx], cur_error.error_message,
				    cur_error.col_idx, borked_line, lines_per_batch,
				    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl),
				    line_pos.GetGlobalPosition(result.requested_size, first_nl),
				    result.parse_types[cur_error.chunk_idx].type_id, result.path);
			} else {
				csv_error = CSVError::CastError(
				    result.state_machine.options, result.names[cur_error.col_idx], cur_error.error_message,
				    cur_error.col_idx, borked_line, lines_per_batch,
				    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl),
				    line_pos.GetGlobalPosition(result.requested_size), result.parse_types[cur_error.chunk_idx].type_id,
				    result.path);
			}
			break;
		case CSVErrorType::MAXIMUM_LINE_SIZE:
			csv_error = CSVError::LineSizeError(
			    result.state_machine.options, cur_error.current_line_size, lines_per_batch, borked_line,
			    result.current_line_position.begin.GetGlobalPosition(result.requested_size, first_nl), result.path);
			break;
		default:
			throw InvalidInputException("CSV Error not allowed when inserting row");
		}
		result.error_handler.Error(csv_error);
	}
	if (is_error_in_line) {
		result.borked_rows.insert(result.number_of_rows);
		result.cur_col_id = 0;
		result.chunk_col_id = 0;
		Reset();
		return true;
	}
	return false;
}

void StringValueResult::QuotedNewLine(StringValueResult &result) {
	result.quoted_new_line = true;
}

void StringValueResult::NullPaddingQuotedNewlineCheck() {
	// We do some checks for null_padding correctness
	if (state_machine.options.null_padding && iterator.IsBoundarySet() && quoted_new_line) {
		// If we have null_padding set, we found a quoted new line, we are scanning the file in parallel; We error.
		LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), lines_read);
		auto csv_error = CSVError::NullPaddingFail(state_machine.options, lines_per_batch, path);
		error_handler.Error(csv_error);
	}
}

//! Reconstructs the current line to be used in error messages
string FullLinePosition::ReconstructCurrentLine(bool &first_char_nl,
                                                unordered_map<idx_t, shared_ptr<CSVBufferHandle>> &buffer_handles) {
	string result;
	if (end.buffer_idx == begin.buffer_idx) {
		if (buffer_handles.find(end.buffer_idx) == buffer_handles.end()) {
			throw InternalException("CSV Buffer is not available to reconstruct CSV Line, please open an issue with "
			                        "your query and dataset.");
		}
		auto buffer = buffer_handles[begin.buffer_idx]->Ptr();
		first_char_nl = buffer[begin.buffer_pos] == '\n' || buffer[begin.buffer_pos] == '\r';
		for (idx_t i = begin.buffer_pos + first_char_nl; i < end.buffer_pos; i++) {
			result += buffer[i];
		}
	} else {
		if (buffer_handles.find(begin.buffer_idx) == buffer_handles.end() ||
		    buffer_handles.find(end.buffer_idx) == buffer_handles.end()) {
			throw InternalException("CSV Buffer is not available to reconstruct CSV Line, please open an issue with "
			                        "your query and dataset.");
		}
		auto first_buffer = buffer_handles[begin.buffer_idx]->Ptr();
		auto first_buffer_size = buffer_handles[begin.buffer_idx]->actual_size;
		auto second_buffer = buffer_handles[end.buffer_idx]->Ptr();
		first_char_nl = first_buffer[begin.buffer_pos] == '\n' || first_buffer[begin.buffer_pos] == '\r';
		for (idx_t i = begin.buffer_pos + first_char_nl; i < first_buffer_size; i++) {
			result += first_buffer[i];
		}
		for (idx_t i = 0; i < end.buffer_pos; i++) {
			result += second_buffer[i];
		}
	}
	// sanitize borked line
	std::vector<char> char_array(result.begin(), result.end());
	char_array.push_back('\0'); // Null-terminate the character array
	Utf8Proc::MakeValid(&char_array[0], char_array.size());
	result = {char_array.begin(), char_array.end() - 1};
	return result;
}

bool StringValueResult::AddRowInternal() {
	LinePosition current_line_start = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, buffer_size};
	idx_t current_line_size = current_line_start - current_line_position.end;
	if (store_line_size) {
		error_handler.NewMaxLineSize(current_line_size);
	}
	current_line_position.begin = current_line_position.end;
	current_line_position.end = current_line_start;
	if (current_line_size > state_machine.options.maximum_line_size) {
		current_errors.Insert(CSVErrorType::MAXIMUM_LINE_SIZE, 1, chunk_col_id, last_position, current_line_size);
	}
	if (!state_machine.options.null_padding) {
		for (idx_t col_idx = cur_col_id; col_idx < number_of_columns; col_idx++) {
			current_errors.Insert(CSVErrorType::TOO_FEW_COLUMNS, col_idx - 1, chunk_col_id, last_position);
		}
	}

	if (current_errors.HandleErrors(*this)) {
		line_positions_per_row[number_of_rows] = current_line_position;
		number_of_rows++;
		if (number_of_rows >= result_size) {
			// We have a full chunk
			return true;
		}
		return false;
	}
	NullPaddingQuotedNewlineCheck();
	quoted_new_line = false;
	// We need to check if we are getting the correct number of columns here.
	// If columns are correct, we add it, and that's it.
	if (cur_col_id != number_of_columns) {
		// We have too few columns:
		if (null_padding) {
			while (cur_col_id < number_of_columns) {
				bool empty = false;
				if (cur_col_id < state_machine.options.force_not_null.size()) {
					empty = state_machine.options.force_not_null[cur_col_id];
				}
				if (projecting_columns) {
					if (!projected_columns[cur_col_id]) {
						cur_col_id++;
						continue;
					}
				}
				if (empty) {
					static_cast<string_t *>(vector_ptr[chunk_col_id])[number_of_rows] = string_t();
				} else {
					validity_mask[chunk_col_id]->SetInvalid(number_of_rows);
				}
				cur_col_id++;
				chunk_col_id++;
			}
		} else {
			// If we are not null-padding this is an error
			if (!state_machine.options.IgnoreErrors()) {
				bool first_nl;
				auto borked_line = current_line_position.ReconstructCurrentLine(first_nl, buffer_handles);
				LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), lines_read);
				if (current_line_position.begin == last_position) {
					auto csv_error = CSVError::IncorrectColumnAmountError(
					    state_machine.options, cur_col_id - 1, lines_per_batch, borked_line,
					    current_line_position.begin.GetGlobalPosition(requested_size, first_nl),
					    last_position.GetGlobalPosition(requested_size, first_nl), path);
					error_handler.Error(csv_error);
				} else {
					auto csv_error = CSVError::IncorrectColumnAmountError(
					    state_machine.options, cur_col_id - 1, lines_per_batch, borked_line,
					    current_line_position.begin.GetGlobalPosition(requested_size, first_nl),
					    last_position.GetGlobalPosition(requested_size), path);
					error_handler.Error(csv_error);
				}
			}
			// If we are here we ignore_errors, so we delete this line
			number_of_rows--;
		}
	}
	line_positions_per_row[number_of_rows] = current_line_position;
	cur_col_id = 0;
	chunk_col_id = 0;
	number_of_rows++;
	if (number_of_rows >= result_size) {
		// We have a full chunk
		return true;
	}
	return false;
}

bool StringValueResult::AddRow(StringValueResult &result, const idx_t buffer_pos) {
	if (result.last_position.buffer_pos <= buffer_pos) {
		// We add the value
		if (result.quoted) {
			StringValueResult::AddQuotedValue(result, buffer_pos);
		} else {
			result.AddValueToVector(result.buffer_ptr + result.last_position.buffer_pos,
			                        buffer_pos - result.last_position.buffer_pos);
		}
		if (result.state_machine.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
			if (result.states.states[1] == CSVState::RECORD_SEPARATOR) {
				// Even though this is marked as a carry on, this is a hippie mixie
				result.last_position.buffer_pos = buffer_pos + 1;
			} else {
				result.last_position.buffer_pos = buffer_pos + 2;
			}
		} else {
			result.last_position.buffer_pos = buffer_pos + 1;
		}
	}

	// We add the value
	return result.AddRowInternal();
}

void StringValueResult::InvalidState(StringValueResult &result) {
	bool force_error = !result.state_machine.options.ignore_errors.GetValue() && result.sniffing;
	// Invalid unicode, we must error
	if (force_error) {
		result.HandleUnicodeError(result.cur_col_id, result.last_position);
	}
	result.current_errors.Insert(CSVErrorType::UNTERMINATED_QUOTES, result.cur_col_id, result.chunk_col_id,
	                             result.last_position);
}

bool StringValueResult::EmptyLine(StringValueResult &result, const idx_t buffer_pos) {
	// We care about empty lines if this is a single column csv file
	result.last_position = {result.iterator.pos.buffer_idx, result.iterator.pos.buffer_pos + 1, result.buffer_size};
	if (result.states.IsCarriageReturn() &&
	    result.state_machine.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		result.last_position.buffer_pos++;
	}
	if (result.number_of_columns == 1) {
		for (idx_t i = 0; i < result.null_str_count; i++) {
			if (result.null_str_size[i] == 0) {
				bool empty = false;
				if (!result.state_machine.options.force_not_null.empty()) {
					empty = result.state_machine.options.force_not_null[0];
				}
				if (empty) {
					static_cast<string_t *>(result.vector_ptr[0])[result.number_of_rows] = string_t();
				} else {
					result.validity_mask[0]->SetInvalid(result.number_of_rows);
				}
				result.number_of_rows++;
			}
		}
		if (result.number_of_rows >= result.result_size) {
			// We have a full chunk
			return true;
		}
	}
	return false;
}

StringValueScanner::StringValueScanner(idx_t scanner_idx_p, const shared_ptr<CSVBufferManager> &buffer_manager,
                                       const shared_ptr<CSVStateMachine> &state_machine,
                                       const shared_ptr<CSVErrorHandler> &error_handler,
                                       const shared_ptr<CSVFileScan> &csv_file_scan, bool sniffing,
                                       CSVIterator boundary, bool figure_out_nl)
    : BaseScanner(buffer_manager, state_machine, error_handler, sniffing, csv_file_scan, boundary),
      scanner_idx(scanner_idx_p),
      result(states, *state_machine, cur_buffer_handle, BufferAllocator::Get(buffer_manager->context), figure_out_nl,
             iterator.pos.buffer_pos, *error_handler, iterator,
             buffer_manager->context.client_data->debug_set_max_line_length, csv_file_scan, lines_read, sniffing,
             buffer_manager->GetFilePath()) {
}

StringValueScanner::StringValueScanner(const shared_ptr<CSVBufferManager> &buffer_manager,
                                       const shared_ptr<CSVStateMachine> &state_machine,
                                       const shared_ptr<CSVErrorHandler> &error_handler, CSVIterator boundary)
    : BaseScanner(buffer_manager, state_machine, error_handler, false, nullptr, boundary), scanner_idx(0),
      result(states, *state_machine, cur_buffer_handle, Allocator::DefaultAllocator(), false, iterator.pos.buffer_pos,
             *error_handler, iterator, buffer_manager->context.client_data->debug_set_max_line_length, csv_file_scan,
             lines_read, sniffing, buffer_manager->GetFilePath()) {
}

unique_ptr<StringValueScanner> StringValueScanner::GetCSVScanner(ClientContext &context, CSVReaderOptions &options) {
	// Its possible we might have to do some skipping first
	auto state_machine = make_shared_ptr<CSVStateMachine>(options, options.dialect_options.state_machine_options,
	                                                      CSVStateMachineCache::Get(context));

	state_machine->dialect_options.num_cols = options.dialect_options.num_cols;
	state_machine->dialect_options.header = options.dialect_options.header;
	auto buffer_manager = make_shared_ptr<CSVBufferManager>(context, options, options.file_path, 0);
	idx_t rows_to_skip = state_machine->options.GetSkipRows() + state_machine->options.GetHeader();
	auto it = BaseScanner::SkipCSVRows(buffer_manager, state_machine, rows_to_skip);
	auto scanner = make_uniq<StringValueScanner>(buffer_manager, state_machine, make_shared_ptr<CSVErrorHandler>(), it);
	scanner->csv_file_scan = make_shared_ptr<CSVFileScan>(context, options.file_path, options);
	scanner->csv_file_scan->InitializeProjection();
	return scanner;
}

bool StringValueScanner::FinishedIterator() {
	return iterator.done;
}

StringValueResult &StringValueScanner::ParseChunk() {
	result.Reset();
	ParseChunkInternal(result);
	return result;
}

void StringValueScanner::Flush(DataChunk &insert_chunk) {
	auto &process_result = ParseChunk();
	// First Get Parsed Chunk
	auto &parse_chunk = process_result.ToChunk();
	// We have to check if we got to error
	error_handler->ErrorIfNeeded();
	if (parse_chunk.size() == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);

	// We keep track of the borked lines, in case we are ignoring errors
	D_ASSERT(csv_file_scan);

	auto &reader_data = csv_file_scan->reader_data;
	// Now Do the cast-aroo
	for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
		idx_t col_idx = c;
		idx_t result_idx = reader_data.column_mapping[c];
		if (!csv_file_scan->projection_ids.empty()) {
			result_idx = reader_data.column_mapping[csv_file_scan->projection_ids[c].second];
		}
		if (col_idx >= parse_chunk.ColumnCount()) {
			throw InvalidInputException("Mismatch between the schema of different files");
		}
		auto &parse_vector = parse_chunk.data[col_idx];
		auto &result_vector = insert_chunk.data[result_idx];
		auto &type = result_vector.GetType();
		auto &parse_type = parse_vector.GetType();
		if (type == LogicalType::VARCHAR || (type != LogicalType::VARCHAR && parse_type != LogicalType::VARCHAR)) {
			// reinterpret rather than reference
			result_vector.Reinterpret(parse_vector);
		} else {
			string error_message;
			idx_t line_error = 0;
			if (VectorOperations::TryCast(buffer_manager->context, parse_vector, result_vector, parse_chunk.size(),
			                              &error_message, false, true)) {
				continue;
			}
			// An error happened, to propagate it we need to figure out the exact line where the casting failed.
			UnifiedVectorFormat inserted_column_data;
			result_vector.ToUnifiedFormat(parse_chunk.size(), inserted_column_data);
			UnifiedVectorFormat parse_column_data;
			parse_vector.ToUnifiedFormat(parse_chunk.size(), parse_column_data);

			for (; line_error < parse_chunk.size(); line_error++) {
				if (!inserted_column_data.validity.RowIsValid(line_error) &&
				    parse_column_data.validity.RowIsValid(line_error)) {
					break;
				}
			}
			{
				vector<Value> row;

				if (state_machine->options.ignore_errors.GetValue()) {
					for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
						row.push_back(parse_chunk.GetValue(col, line_error));
					}
				}
				if (!state_machine->options.IgnoreErrors()) {
					LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(),
					                                 lines_read - parse_chunk.size() + line_error);
					bool first_nl;
					auto borked_line = result.line_positions_per_row[line_error].ReconstructCurrentLine(
					    first_nl, result.buffer_handles);
					std::ostringstream error;
					error << "Could not convert string \"" << parse_vector.GetValue(line_error) << "\" to \'"
					      << LogicalTypeIdToString(type.id()) << "\'";
					string error_msg = error.str();
					auto csv_error = CSVError::CastError(
					    state_machine->options, csv_file_scan->names[col_idx], error_msg, col_idx, borked_line,
					    lines_per_batch,
					    result.line_positions_per_row[line_error].begin.GetGlobalPosition(result.result_size, first_nl),
					    optional_idx::Invalid(), result_vector.GetType().id(), result.path);
					error_handler->Error(csv_error);
				}
			}
			result.borked_rows.insert(line_error++);
			D_ASSERT(state_machine->options.ignore_errors.GetValue());
			// We are ignoring errors. We must continue but ignoring borked rows
			for (; line_error < parse_chunk.size(); line_error++) {
				if (!inserted_column_data.validity.RowIsValid(line_error) &&
				    parse_column_data.validity.RowIsValid(line_error)) {
					result.borked_rows.insert(line_error);
					vector<Value> row;
					for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
						row.push_back(parse_chunk.GetValue(col, line_error));
					}
					if (!state_machine->options.IgnoreErrors()) {
						LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(),
						                                 lines_read - parse_chunk.size() + line_error);
						bool first_nl;
						auto borked_line = result.line_positions_per_row[line_error].ReconstructCurrentLine(
						    first_nl, result.buffer_handles);
						std::ostringstream error;
						// Casting Error Message
						error << "Could not convert string \"" << parse_vector.GetValue(line_error) << "\" to \'"
						      << LogicalTypeIdToString(type.id()) << "\'";
						string error_msg = error.str();
						auto csv_error =
						    CSVError::CastError(state_machine->options, csv_file_scan->names[col_idx], error_msg,
						                        col_idx, borked_line, lines_per_batch,
						                        result.line_positions_per_row[line_error].begin.GetGlobalPosition(
						                            result.result_size, first_nl),
						                        optional_idx::Invalid(), result_vector.GetType().id(), result.path);
						error_handler->Error(csv_error);
					}
				}
			}
		}
	}
	if (!result.borked_rows.empty()) {
		// We must remove the borked lines from our chunk
		SelectionVector succesful_rows(parse_chunk.size());
		idx_t sel_idx = 0;
		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {
			if (result.borked_rows.find(row_idx) == result.borked_rows.end()) {
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
	result.last_position = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, cur_buffer_handle->actual_size};
	result.current_line_position.begin = result.last_position;

	result.current_line_position.end = result.current_line_position.begin;
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
			return;
		case CSVState::RECORD_SEPARATOR:
			if (states.states[0] == CSVState::RECORD_SEPARATOR) {
				result.EmptyLine(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				lines_read++;
				return;
			} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
				result.AddRow(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				lines_read++;
				return;
			}
			lines_read++;
			iterator.pos.buffer_pos++;
			break;
		case CSVState::CARRIAGE_RETURN:
			if (states.states[0] != CSVState::RECORD_SEPARATOR) {
				result.AddRow(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				lines_read++;
				return;
			} else {
				result.EmptyLine(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				lines_read++;
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
			result.SetQuoted(result, iterator.pos.buffer_pos);
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
	if (result.last_position.buffer_pos == previous_buffer_handle->actual_size) {
		state_machine->Transition(states, previous_buffer[result.last_position.buffer_pos - 1]);
	}
	idx_t j = 0;
	result.quoted = false;
	for (idx_t i = result.last_position.buffer_pos; i < previous_buffer_handle->actual_size; i++) {
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
			result.SetQuoted(result, j);
		}
		if (states.IsEscaped()) {
			result.escaped = true;
		}
		if (states.IsInvalid()) {
			result.InvalidState(result);
		}
		j++;
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
			result.SetQuoted(result, j);
		}
		if (states.IsEscaped()) {
			result.escaped = true;
		}
		if (states.IsInvalid()) {
			result.InvalidState(result);
		}
		j++;
	}
	bool skip_value = false;
	if (result.projecting_columns) {
		if (!result.projected_columns[result.cur_col_id]) {
			result.cur_col_id++;
			skip_value = true;
		}
	}
	if (!skip_value) {
		string_t value;
		if (result.quoted) {
			value = string_t(overbuffer_string.c_str() + result.quoted_position,
			                 UnsafeNumericCast<uint32_t>(overbuffer_string.size() - 1 - result.quoted_position));
			if (result.escaped) {
				const auto str_ptr = static_cast<const char *>(overbuffer_string.c_str() + result.quoted_position);
				value = StringValueScanner::RemoveEscape(
				    str_ptr, overbuffer_string.size() - 2,
				    state_machine->dialect_options.state_machine_options.escape.GetValue(),
				    result.parse_chunk.data[result.chunk_col_id]);
			}
		} else {
			value = string_t(overbuffer_string.c_str(), UnsafeNumericCast<uint32_t>(overbuffer_string.size()));
		}
		if (states.EmptyLine() && state_machine->dialect_options.num_cols == 1) {
			result.EmptyLine(result, iterator.pos.buffer_pos);
		} else if (!states.IsNotSet()) {
			result.AddValueToVector(value.GetData(), value.GetSize(), true);
		}
	} else {
		if (states.EmptyLine() && state_machine->dialect_options.num_cols == 1) {
			result.EmptyLine(result, iterator.pos.buffer_pos);
		}
	}

	if (states.NewRow() && !states.IsNotSet()) {
		result.AddRowInternal();
		lines_read++;
	}

	if (iterator.pos.buffer_pos >= cur_buffer_handle->actual_size && cur_buffer_handle->is_last_buffer) {
		result.added_last_line = true;
	}
	if (states.IsCarriageReturn() &&
	    state_machine->dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		result.last_position = {iterator.pos.buffer_idx, ++iterator.pos.buffer_pos + 1, result.buffer_size};
	} else {
		result.last_position = {iterator.pos.buffer_idx, ++iterator.pos.buffer_pos, result.buffer_size};
	}
	// Be sure to reset the quoted and escaped variables
	result.quoted = false;
	result.escaped = false;
}

bool StringValueScanner::MoveToNextBuffer() {
	if (iterator.pos.buffer_pos >= cur_buffer_handle->actual_size) {
		previous_buffer_handle = cur_buffer_handle;
		cur_buffer_handle = buffer_manager->GetBuffer(++iterator.pos.buffer_idx);
		if (!cur_buffer_handle) {
			iterator.pos.buffer_idx--;
			buffer_handle_ptr = nullptr;
			// We do not care if it's a quoted new line on the last row of our file.
			result.quoted_new_line = false;
			// This means we reached the end of the file, we must add a last line if there is any to be added
			if (states.EmptyLine() || states.NewRow() || result.added_last_line || states.IsCurrentNewRow() ||
			    states.IsNotSet()) {
				if (result.cur_col_id == result.number_of_columns) {
					result.number_of_rows++;
				}
				result.cur_col_id = 0;
				result.chunk_col_id = 0;
				return false;
			} else if (states.NewValue()) {
				// we add the value
				result.AddValue(result, previous_buffer_handle->actual_size);
				// And an extra empty value to represent what comes after the delimiter
				result.AddRow(result, previous_buffer_handle->actual_size);
				lines_read++;
			} else if (states.IsQuotedCurrent()) {
				// Unterminated quote
				LinePosition current_line_start = {iterator.pos.buffer_idx, iterator.pos.buffer_pos,
				                                   result.buffer_size};
				result.current_line_position.begin = result.current_line_position.end;
				result.current_line_position.end = current_line_start;
				result.InvalidState(result);
			} else {
				result.AddRow(result, previous_buffer_handle->actual_size);
				lines_read++;
			}
			return false;
		}
		result.buffer_handles[cur_buffer_handle->buffer_idx] = cur_buffer_handle;

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

void StringValueResult::SkipBOM() {
	if (buffer_size >= 3 && buffer_ptr[0] == '\xEF' && buffer_ptr[1] == '\xBB' && buffer_ptr[2] == '\xBF' &&
	    iterator.pos.buffer_pos == 0) {
		iterator.pos.buffer_pos = 3;
	}
}

void StringValueScanner::SkipUntilNewLine() {
	// Now skip until next newline
	if (state_machine->options.dialect_options.state_machine_options.new_line.GetValue() ==
	    NewLineIdentifier::CARRY_ON) {
		bool carriage_return = false;
		bool not_carriage_return = false;
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\r') {
				carriage_return = true;
			} else if (buffer_handle_ptr[iterator.pos.buffer_pos] != '\n') {
				not_carriage_return = true;
			}
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n') {
				if (carriage_return || not_carriage_return) {
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

bool StringValueScanner::CanDirectlyCast(const LogicalType &type) {

	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME:
	case LogicalTypeId::DECIMAL:
	case LogicalType::VARCHAR:
		return true;
	default:
		return false;
	}
}

void StringValueScanner::SetStart() {
	if (iterator.first_one) {
		if (result.store_line_size) {
			result.error_handler.NewMaxLineSize(iterator.pos.buffer_pos);
		}
		return;
	}
	// We have to look for a new line that fits our schema
	// 1. We walk until the next new line
	bool line_found;
	unique_ptr<StringValueScanner> scan_finder;
	do {
		SkipUntilNewLine();
		if (state_machine->options.null_padding) {
			// When Null Padding, we assume we start from the correct new-line
			return;
		}
		scan_finder =
		    make_uniq<StringValueScanner>(0U, buffer_manager, state_machine, make_shared_ptr<CSVErrorHandler>(true),
		                                  csv_file_scan, false, iterator, true);
		auto &tuples = scan_finder->ParseChunk();
		line_found = true;
		if (tuples.number_of_rows != 1 ||
		    (!tuples.borked_rows.empty() && !state_machine->options.ignore_errors.GetValue())) {
			line_found = false;
			// If no tuples were parsed, this is not the correct start, we need to skip until the next new line
			// Or if columns don't match, this is not the correct start, we need to skip until the next new line
			if (scan_finder->previous_buffer_handle) {
				if (scan_finder->iterator.pos.buffer_pos >= scan_finder->previous_buffer_handle->actual_size &&
				    scan_finder->previous_buffer_handle->is_last_buffer) {
					iterator.pos.buffer_idx = scan_finder->iterator.pos.buffer_idx;
					iterator.pos.buffer_pos = scan_finder->iterator.pos.buffer_pos;
					result.last_position = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, result.buffer_size};
					iterator.done = scan_finder->iterator.done;
					return;
				}
			}
			if (iterator.pos.buffer_pos == cur_buffer_handle->actual_size ||
			    scan_finder->iterator.GetBufferIdx() > iterator.GetBufferIdx()) {
				// If things go terribly wrong, we never loop indefinetly.
				iterator.pos.buffer_idx = scan_finder->iterator.pos.buffer_idx;
				iterator.pos.buffer_pos = scan_finder->iterator.pos.buffer_pos;
				result.last_position = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, result.buffer_size};
				iterator.done = scan_finder->iterator.done;
				return;
			}
		}
	} while (!line_found);
	iterator.pos.buffer_idx = scan_finder->result.current_line_position.begin.buffer_idx;
	iterator.pos.buffer_pos = scan_finder->result.current_line_position.begin.buffer_pos;
	result.last_position = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, result.buffer_size};
}

void StringValueScanner::FinalizeChunkProcess() {
	if (result.number_of_rows >= result.result_size || iterator.done) {
		// We are done
		if (!sniffing) {
			if (csv_file_scan) {
				csv_file_scan->bytes_read += bytes_read;
				bytes_read = 0;
			}
		}
		return;
	}
	// If we are not done we have two options.
	// 1) If a boundary is set.
	if (iterator.IsBoundarySet()) {
		if (!result.current_errors.HasErrorType(CSVErrorType::UNTERMINATED_QUOTES)) {
			iterator.done = true;
		}
		// We read until the next line or until we have nothing else to read.
		// Move to next buffer
		if (!cur_buffer_handle) {
			return;
		}
		bool moved = MoveToNextBuffer();
		if (cur_buffer_handle) {
			if (moved && result.cur_col_id < result.number_of_columns && result.cur_col_id > 0) {
				ProcessExtraRow();
			} else if (!moved) {
				ProcessExtraRow();
			}
			if (cur_buffer_handle->is_last_buffer && iterator.pos.buffer_pos >= cur_buffer_handle->actual_size) {
				MoveToNextBuffer();
			}
		} else {
			result.current_errors.HandleErrors(result);
		}
		if (!iterator.done) {
			if (iterator.pos.buffer_pos >= iterator.GetEndPos() || iterator.pos.buffer_idx > iterator.GetBufferIdx() ||
			    FinishedFile()) {
				iterator.done = true;
			}
		}
	} else {
		// 2) If a boundary is not set
		// We read until the chunk is complete, or we have nothing else to read.
		while (!FinishedFile() && result.number_of_rows < result.result_size) {
			MoveToNextBuffer();
			if (result.number_of_rows >= result.result_size) {
				return;
			}
			if (cur_buffer_handle) {
				Process(result);
			}
		}
		iterator.done = FinishedFile();
		if (result.null_padding && result.number_of_rows < STANDARD_VECTOR_SIZE && result.chunk_col_id > 0) {
			while (result.chunk_col_id < result.parse_chunk.ColumnCount()) {
				result.validity_mask[result.chunk_col_id++]->SetInvalid(result.number_of_rows);
				result.cur_col_id++;
			}
			result.number_of_rows++;
		}
	}
}
} // namespace duckdb
