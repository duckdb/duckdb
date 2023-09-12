#include "duckdb/execution/operator/scan/csv/parallel_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "utf8proc_wrapper.hpp"
#include "utf8proc.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/scan/csv/csv_line_info.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

ParallelCSVReader::ParallelCSVReader(ClientContext &context, CSVReaderOptions options_p,
                                     unique_ptr<CSVBufferRead> buffer_p, idx_t first_pos_first_buffer_p,
                                     const vector<LogicalType> &requested_types, idx_t file_idx_p)
    : BaseCSVReader(context, std::move(options_p), requested_types), file_idx(file_idx_p),
      first_pos_first_buffer(first_pos_first_buffer_p) {
	Initialize(requested_types);
	SetBufferRead(std::move(buffer_p));
}

void ParallelCSVReader::Initialize(const vector<LogicalType> &requested_types) {
	return_types = requested_types;
	InitParseChunk(return_types.size());
}

bool ParallelCSVReader::NewLineDelimiter(bool carry, bool carry_followed_by_nl, bool first_char) {
	// Set the delimiter if not set yet.
	SetNewLineDelimiter(carry, carry_followed_by_nl);
	D_ASSERT(options.dialect_options.new_line == NewLineIdentifier::SINGLE ||
	         options.dialect_options.new_line == NewLineIdentifier::CARRY_ON);
	if (options.dialect_options.new_line == NewLineIdentifier::SINGLE) {
		return (!carry) || (carry && !carry_followed_by_nl);
	}
	return (carry && carry_followed_by_nl) || (!carry && first_char);
}

void ParallelCSVReader::SkipEmptyLines() {
	idx_t new_pos_buffer = position_buffer;
	if (parse_chunk.data.size() == 1) {
		// Empty lines are null data.
		return;
	}
	for (; new_pos_buffer < end_buffer; new_pos_buffer++) {
		if (StringUtil::CharacterIsNewline((*buffer)[new_pos_buffer])) {
			bool carrier_return = (*buffer)[new_pos_buffer] == '\r';
			new_pos_buffer++;
			if (carrier_return && new_pos_buffer < buffer_size && (*buffer)[new_pos_buffer] == '\n') {
				position_buffer++;
			}
			if (new_pos_buffer > end_buffer) {
				return;
			}
			position_buffer = new_pos_buffer;
		} else if ((*buffer)[new_pos_buffer] != ' ') {
			return;
		}
	}
}

bool ParallelCSVReader::SetPosition() {
	if (buffer->buffer->is_first_buffer && start_buffer == position_buffer && start_buffer == first_pos_first_buffer) {
		start_buffer = buffer->buffer->start_position;
		position_buffer = start_buffer;
		verification_positions.beginning_of_first_line = position_buffer;
		verification_positions.end_of_last_line = position_buffer;
		// First buffer doesn't need any setting

		if (options.dialect_options.header) {
			for (; position_buffer < end_buffer; position_buffer++) {
				if (StringUtil::CharacterIsNewline((*buffer)[position_buffer])) {
					bool carrier_return = (*buffer)[position_buffer] == '\r';
					position_buffer++;
					if (carrier_return && position_buffer < buffer_size && (*buffer)[position_buffer] == '\n') {
						position_buffer++;
					}
					if (position_buffer > end_buffer) {
						VerifyLineLength(position_buffer, buffer->batch_index);
						return false;
					}
					SkipEmptyLines();
					if (verification_positions.beginning_of_first_line == 0) {
						verification_positions.beginning_of_first_line = position_buffer;
					}
					VerifyLineLength(position_buffer, buffer->batch_index);
					verification_positions.end_of_last_line = position_buffer;
					return true;
				}
			}
			VerifyLineLength(position_buffer, buffer->batch_index);
			return false;
		}
		SkipEmptyLines();
		if (verification_positions.beginning_of_first_line == 0) {
			verification_positions.beginning_of_first_line = position_buffer;
		}

		verification_positions.end_of_last_line = position_buffer;
		return true;
	}

	// We have to move position up to next new line
	idx_t end_buffer_real = end_buffer;
	// Check if we already start in a valid line
	string error_message;
	bool successfully_read_first_line = false;
	while (!successfully_read_first_line) {
		DataChunk first_line_chunk;
		first_line_chunk.Initialize(allocator, return_types);
		// Ensure that parse_chunk has no gunk when trying to figure new line
		parse_chunk.Reset();
		for (; position_buffer < end_buffer; position_buffer++) {
			if (StringUtil::CharacterIsNewline((*buffer)[position_buffer])) {
				bool carriage_return = (*buffer)[position_buffer] == '\r';
				bool carriage_return_followed = false;
				position_buffer++;
				if (position_buffer < end_buffer) {
					if (carriage_return && (*buffer)[position_buffer] == '\n') {
						carriage_return_followed = true;
						position_buffer++;
					}
				}
				if (NewLineDelimiter(carriage_return, carriage_return_followed, position_buffer - 1 == start_buffer)) {
					break;
				}
			}
		}
		SkipEmptyLines();

		if (position_buffer > buffer_size) {
			break;
		}

		auto pos_check = position_buffer == 0 ? position_buffer : position_buffer - 1;
		if (position_buffer >= end_buffer && !StringUtil::CharacterIsNewline((*buffer)[pos_check])) {
			break;
		}

		if (position_buffer > end_buffer && options.dialect_options.new_line == NewLineIdentifier::CARRY_ON &&
		    (*buffer)[pos_check] == '\n') {
			break;
		}
		idx_t position_set = position_buffer;
		start_buffer = position_buffer;
		// We check if we can add this line
		// disable the projection pushdown while reading the first line
		// otherwise the first line parsing can be influenced by which columns we are reading
		auto column_ids = std::move(reader_data.column_ids);
		auto column_mapping = std::move(reader_data.column_mapping);
		InitializeProjection();
		try {
			successfully_read_first_line = TryParseSimpleCSV(first_line_chunk, error_message, true);
		} catch (...) {
			successfully_read_first_line = false;
		}
		// restore the projection pushdown
		reader_data.column_ids = std::move(column_ids);
		reader_data.column_mapping = std::move(column_mapping);
		end_buffer = end_buffer_real;
		start_buffer = position_set;
		if (position_buffer >= end_buffer) {
			if (successfully_read_first_line) {
				position_buffer = position_set;
			}
			break;
		}
		position_buffer = position_set;
	}
	if (verification_positions.beginning_of_first_line == 0) {
		verification_positions.beginning_of_first_line = position_buffer;
	}
	// Ensure that parse_chunk has no gunk when trying to figure new line
	parse_chunk.Reset();

	verification_positions.end_of_last_line = position_buffer;
	finished = false;
	return successfully_read_first_line;
}

void ParallelCSVReader::SetBufferRead(unique_ptr<CSVBufferRead> buffer_read_p) {
	if (!buffer_read_p->buffer) {
		throw InternalException("ParallelCSVReader::SetBufferRead - CSVBufferRead does not have a buffer to read");
	}
	position_buffer = buffer_read_p->buffer_start;
	start_buffer = buffer_read_p->buffer_start;
	end_buffer = buffer_read_p->buffer_end;
	if (buffer_read_p->next_buffer) {
		buffer_size = buffer_read_p->buffer->actual_size + buffer_read_p->next_buffer->actual_size;
	} else {
		buffer_size = buffer_read_p->buffer->actual_size;
	}
	buffer = std::move(buffer_read_p);

	reached_remainder_state = false;
	verification_positions.beginning_of_first_line = 0;
	verification_positions.end_of_last_line = 0;
	finished = false;
	D_ASSERT(end_buffer <= buffer_size);
}

VerificationPositions ParallelCSVReader::GetVerificationPositions() {
	verification_positions.beginning_of_first_line += buffer->buffer->csv_global_start;
	verification_positions.end_of_last_line += buffer->buffer->csv_global_start;
	return verification_positions;
}

// If BufferRemainder returns false, it means we are done scanning this buffer and should go to the end_state
bool ParallelCSVReader::BufferRemainder() {
	if (position_buffer >= end_buffer && !reached_remainder_state) {
		// First time we finish the buffer piece we should scan here, we set the variables
		// to allow this piece to be scanned up to the end of the buffer or the next new line
		reached_remainder_state = true;
		// end_buffer is allowed to go to buffer size to finish its last line
		end_buffer = buffer_size;
	}
	if (position_buffer >= end_buffer) {
		// buffer ends, return false
		return false;
	}
	// we can still scan stuff, return true
	return true;
}

bool AllNewLine(string_t value, idx_t column_amount) {
	auto value_str = value.GetString();
	if (value_str.empty() && column_amount == 1) {
		// This is a one column (empty)
		return false;
	}
	for (idx_t i = 0; i < value.GetSize(); i++) {
		if (!StringUtil::CharacterIsNewline(value_str[i])) {
			return false;
		}
	}
	return true;
}

bool ParallelCSVReader::TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message, bool try_add_line) {
	// If line is not set, we have to figure it out, we assume whatever is in the first line
	if (options.dialect_options.new_line == NewLineIdentifier::NOT_SET) {
		idx_t cur_pos = position_buffer;
		// we can start in the middle of a new line, so move a bit forward.
		while (cur_pos < end_buffer) {
			if (StringUtil::CharacterIsNewline((*buffer)[cur_pos])) {
				cur_pos++;
			} else {
				break;
			}
		}
		for (; cur_pos < end_buffer; cur_pos++) {
			if (StringUtil::CharacterIsNewline((*buffer)[cur_pos])) {
				bool carriage_return = (*buffer)[cur_pos] == '\r';
				bool carriage_return_followed = false;
				cur_pos++;
				if (cur_pos < end_buffer) {
					if (carriage_return && (*buffer)[cur_pos] == '\n') {
						carriage_return_followed = true;
						cur_pos++;
					}
				}
				SetNewLineDelimiter(carriage_return, carriage_return_followed);
				break;
			}
		}
	}
	// used for parsing algorithm
	if (start_buffer == buffer_size) {
		// Nothing to read
		finished = true;
		return true;
	}
	D_ASSERT(end_buffer <= buffer_size);
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	bool has_quotes = false;

	vector<idx_t> escape_positions;
	if ((start_buffer == buffer->buffer_start || start_buffer == buffer->buffer_end) && !try_add_line) {
		// First time reading this buffer piece
		if (!SetPosition()) {
			finished = true;
			return true;
		}
	}
	if (position_buffer == buffer_size) {
		// Nothing to read
		finished = true;
		return true;
	}
	// Keep track of line size
	idx_t line_start = position_buffer;
	// start parsing the first value
	goto value_start;

value_start : {
	/* state: value_start */
	if (!BufferRemainder()) {
		goto final_state;
	}
	offset = 0;

	// this state parses the first character of a value
	if ((*buffer)[position_buffer] == options.dialect_options.state_machine_options.quote) {
		// quote: actual value starts in the next position
		// move to in_quotes state
		start_buffer = position_buffer + 1;
		goto in_quotes;
	} else {
		// no quote, move to normal parsing state
		start_buffer = position_buffer;
		goto normal;
	}
};

normal : {
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	for (; position_buffer < end_buffer; position_buffer++) {
		auto c = (*buffer)[position_buffer];
		if (c == options.dialect_options.state_machine_options.delimiter) {
			// delimiter: end the value and add it to the chunk
			goto add_value;
		} else if (c == options.dialect_options.state_machine_options.quote && try_add_line) {
			return false;
		} else if (StringUtil::CharacterIsNewline(c)) {
			// newline: add row
			if (column > 0 || try_add_line || parse_chunk.data.size() == 1) {
				goto add_row;
			}
			if (column == 0 && position_buffer == start_buffer) {
				start_buffer++;
			}
		}
	}
	if (!BufferRemainder()) {
		goto final_state;
	} else {
		goto normal;
	}
};

add_value : {
	/* state: Add value to string vector */
	AddValue(buffer->GetValue(start_buffer, position_buffer, offset), column, escape_positions, has_quotes,
	         buffer->local_batch_index);
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	start_buffer = ++position_buffer;
	if (!BufferRemainder()) {
		goto final_state;
	}
	goto value_start;
};

add_row : {
	/* state: Add Row to Parse chunk */
	// check type of newline (\r or \n)
	bool carriage_return = (*buffer)[position_buffer] == '\r';

	AddValue(buffer->GetValue(start_buffer, position_buffer, offset), column, escape_positions, has_quotes,
	         buffer->local_batch_index);
	if (try_add_line) {
		bool success = column == insert_chunk.ColumnCount();
		if (success) {
			idx_t cur_linenr = linenr;
			AddRow(insert_chunk, column, error_message, buffer->local_batch_index);
			success = Flush(insert_chunk, buffer->local_batch_index, true);
			linenr = cur_linenr;
		}
		reached_remainder_state = false;
		parse_chunk.Reset();
		return success;
	} else {
		VerifyLineLength(position_buffer - line_start, buffer->batch_index);
		line_start = position_buffer;
		finished_chunk = AddRow(insert_chunk, column, error_message, buffer->local_batch_index);
	}
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	position_buffer++;
	start_buffer = position_buffer;
	verification_positions.end_of_last_line = position_buffer;
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		// optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
		if (!BufferRemainder()) {
			goto final_state;
		}
		if ((*buffer)[position_buffer] == '\n') {
			if (options.dialect_options.new_line == NewLineIdentifier::SINGLE) {
				error_message = "Wrong NewLine Identifier. Expecting \\r\\n";
				return false;
			}
			// newline after carriage return: skip
			// increase position by 1 and move start to the new position
			start_buffer = ++position_buffer;

			SkipEmptyLines();
			verification_positions.end_of_last_line = position_buffer;
			start_buffer = position_buffer;
			if (reached_remainder_state) {
				goto final_state;
			}
		} else {
			if (options.dialect_options.new_line == NewLineIdentifier::CARRY_ON) {
				error_message = "Wrong NewLine Identifier. Expecting \\r or \\n";
				return false;
			}
		}
		if (!BufferRemainder()) {
			goto final_state;
		}
		if (reached_remainder_state || finished_chunk) {
			goto final_state;
		}
		goto value_start;
	} else {
		if (options.dialect_options.new_line == NewLineIdentifier::CARRY_ON) {
			error_message = "Wrong NewLine Identifier. Expecting \\r or \\n";
			return false;
		}
		if (reached_remainder_state) {
			goto final_state;
		}
		if (!BufferRemainder()) {
			goto final_state;
		}
		SkipEmptyLines();
		if (position_buffer - verification_positions.end_of_last_line > options.buffer_size) {
			error_message = "Line does not fit in one buffer. Increase the buffer size.";
			return false;
		}
		verification_positions.end_of_last_line = position_buffer;
		start_buffer = position_buffer;
		// \n newline, move to value start
		if (finished_chunk) {
			goto final_state;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes this state parses the remainder of a quoted value*/
	has_quotes = true;
	position_buffer++;
	for (; position_buffer < end_buffer; position_buffer++) {
		auto c = (*buffer)[position_buffer];
		if (c == options.dialect_options.state_machine_options.quote) {
			// quote: move to unquoted state
			goto unquote;
		} else if (c == options.dialect_options.state_machine_options.escape) {
			// escape: store the escaped position and move to handle_escape state
			escape_positions.push_back(position_buffer - start_buffer);
			goto handle_escape;
		}
	}
	if (!BufferRemainder()) {
		if (buffer->buffer->is_last_buffer) {
			if (try_add_line) {
				return false;
			}
			// still in quoted state at the end of the file or at the end of a buffer when running multithreaded, error:
			throw InvalidInputException("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
			                            GetLineNumberStr(linenr, linenr_estimated, buffer->local_batch_index).c_str(),
			                            options.ToString());
		} else {
			goto final_state;
		}
	} else {
		position_buffer--;
		goto in_quotes;
	}

unquote : {
	/* state: unquote: this state handles the state directly after we unquote*/
	//
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	position_buffer++;
	if (!BufferRemainder()) {
		offset = 1;
		goto final_state;
	}
	auto c = (*buffer)[position_buffer];
	if (c == options.dialect_options.state_machine_options.quote &&
	    (options.dialect_options.state_machine_options.escape == '\0' ||
	     options.dialect_options.state_machine_options.escape == options.dialect_options.state_machine_options.quote)) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position_buffer - start_buffer);
		goto in_quotes;
	} else if (c == options.dialect_options.state_machine_options.delimiter) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (StringUtil::CharacterIsNewline(c)) {
		offset = 1;
		// FIXME: should this be an assertion?
		D_ASSERT(try_add_line || (!try_add_line && column == parse_chunk.ColumnCount() - 1));
		goto add_row;
	} else if (position_buffer >= end_buffer) {
		// reached end of buffer
		offset = 1;
		goto final_state;
	} else {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of "
		    "row or another quote. (%s). ",
		    options.file_path, GetLineNumberStr(linenr, linenr_estimated, buffer->local_batch_index).c_str(),
		    options.ToString());
		return false;
	}
}
handle_escape : {
	/* state: handle_escape */
	// escape should be followed by a quote or another escape character
	position_buffer++;
	if (!BufferRemainder()) {
		goto final_state;
	}
	if (position_buffer >= buffer_size && buffer->buffer->is_last_buffer) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated, buffer->local_batch_index).c_str(), options.ToString());
		return false;
	}
	if ((*buffer)[position_buffer] != options.dialect_options.state_machine_options.quote &&
	    (*buffer)[position_buffer] != options.dialect_options.state_machine_options.escape) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated, buffer->local_batch_index).c_str(), options.ToString());
		return false;
	}
	// escape was followed by quote or escape, go back to quoted state
	goto in_quotes;
}
final_state : {
	/* state: final_stage reached after we finished reading the end_buffer of the csv buffer */
	// reset end buffer
	end_buffer = buffer->buffer_end;
	if (position_buffer == end_buffer) {
		reached_remainder_state = false;
	}
	if (finished_chunk) {
		if (position_buffer >= end_buffer) {
			if (position_buffer == end_buffer && StringUtil::CharacterIsNewline((*buffer)[position_buffer - 1]) &&
			    position_buffer < buffer_size) {
				// last position is a new line, we still have to go through one more line of this buffer
				finished = false;
			} else {
				finished = true;
			}
		}
		buffer->lines_read += insert_chunk.size();
		return true;
	}
	// If this is the last buffer, we have to read the last value
	if (buffer->buffer->is_last_buffer || !buffer->next_buffer ||
	    (buffer->next_buffer && buffer->next_buffer->is_last_buffer)) {
		if (column > 0 || start_buffer != position_buffer || try_add_line ||
		    (insert_chunk.data.size() == 1 && start_buffer != position_buffer)) {
			// remaining values to be added to the chunk
			auto str_value = buffer->GetValue(start_buffer, position_buffer, offset);
			if (!AllNewLine(str_value, insert_chunk.data.size()) || offset == 0) {
				AddValue(str_value, column, escape_positions, has_quotes, buffer->local_batch_index);
				if (try_add_line) {
					bool success = column == return_types.size();
					if (success) {
						auto cur_linenr = linenr;
						AddRow(insert_chunk, column, error_message, buffer->local_batch_index);
						success = Flush(insert_chunk, buffer->local_batch_index);
						linenr = cur_linenr;
					}
					parse_chunk.Reset();
					reached_remainder_state = false;
					return success;
				} else {
					VerifyLineLength(position_buffer - line_start, buffer->batch_index);
					line_start = position_buffer;
					AddRow(insert_chunk, column, error_message, buffer->local_batch_index);
					if (position_buffer - verification_positions.end_of_last_line > options.buffer_size) {
						error_message = "Line does not fit in one buffer. Increase the buffer size.";
						return false;
					}
					verification_positions.end_of_last_line = position_buffer;
				}
			}
		}
	}
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk, buffer->local_batch_index);
		buffer->lines_read += insert_chunk.size();
	}
	if (position_buffer - verification_positions.end_of_last_line > options.buffer_size) {
		error_message = "Line does not fit in one buffer. Increase the buffer size.";
		return false;
	}
	end_buffer = buffer_size;
	SkipEmptyLines();
	end_buffer = buffer->buffer_end;
	verification_positions.end_of_last_line = position_buffer;
	if (position_buffer >= end_buffer) {
		if (position_buffer >= end_buffer) {
			if (position_buffer == end_buffer && StringUtil::CharacterIsNewline((*buffer)[position_buffer - 1]) &&
			    position_buffer < buffer_size) {
				// last position is a new line, we still have to go through one more line of this buffer
				finished = false;
			} else {
				finished = true;
			}
		}
	}
	return true;
};
}

void ParallelCSVReader::ParseCSV(DataChunk &insert_chunk) {
	string error_message;
	if (!TryParseCSV(ParserMode::PARSING, insert_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

idx_t ParallelCSVReader::GetLineError(idx_t line_error, idx_t buffer_idx, bool stop_at_first) {
	while (true) {
		if (buffer->line_info->CanItGetLine(file_idx, buffer_idx)) {
			auto cur_start = verification_positions.beginning_of_first_line + buffer->buffer->csv_global_start;
			return buffer->line_info->GetLine(buffer_idx, line_error, file_idx, cur_start, false, stop_at_first);
		}
	}
}

bool ParallelCSVReader::TryParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	return TryParseCSV(mode, dummy_chunk, error_message);
}

void ParallelCSVReader::ParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	if (!TryParseCSV(mode, dummy_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

bool ParallelCSVReader::TryParseCSV(ParserMode parser_mode, DataChunk &insert_chunk, string &error_message) {
	mode = parser_mode;
	return TryParseSimpleCSV(insert_chunk, error_message);
}

} // namespace duckdb
