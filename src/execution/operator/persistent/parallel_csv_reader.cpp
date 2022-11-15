#include "duckdb/execution/operator/persistent/parallel_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "utf8proc_wrapper.hpp"
#include "utf8proc.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/function/table/read_csv.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>
#include <utility>

namespace duckdb {

ParallelCSVReader::ParallelCSVReader(ClientContext &context, BufferedCSVReaderOptions options_p,
                                     unique_ptr<CSVBufferRead> buffer_p, const vector<LogicalType> &requested_types)
    : BaseCSVReader(context, move(options_p), requested_types) {
	Initialize(requested_types);
	SetBufferRead(move(buffer_p));
	if (options.delimiter.size() > 1 || options.escape.size() > 1 || options.quote.size() > 1) {
		throw InternalException("Parallel CSV reader cannot handle CSVs with multi-byte delimiters/escapes/quotes");
	}
}

ParallelCSVReader::~ParallelCSVReader() {
}

void ParallelCSVReader::Initialize(const vector<LogicalType> &requested_types) {
	sql_types = requested_types;
	InitParseChunk(sql_types.size());
	InitInsertChunkIdx(sql_types.size());
}

bool ParallelCSVReader::SetPosition(DataChunk &insert_chunk) {
	if (buffer->buffer->IsCSVFileFirstBuffer() && start_buffer == position_buffer &&
	    start_buffer == buffer->buffer->GetStart()) {
		verification_positions.beginning_of_first_line = position_buffer;
		verification_positions.end_of_last_line = position_buffer;
		// First buffer doesn't need any setting
		// Unless we have a header
		if (options.header && options.auto_detect) {
			for (; position_buffer < end_buffer; position_buffer++) {
				if (StringUtil::CharacterIsNewline((*buffer)[position_buffer])) {
					position_buffer++;
					return true;
				}
			}
			return false;
		}
		return true;
	}

	// We have to move position up to next new line
	idx_t end_buffer_real = end_buffer;
	// Check if we already start in a valid line
	string error_message;
	bool successfully_read_first_line = false;
	while (!successfully_read_first_line) {
		DataChunk first_line_chunk;
		first_line_chunk.Initialize(allocator, insert_chunk.GetTypes());
		bool carriage_return = false;
		for (; position_buffer < end_buffer; position_buffer++) {
			if (StringUtil::CharacterIsNewline((*buffer)[position_buffer])) {
				carriage_return = (*buffer)[position_buffer] == '\r';
				position_buffer++;
				break;
			}
		}
		if (position_buffer < end_buffer) {
			if (carriage_return && (*buffer)[position_buffer] == '\n') {
				position_buffer++;
				break;
			}
		}
		D_ASSERT(position_buffer <= end_buffer);
		if (position_buffer == end_buffer && !StringUtil::CharacterIsNewline((*buffer)[position_buffer - 1])) {
			break;
		}
		idx_t position_set = position_buffer;
		start_buffer = position_buffer;
		// We check if we can add this line
		successfully_read_first_line = TryParseSimpleCSV(first_line_chunk, error_message, true);
		start_buffer = position_set;
		end_buffer = end_buffer_real;
		position_buffer = position_set;
		if (end_buffer == position_buffer) {
			break;
		}
	}
	verification_positions.beginning_of_first_line = position_buffer;
	verification_positions.end_of_last_line = position_buffer;
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
		buffer_size = buffer_read_p->buffer->GetBufferSize() + buffer_read_p->next_buffer->GetBufferSize();
	} else {
		buffer_size = buffer_read_p->buffer->GetBufferSize();
	}
	linenr = buffer_read_p->estimated_linenr;
	buffer = move(buffer_read_p);

	linenr_estimated = true;
	reached_remainder_state = false;
	verification_positions.beginning_of_first_line = 0;
	verification_positions.end_of_last_line = 0;
	D_ASSERT(end_buffer <= buffer_size);
}

VerificationPositions ParallelCSVReader::GetVerificationPositions() {
	verification_positions.beginning_of_first_line += buffer->buffer->GetCSVGlobalStart();
	verification_positions.end_of_last_line += buffer->buffer->GetCSVGlobalStart();
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

bool ParallelCSVReader::TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message, bool try_add_line) {

	// used for parsing algorithm
	D_ASSERT(end_buffer <= buffer_size);
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	bool has_quotes = false;
	vector<idx_t> escape_positions;
	if (start_buffer == buffer->buffer_start && !try_add_line) {
		// First time reading this buffer piece
		if (!SetPosition(insert_chunk)) {
			// This means the buffer size does not contain a new line
			return true;
		}
	}

	// start parsing the first value
	goto value_start;

value_start : {
	/* state: value_start */
	if (!BufferRemainder()) {
		goto final_state;
	}
	offset = 0;

	// this state parses the first character of a value
	if ((*buffer)[position_buffer] == options.quote[0]) {
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
		if (c == options.delimiter[0]) {
			// delimiter: end the value and add it to the chunk
			goto add_value;
		} else if (StringUtil::CharacterIsNewline(c)) {
			// newline: add row
			if (column > 0 || try_add_line) {
				goto add_row;
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
	AddValue(buffer->GetValue(start_buffer, position_buffer, offset), column, escape_positions, has_quotes);
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

	AddValue(buffer->GetValue(start_buffer, position_buffer, offset), column, escape_positions, has_quotes);
	if (try_add_line) {
		bool success = column == insert_chunk.ColumnCount();
		if (success) {
			AddRow(insert_chunk, column);
			success = Flush(insert_chunk);
		}
		reached_remainder_state = false;
		parse_chunk.Reset();
		return success;
	} else {
		finished_chunk = AddRow(insert_chunk, column);
	}
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	start_buffer = ++position_buffer;
	verification_positions.end_of_last_line = position_buffer;
	if (reached_remainder_state || !BufferRemainder()) {
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
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
		if (c == options.quote[0]) {
			// quote: move to unquoted state
			goto unquote;
		} else if (c == options.escape[0]) {
			// escape: store the escaped position and move to handle_escape state
			escape_positions.push_back(position_buffer - start_buffer);
			goto handle_escape;
		}
	}
	if (!BufferRemainder()) {
		if (buffer->buffer->IsCSVFileLastBuffer()) {
			if (try_add_line) {
				return false;
			}
			// still in quoted state at the end of the file or at the end of a buffer when running multithreaded, error:
			throw InvalidInputException("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
			                            GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
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
	if (c == options.quote[0] && (options.escape.empty() || options.escape[0] == options.quote[0])) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position_buffer - start_buffer);
		goto in_quotes;
	} else if (c == options.delimiter[0]) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (StringUtil::CharacterIsNewline(c)) {
		offset = 1;
		D_ASSERT(column == insert_chunk.ColumnCount() - 1);
		goto add_row;
	} else if (position_buffer >= end_buffer) {
		// reached end of buffer
		offset = 1;
		goto final_state;
	} else {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of "
		    "row or another quote. (%s). ",
		    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
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
	if (position_buffer >= buffer_size && buffer->buffer->IsCSVFileLastBuffer()) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	if ((*buffer)[position_buffer] != options.quote[0] && (*buffer)[position_buffer] != options.escape[0]) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	// escape was followed by quote or escape, go back to quoted state
	goto in_quotes;
}

carriage_return : {
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if ((*buffer)[position_buffer] == '\n') {
		// newline after carriage return: skip
		// increase position by 1 and move start to the new position
		start_buffer = ++position_buffer;
		verification_positions.end_of_last_line = position_buffer;
	}
	if (!BufferRemainder()) {
		goto final_state;
	}
	goto value_start;
}
final_state : {
	/* state: final_stage reached after we finished reading the end_buffer of the csv buffer */
	// reset end buffer
	end_buffer = buffer->buffer_end;
	if (finished_chunk) {
		return true;
	}
	// If this is the last buffer, we have to read the last value
	if (buffer->buffer->IsCSVFileLastBuffer() || (buffer->next_buffer->IsCSVFileLastBuffer())) {
		if (column > 0 || try_add_line) {
			// remaining values to be added to the chunk
			AddValue(buffer->GetValue(start_buffer, position_buffer, offset), column, escape_positions, has_quotes);
			if (try_add_line) {
				bool success = column == sql_types.size();
				if (success) {
					AddRow(insert_chunk, column);
					success = Flush(insert_chunk);
				}
				parse_chunk.Reset();
				reached_remainder_state = false;
				return success;
			} else {
				AddRow(insert_chunk, column);
				verification_positions.end_of_last_line = position_buffer;
			}
		}
	}
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk);
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
