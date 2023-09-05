#include "duckdb/execution/operator/scan/csv/parallel_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"

#include <algorithm>


namespace duckdb {

ParallelCSVReader::ParallelCSVReader(ClientContext &context, CSVReaderOptions options_p,
                                     const vector<LogicalType> &requested_types, idx_t file_idx_p, unique_ptr<CSVScanner> scanner_p)
    : BaseCSVReader(context, std::move(options_p)), file_idx(file_idx_p), scanner(std::move(scanner_p)){
	return_types = requested_types;
	InitParseChunk(return_types.size());
}

//bool ParallelCSVReader::NewLineDelimiter(bool carry, bool carry_followed_by_nl, bool first_char) {
//	// Set the delimiter if not set yet.
//	SetNewLineDelimiter(carry, carry_followed_by_nl);
//	D_ASSERT(options.dialect_options.new_line == NewLineIdentifier::SINGLE ||
//	         options.dialect_options.new_line == NewLineIdentifier::CARRY_ON);
//	if (options.dialect_options.new_line == NewLineIdentifier::SINGLE) {
//		return (!carry) || (carry && !carry_followed_by_nl);
//	}
//	return (carry && carry_followed_by_nl) || (!carry && first_char);
//}

VerificationPositions ParallelCSVReader::GetVerificationPositions() {
//	verification_positions.beginning_of_first_line += buffer->buffer->csv_global_start;
//	verification_positions.end_of_last_line += buffer->buffer->csv_global_start;
	return verification_positions;
}

bool ParallelCSVReader::Parse(DataChunk &insert_chunk, string &error_message, bool try_add_line) {

	// If line is not set, we have to figure it out, we assume whatever is in the first line
//	if (options.dialect_options.new_line == NewLineIdentifier::NOT_SET) {
//		idx_t cur_pos = position_buffer;
//		// we can start in the middle of a new line, so move a bit forward.
//		while (cur_pos < end_buffer) {
//			if (StringUtil::CharacterIsNewline((*buffer)[cur_pos])) {
//				cur_pos++;
//			} else {
//				break;
//			}
//		}
//		for (; cur_pos < end_buffer; cur_pos++) {
//			if (StringUtil::CharacterIsNewline((*buffer)[cur_pos])) {
//				bool carriage_return = (*buffer)[cur_pos] == '\r';
//				bool carriage_return_followed = false;
//				cur_pos++;
//				if (cur_pos < end_buffer) {
//					if (carriage_return && (*buffer)[cur_pos] == '\n') {
//						carriage_return_followed = true;
//						cur_pos++;
//					}
//				}
//				SetNewLineDelimiter(carriage_return, carriage_return_followed);
//				break;
//			}
//		}
//	}
	D_ASSERT(options.dialect_options.new_line != NewLineIdentifier::NOT_SET);
	scanner->Parse(parse_chunk,verification_positions,return_types);

	// flush the parsed chunk and finalize parsing
	Flush(insert_chunk, scanner->GetBufferIndex());
//	buffer->lines_read += insert_chunk.size();

//	if (position_buffer - verification_positions.end_of_last_line > options.buffer_size) {
//		error_message = "Line does not fit in one buffer. Increase the buffer size.";
//		return false;
//	}

	return true;

}

void ParallelCSVReader::ParseCSV(DataChunk &insert_chunk) {
	string error_message;
	if (!Parse(insert_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

idx_t ParallelCSVReader::GetLineError(idx_t line_error, idx_t buffer_idx, bool stop_at_first) {
//	while (true) {
//		if (buffer->line_info->CanItGetLine(file_idx, buffer_idx)) {
//			auto cur_start = verification_positions.beginning_of_first_line + buffer->buffer->csv_global_start;
//			return buffer->line_info->GetLine(buffer_idx, line_error, file_idx, cur_start, false, stop_at_first);
//		}
//	}
}


} // namespace duckdb
