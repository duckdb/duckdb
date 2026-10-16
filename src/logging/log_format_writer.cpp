#include "duckdb/logging/log_format_writer.hpp"
#include "duckdb/logging/log_sink.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

CSVFormatWriter::CSVFormatWriter(LoggingTargetTable table_p, const string &delimiter, CSVNewLineMode newline_mode)
    : table(table_p), column_names(LogSink::GetColumnNames(table_p)) {
	reader_options.dialect_options.state_machine_options.escape = '"';
	reader_options.dialect_options.state_machine_options.quote = '"';
	writer_options = make_uniq<CSVWriterOptions>(reader_options);
	writer_options->newline_writing_mode = newline_mode;
	SetDelimiter(delimiter);
	ResetCastChunk(STANDARD_VECTOR_SIZE);
}

void CSVFormatWriter::ResetCastChunk(idx_t capacity) {
	cast_chunk = make_uniq<DataChunk>();
	auto schema = LogSink::GetSchema(table);
	vector<LogicalType> types;
	types.resize(schema.size(), LogicalType::VARCHAR);
	cast_chunk->Initialize(Allocator::DefaultAllocator(), types, capacity);
	cast_chunk_capacity = capacity;
}

void CSVFormatWriter::ExecuteCast(DataChunk &chunk) {
	auto count = chunk.size();
	// sinks flush chunks of up to their buffer size, which can be larger than a vector
	if (count > cast_chunk_capacity) {
		ResetCastChunk(count);
	}
	cast_chunk->Reset();
	for (idx_t i = 0; i < chunk.data.size(); i++) {
		VectorOperations::DefaultCast(chunk.data[i], cast_chunk->data[i], count, false);
	}
	cast_chunk->CheckCardinality(count);
}

void CSVFormatWriter::Initialize(WriteStream &stream, bool write_header) {
	writer = make_uniq<CSVWriter>(stream, column_names, false);
	ApplyOptions();
	writer->options.dialect_options.header = {write_header, true};
	writer->Initialize();
	writer->SetWrittenAnything(true);
}

void CSVFormatWriter::WriteChunk(DataChunk &chunk) {
	D_ASSERT(writer);
	ExecuteCast(chunk);
	writer->WriteChunk(*cast_chunk);
	writer->Flush();
	cast_chunk->Reset();
}

void CSVFormatWriter::Reset() {
	D_ASSERT(writer);
	writer->Reset(nullptr);
	writer->options.dialect_options.header = CSVOption<bool>(true);
	writer->Initialize(true);
}

void CSVFormatWriter::SetDelimiter(const string &delimiter) {
	if (delimiter.empty()) {
		throw InvalidInputException("The 'delim' option of a CSV log sink cannot be empty");
	}
	reader_options.dialect_options.state_machine_options.delimiter = CSVOption<string>(delimiter);
	// the quoting rules depend on the delimiter, so the writer options are derived again
	auto newline_mode = writer_options->newline_writing_mode;
	writer_options = make_uniq<CSVWriterOptions>(reader_options);
	writer_options->newline_writing_mode = newline_mode;
	ApplyOptions();
}

void CSVFormatWriter::ApplyOptions() {
	if (!writer) {
		return;
	}
	auto header = writer->options.dialect_options.header;
	writer->options = reader_options;
	writer->options.dialect_options.header = header;
	writer->writer_options = *writer_options;
	writer->options.name_list = column_names;
	writer->options.force_quote = vector<bool>(column_names.size(), false);
}

bool CSVFormatWriter::UpdateConfig(case_insensitive_map_t<Value> &config) {
	auto entry = config.find("delim");
	if (entry == config.end()) {
		return false;
	}
	SetDelimiter(entry->second.ToString());
	config.erase(entry);
	return true;
}

} // namespace duckdb
