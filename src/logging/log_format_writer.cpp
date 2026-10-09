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

CSVFormatWriter::CSVFormatWriter(WriteStream &stream, LoggingTargetTable table_p, vector<Identifier> column_names_p,
                                 const string &delimiter, CSVNewLineMode newline_mode)
    : table(table_p), column_names(std::move(column_names_p)) {
	writer = make_uniq<CSVWriter>(stream, column_names, false);

	reader_options = CSVReaderOptions();
	reader_options.dialect_options.state_machine_options.escape = '"';
	reader_options.dialect_options.state_machine_options.quote = '"';
	reader_options.dialect_options.state_machine_options.delimiter = CSVOption<string>(delimiter);

	writer_options = make_uniq<CSVWriterOptions>(reader_options);
	writer_options->newline_writing_mode = newline_mode;

	ApplyOptions(reader_options, *writer_options);
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

void CSVFormatWriter::WriteChunk(DataChunk &chunk) {
	ExecuteCast(chunk);
	writer->WriteChunk(*cast_chunk);
	writer->Flush();
	cast_chunk->Reset();
}

void CSVFormatWriter::Truncate() {
	writer->Reset(nullptr);
	writer->options.dialect_options.header = CSVOption<bool>(true);
	writer->Initialize(true);
}

void CSVFormatWriter::Initialize(bool write_header) {
	ApplyOptions(reader_options, *writer_options);
	writer->options.dialect_options.header = {write_header, true};
	writer->Initialize();
	writer->SetWrittenAnything(true);
}

void CSVFormatWriter::ApplyOptions(const CSVReaderOptions &reader_options_p, const CSVWriterOptions &writer_options_p) {
	writer->options = reader_options_p;
	writer->writer_options = writer_options_p;
	writer->options.name_list = column_names;
	writer->options.force_quote = vector<bool>(column_names.size(), false);
}

void CSVFormatWriter::UpdateConfig(const case_insensitive_map_t<Value> &config) {
	for (const auto &it : config) {
		if (StringUtil::Lower(it.first) == "delim") {
			reader_options.dialect_options.state_machine_options.delimiter = CSVOption<string>(it.second.ToString());
		}
	}

	ApplyOptions(reader_options, *writer_options);
}

} // namespace duckdb
