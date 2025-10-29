#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"

namespace duckdb {

static string TransformNewLine(string new_line) {
	new_line = StringUtil::Replace(new_line, "\\r", "\r");
	return StringUtil::Replace(new_line, "\\n", "\n");
}

CSVWriterState::CSVWriterState()
    : flush_size(MemoryStream::DEFAULT_INITIAL_CAPACITY), stream(make_uniq<MemoryStream>()) {
}

CSVWriterState::CSVWriterState(ClientContext &context, idx_t flush_size_p)
    : flush_size(flush_size_p), stream(make_uniq<MemoryStream>(Allocator::Get(context), flush_size)) {
}

CSVWriterState::CSVWriterState(DatabaseInstance &db, idx_t flush_size_p)
    : flush_size(flush_size_p), stream(make_uniq<MemoryStream>(BufferAllocator::Get(db), flush_size)) {
}

CSVWriterState::~CSVWriterState() {
	if (stream && !Exception::UncaughtException()) {
		// Ensure we don't accidentally destroy unflushed data
		D_ASSERT(stream->GetPosition() == 0);
	}
}

CSVWriterOptions::CSVWriterOptions(const string &delim, const char &quote, const string &write_newline) {
	requires_quotes = vector<bool>(256, false);
	requires_quotes['\n'] = true;
	requires_quotes['\r'] = true;
	requires_quotes['#'] = true;
	requires_quotes[NumericCast<idx_t>(delim[0])] = true;
	requires_quotes[NumericCast<idx_t>(quote)] = true;

	if (!write_newline.empty()) {
		newline = TransformNewLine(write_newline);
	}
}

CSVWriterOptions::CSVWriterOptions(CSVReaderOptions &options)
    : CSVWriterOptions(options.dialect_options.state_machine_options.delimiter.GetValue(),
                       options.dialect_options.state_machine_options.quote.GetValue(), options.write_newline) {
}

CSVWriter::CSVWriter(WriteStream &stream, vector<string> name_list, bool shared)
    : writer_options(options.dialect_options.state_machine_options.delimiter.GetValue(),
                     options.dialect_options.state_machine_options.quote.GetValue(), options.write_newline),
      write_stream(stream), should_initialize(true), shared(shared) {
	auto size = name_list.size();
	options.name_list = std::move(name_list);
	options.force_quote.resize(size, false);
	options.force_quote.resize(size, false);

	if (!shared) {
		global_write_state = make_uniq<CSVWriterState>();
	}
}

CSVWriter::CSVWriter(CSVReaderOptions &options_p, FileSystem &fs, const string &file_path,
                     FileCompressionType compression, bool shared)
    : options(options_p),
      writer_options(options.dialect_options.state_machine_options.delimiter.GetValue(),
                     options.dialect_options.state_machine_options.quote.GetValue(), options.write_newline),
      file_writer(make_uniq<BufferedFileWriter>(fs, file_path,
                                                FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
                                                    FileLockType::WRITE_LOCK | compression)),
      write_stream(*file_writer), should_initialize(true), shared(shared) {
	if (!shared) {
		global_write_state = make_uniq<CSVWriterState>();
	}
}

void CSVWriter::Initialize(bool force) {
	if (!force && !should_initialize) {
		return;
	}

	if (!options.prefix.empty()) {
		WriteRawString(options.prefix);
	}

	if (!(options.dialect_options.header.IsSetByUser() && !options.dialect_options.header.GetValue())) {
		WriteHeader();
	}

	should_initialize = false;
}

void CSVWriter::WriteChunk(DataChunk &input, CSVWriterState &local_state) {
	WriteChunk(input, *local_state.stream, options, local_state.written_anything, writer_options);

	if (!local_state.require_manual_flush && local_state.stream->GetPosition() >= local_state.flush_size) {
		Flush(local_state);
	}
}

void CSVWriter::WriteChunk(DataChunk &input) {
	// Method intended for non-shared use only
	D_ASSERT(!shared);

	WriteChunk(input, *global_write_state);
}

void CSVWriter::WriteRawString(const string &raw_string) {
	if (shared) {
		lock_guard<mutex> flock(lock);
		bytes_written += raw_string.size();
		write_stream.WriteData(const_data_ptr_cast(raw_string.c_str()), raw_string.size());
	} else {
		bytes_written += raw_string.size();
		write_stream.WriteData(const_data_ptr_cast(raw_string.c_str()), raw_string.size());
	}
}

void CSVWriter::WriteRawString(const string &prefix, CSVWriterState &local_state) {
	local_state.stream->WriteData(const_data_ptr_cast(prefix.c_str()), prefix.size());

	if (!local_state.require_manual_flush && local_state.stream->GetPosition() >= writer_options.flush_size) {
		Flush(local_state);
	}
}

void CSVWriter::WriteHeader() {
	CSVWriterState state;
	WriteHeader(*state.stream, options, writer_options);
	state.written_anything = true;
	Flush(state);
}

void CSVWriter::Flush(CSVWriterState &local_state) {
	if (shared) {
		lock_guard<mutex> flock(lock);
		FlushInternal(local_state);
	} else {
		FlushInternal(local_state);
	}
}

void CSVWriter::Flush() {
	// Method intended for non-shared use only
	D_ASSERT(!shared);
	FlushInternal(*global_write_state);
}

void CSVWriter::Reset(optional_ptr<CSVWriterState> local_state) {
	if (shared) {
		lock_guard<mutex> flock(lock);
		ResetInternal(local_state);
	} else {
		ResetInternal(local_state);
	}
}

void CSVWriter::Close() {
	if (shared) {
		lock_guard<mutex> flock(lock);
		if (file_writer) {
			file_writer->Close();
		}
	} else {
		if (file_writer) {
			file_writer->Close();
		}
	}
}

void CSVWriter::FlushInternal(CSVWriterState &local_state) {
	if (!local_state.written_anything) {
		return;
	}

	if (!written_anything) {
		written_anything = true;
	} else if (writer_options.newline_writing_mode == CSVNewLineMode::WRITE_BEFORE) {
		write_stream.WriteData(const_data_ptr_cast(writer_options.newline.c_str()), writer_options.newline.size());
	}

	written_anything = true;
	bytes_written += local_state.stream->GetPosition();
	write_stream.WriteData(local_state.stream->GetData(), local_state.stream->GetPosition());

	local_state.Reset();
}

void CSVWriter::ResetInternal(optional_ptr<CSVWriterState> local_state) {
	if (local_state) {
		local_state->Reset();
	}

	written_anything = false;
	bytes_written = 0;
}

idx_t CSVWriter::BytesWritten() {
	if (shared) {
		lock_guard<mutex> flock(lock);
		return bytes_written;
	}
	return bytes_written;
}

static idx_t GetFileSize(unique_ptr<BufferedFileWriter> &file_writer, idx_t &bytes_written) {
	if (file_writer) {
		return file_writer->GetFileSize();
	}
	return bytes_written;
}

idx_t CSVWriter::FileSize() {
	if (shared) {
		lock_guard<mutex> flock(lock);
		return GetFileSize(file_writer, bytes_written);
	}
	return GetFileSize(file_writer, bytes_written);
}

void CSVWriter::WriteQuoteOrEscape(WriteStream &writer, char quote_or_escape) {
	if (quote_or_escape != '\0') {
		writer.Write(quote_or_escape);
	}
}

string CSVWriter::AddEscapes(char to_be_escaped, char escape, const string &val) {
	idx_t i = 0;
	string new_val = "";
	idx_t found = val.find(to_be_escaped);

	while (found != string::npos) {
		while (i < found) {
			new_val += val[i];
			i++;
		}
		if (escape != '\0') {
			new_val += escape;
			found = val.find(to_be_escaped, found + 1);
		}
	}
	while (i < val.length()) {
		new_val += val[i];
		i++;
	}
	return new_val;
}

bool CSVWriter::RequiresQuotes(const char *str, idx_t len, const string &null_str,
                               const vector<bool> &requires_quotes) {
	// check if the string is equal to the null string
	if (len == null_str.size() && memcmp(str, null_str.c_str(), len) == 0) {
		return true;
	}
	auto str_data = const_data_ptr_cast(str);
	for (idx_t i = 0; i < len; i++) {
		if (requires_quotes[str_data[i]]) {
			// this byte requires quotes - write a quoted string
			return true;
		}
	}
	// no newline, quote or delimiter in the string
	// no quoting or escaping necessary
	return false;
}

void CSVWriter::WriteQuotedString(WriteStream &writer, const char *str, idx_t len, idx_t col_idx,
                                  CSVReaderOptions &options, CSVWriterOptions &writer_options) {
	WriteQuotedString(writer, str, len, options.force_quote[col_idx], options.null_str[0],
	                  writer_options.requires_quotes, options.dialect_options.state_machine_options.quote.GetValue(),
	                  options.dialect_options.state_machine_options.escape.GetValue());
}

void CSVWriter::WriteQuotedString(WriteStream &writer, const char *str, idx_t len, bool force_quote,
                                  const string &null_str, const vector<bool> &requires_quotes, char quote,
                                  char escape) {
	if (!force_quote) {
		// force quote is disabled: check if we need to add quotes anyway
		force_quote = RequiresQuotes(str, len, null_str, requires_quotes);
	}
	// If a quote is set to none (i.e., null-terminator) we skip the quotation
	if (force_quote && quote != '\0') {
		// quoting is enabled: we might need to escape things in the string
		bool requires_escape = false;
		// simple CSV
		// do a single loop to check for a quote or escape value
		for (idx_t i = 0; i < len; i++) {
			if (str[i] == quote || str[i] == escape) {
				requires_escape = true;
				break;
			}
		}

		if (!requires_escape) {
			// fast path: no need to escape anything
			WriteQuoteOrEscape(writer, quote);
			writer.WriteData(const_data_ptr_cast(str), len);
			WriteQuoteOrEscape(writer, quote);
			return;
		}

		// slow path: need to add escapes
		string new_val(str, len);
		new_val = AddEscapes(escape, escape, new_val);
		if (escape != quote) {
			// need to escape quotes separately
			new_val = AddEscapes(quote, escape, new_val);
		}
		WriteQuoteOrEscape(writer, quote);
		writer.WriteData(const_data_ptr_cast(new_val.c_str()), new_val.size());
		WriteQuoteOrEscape(writer, quote);
	} else {
		writer.WriteData(const_data_ptr_cast(str), len);
	}
}

// Write a chunk to a csv file
void CSVWriter::WriteChunk(DataChunk &input, MemoryStream &writer, CSVReaderOptions &options, bool &written_anything,
                           CSVWriterOptions &writer_options) {
	// now loop over the vectors and output the values
	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		if (row_idx == 0 && !written_anything) {
			written_anything = true;
		} else if (writer_options.newline_writing_mode == CSVNewLineMode::WRITE_BEFORE) {
			writer.WriteData(const_data_ptr_cast(writer_options.newline.c_str()), writer_options.newline.size());
		}
		// write values
		D_ASSERT(options.null_str.size() == 1);
		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			if (col_idx != 0) {
				CSVWriter::WriteQuoteOrEscape(writer,
				                              options.dialect_options.state_machine_options.delimiter.GetValue()[0]);
			}
			if (FlatVector::IsNull(input.data[col_idx], row_idx)) {
				// write null value
				writer.WriteData(const_data_ptr_cast(options.null_str[0].c_str()), options.null_str[0].size());
				continue;
			}

			// non-null value, fetch the string value from the cast chunk
			auto str_data = FlatVector::GetData<string_t>(input.data[col_idx]);
			// FIXME: we could gain some performance here by checking for certain types if they ever require quotes
			// (e.g. integers only require quotes if the delimiter is a number, decimals only require quotes if the
			// delimiter is a number or "." character)

			WriteQuotedString(writer, str_data[row_idx].GetData(), str_data[row_idx].GetSize(), col_idx, options,
			                  writer_options);
		}
		if (writer_options.newline_writing_mode == CSVNewLineMode::WRITE_AFTER) {
			writer.WriteData(const_data_ptr_cast(writer_options.newline.c_str()), writer_options.newline.size());
		}
	}
}

void CSVWriter::WriteHeader(MemoryStream &stream, CSVReaderOptions &options, CSVWriterOptions &writer_options) {
	for (idx_t i = 0; i < options.name_list.size(); i++) {
		if (i != 0) {
			WriteQuoteOrEscape(stream, options.dialect_options.state_machine_options.delimiter.GetValue()[0]);
		}

		WriteQuotedString(stream, options.name_list[i].c_str(), options.name_list[i].size(), i, options,
		                  writer_options);
	}
}

} // namespace duckdb
