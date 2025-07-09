#include "duckdb/common/csv_utils.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"

#include <unistd.h>

namespace duckdb {

static string TransformNewLine(string new_line) {
	new_line = StringUtil::Replace(new_line, "\\r", "\r");
	return StringUtil::Replace(new_line, "\\n", "\n");
	;
}

CSVWriterLocalState::CSVWriterLocalState() : stream(make_uniq<MemoryStream>()) {
}

CSVWriterLocalState::CSVWriterLocalState(ClientContext &context) : stream(make_uniq<MemoryStream>(Allocator::Get(context))) {
}

CSVWriterLocalState::~CSVWriterLocalState() {
	if (stream) {
		// Ensure we don't accidentially destroy unflushed data
		D_ASSERT(stream->GetPosition() == 0);
	}
}

CSVWriter::CSVWriter(FileSystem &fs, const string &file_path, FileCompressionType compression) :  writer_options({}), written_anything(false) {
	output_file = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileLockType::WRITE_LOCK | compression);

	writer_options.requires_quotes = make_unsafe_uniq_array<bool>(256);
	memset(writer_options.requires_quotes.get(), 0, sizeof(bool) * 256);
	writer_options.requires_quotes['\n'] = true;
	writer_options.requires_quotes['\r'] = true;
	writer_options.requires_quotes[NumericCast<idx_t>(options.dialect_options.state_machine_options.delimiter.GetValue()[0])] = true;
	writer_options.requires_quotes[NumericCast<idx_t>(options.dialect_options.state_machine_options.quote.GetValue())] = true;

	if (!options.write_newline.empty()) {
		writer_options.newline = TransformNewLine(options.write_newline);
	}
}

CSVWriter::CSVWriter(CSVReaderOptions &options_p, FileSystem &fs, const string &file_path, FileCompressionType compression) : options(options_p){
	output_file = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileLockType::WRITE_LOCK | compression);

	writer_options.requires_quotes = make_unsafe_uniq_array<bool>(256);
	memset(writer_options.requires_quotes.get(), 0, sizeof(bool) * 256);
	writer_options.requires_quotes['\n'] = true;
	writer_options.requires_quotes['\r'] = true;
	writer_options.requires_quotes[NumericCast<idx_t>(options.dialect_options.state_machine_options.delimiter.GetValue()[0])] = true;
	writer_options.requires_quotes[NumericCast<idx_t>(options.dialect_options.state_machine_options.quote.GetValue())] = true;

	if (!options.write_newline.empty()) {
		writer_options.newline = TransformNewLine(options.write_newline);
	}
}

void CSVWriter::WriteChunk(DataChunk &input, CSVWriterLocalState &local_state) {
	WriteChunk(input, *local_state.stream, options, written_anything, writer_options);

	if (!local_state.require_manual_flush && local_state.stream->GetPosition() >= writer_options.flush_size) {
		Flush(local_state);
	}
}

void CSVWriter::WriteRawString(const string& raw_string) {
	lock_guard<mutex> flock(lock);
	output_file->Write((void *)raw_string.c_str(), raw_string.size());
}

void CSVWriter::WriteRawString(const string& prefix, CSVWriterLocalState &local_state) {
	local_state.stream->WriteData(const_data_ptr_cast(prefix.c_str()), prefix.size());

	if (!local_state.require_manual_flush && local_state.stream->GetPosition() >= writer_options.flush_size) {
		Flush(local_state);
	}
}

void CSVWriter::WriteHeader() {
	CSVWriterLocalState state;
	WriteHeader(*state.stream, options, writer_options);
	Flush(state);
}

void CSVWriter::Flush(CSVWriterLocalState &local_state) {
	lock_guard<mutex> flock(lock);
	FlushInternal(local_state);
}

void CSVWriter::Close() {
	lock_guard<mutex> flock(lock);
	output_file->Close();
}

// TODO: this no longer writes the newlines on written_anything == false
void CSVWriter::FlushInternal(CSVWriterLocalState &local_state) {
	written_anything = true;
	output_file->Write((void *)local_state.stream->GetData(), local_state.stream->GetPosition());
	local_state.stream->Rewind();
}

unique_ptr<CSVWriterLocalState> CSVWriter::InitializeLocalWriteState(ClientContext &context) {
	auto res = make_uniq<CSVWriterLocalState>();
	res->stream = make_uniq<MemoryStream>();
	return res;
}

idx_t CSVWriter::FileSize() {
	lock_guard<mutex> flock(lock);
	return output_file->GetFileSize();
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

bool CSVWriter::RequiresQuotes(const char *str, idx_t len, vector<string> &null_str,
                              unsafe_unique_array<bool> &requires_quotes) {
	// check if the string is equal to the null string
	if (len == null_str[0].size() && memcmp(str, null_str[0].c_str(), len) == 0) {
		return true;
	}
	auto str_data = reinterpret_cast<const_data_ptr_t>(str);
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

void CSVWriter::WriteQuotedString(WriteStream &writer, const char *str, idx_t len, idx_t col_idx, CSVReaderOptions &options, CSVWriterOptions &writer_options) {
	WriteQuotedString(writer, str, len,  options.force_quote[col_idx], options.null_str, writer_options.requires_quotes, options.dialect_options.state_machine_options.quote.GetValue(), options.dialect_options.state_machine_options.escape.GetValue());
}

void CSVWriter::WriteQuotedString(WriteStream &writer, const char *str, idx_t len, bool force_quote,
                                 vector<string> &null_str, unsafe_unique_array<bool> &requires_quotes, char quote,
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
void CSVWriter::WriteChunk(DataChunk &input, MemoryStream &writer, CSVReaderOptions &options, bool &written_anything, CSVWriterOptions &writer_options) {
	// now loop over the vectors and output the values
	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		if (row_idx == 0 && !written_anything) {
			written_anything = true;
		} else {
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

			WriteQuotedString(writer, str_data[row_idx].GetData(), str_data[row_idx].GetSize(),  col_idx, options, writer_options);
		}
	}
}

void CSVWriter::WriteHeader(MemoryStream &stream, CSVReaderOptions &options, CSVWriterOptions &writer_options) {
	for (idx_t i = 0; i < options.name_list.size(); i++) {
		if (i != 0) {
			WriteQuoteOrEscape(stream,
										 options.dialect_options.state_machine_options.delimiter.GetValue()[0]);
		}

		WriteQuotedString(stream, options.name_list[i].c_str(), options.name_list[i].size(), i, options, writer_options);
	}

	// TODO: why was this done before?
	// stream.WriteData(const_data_ptr_cast(writer_options.newline.c_str()), writer_options.newline.size());
}

} // namespace duckdb
