#include "duckdb/execution/operator/csv_scanner/encode/csv_decoder.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void CSVEncoderBuffer::Initialize(idx_t encoded_size) {
	encoded_buffer_size = encoded_size;
	encoded_buffer = std::unique_ptr<char[]>(new char[encoded_size]);
}

char *CSVEncoderBuffer::Ptr() const {
	return encoded_buffer.get();
}

idx_t CSVEncoderBuffer::GetCapacity() const {
	return encoded_buffer_size;
}

idx_t CSVEncoderBuffer::GetSize() const {
	return actual_encoded_buffer_size;
}

void CSVEncoderBuffer::SetSize(const idx_t buffer_size) {
	D_ASSERT(buffer_size <= encoded_buffer_size);
	actual_encoded_buffer_size = buffer_size;
}

bool CSVEncoderBuffer::HasDataToRead() const {
	return cur_pos < actual_encoded_buffer_size;
}

void CSVEncoderBuffer::Reset() {
	cur_pos = 0;
	actual_encoded_buffer_size = 0;
}

// void CSVReaderOptions::SetEncoding(const string &encoding_value) {
// 	auto encoding_string = StringUtil::Lower(encoding_value);
// 	if (encoding_value == "utf-8" || encoding_value == "utf8") {
// 		encoding = CSVEncoding::UTF_8;
// 	} else if (encoding_value == "utf-16" || encoding_value == "utf16") {
// 		encoding = CSVEncoding::UTF_16;
// 	} else if (encoding_value == "latin-1" || encoding_value == "latin1") {
// 		encoding = CSVEncoding::LATIN_1;
// 	} else {
// 		std::ostringstream error;
// 		error << "The CSV Reader does not support the encoding: \"" << encoding_value << "\"\n";
// 		error << "The currently supported encodings are: " << '\n';
// 		error << "* utf-8 " << '\n';
// 		error << "* utf-16 " << '\n';
// 		error << "* latin-1 " << '\n';
// 		throw InvalidInputException(error.str());
// 	}
// }

CSVDecoder::CSVDecoder(DBConfig &config, const string &enconding_name_p, idx_t buffer_size) {
	encoding_name = StringUtil::Lower(enconding_name_p);
	auto function = config.GetDecodeFunction(encoding_name);
	if (!function) {
		throw std::runtime_error("CSVDecoder: Could not find decode function");
	}
	// Let's enforce that the encoded buffer size is divisible by 2, makes life easier.
	idx_t encoded_buffer_size = buffer_size / function->ratio;
	if (encoded_buffer_size % 2 != 0) {
		encoded_buffer_size = encoded_buffer_size - 1;
	}
	encoded_buffer.Initialize(encoded_buffer_size);
	remaining_bytes_buffer.Initialize(function->bytes_per_iteration);
	decoding_function = function;
	D_ASSERT(encoded_buffer_size > 0);
}

idx_t CSVDecoder::Decode(FileHandle &file_handle_input, char *output_buffer, const idx_t nr_bytes_to_read) {
	idx_t output_buffer_pos = 0;
	// Check if we have some left-overs. These can either be
	// 1. missing decoded bytes
	if (remaining_bytes_buffer.HasDataToRead()) {
		D_ASSERT(remaining_bytes_buffer.cur_pos == 0);
		const auto remaining_bytes_buffer_ptr = remaining_bytes_buffer.Ptr();
		for (; remaining_bytes_buffer.cur_pos < remaining_bytes_buffer.GetSize(); remaining_bytes_buffer.cur_pos++) {
			output_buffer[output_buffer_pos++] = remaining_bytes_buffer_ptr[remaining_bytes_buffer.cur_pos];
		}
		remaining_bytes_buffer.Reset();
	}
	// 2. remaining encoded buffer
	if (encoded_buffer.HasDataToRead()) {
		decoding_function->decode_function(
		    encoded_buffer.Ptr(), encoded_buffer.cur_pos, encoded_buffer.GetSize(), output_buffer, output_buffer_pos,
		    nr_bytes_to_read, remaining_bytes_buffer.Ptr(), remaining_bytes_buffer.actual_encoded_buffer_size);
	}
	// Otherwise we read a new encoded buffer from the file
	while (output_buffer_pos < nr_bytes_to_read) {
		idx_t current_decoded_buffer_start = output_buffer_pos;
		encoded_buffer.Reset();
		auto actual_encoded_bytes =
		    static_cast<idx_t>(file_handle_input.Read(encoded_buffer.Ptr(), encoded_buffer.GetCapacity()));
		encoded_buffer.SetSize(actual_encoded_bytes);
		decoding_function->decode_function(
		    encoded_buffer.Ptr(), encoded_buffer.cur_pos, encoded_buffer.GetSize(), output_buffer, output_buffer_pos,
		    nr_bytes_to_read, remaining_bytes_buffer.Ptr(), remaining_bytes_buffer.actual_encoded_buffer_size);
		if (output_buffer_pos == current_decoded_buffer_start) {
			return output_buffer_pos;
		}
	}
	return output_buffer_pos;
}

} // namespace duckdb
