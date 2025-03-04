#include "duckdb/execution/operator/csv_scanner/encode/csv_encoder.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/encoding_function.hpp"

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

CSVEncoder::CSVEncoder(const DBConfig &config, const string &encoding_name_to_find, idx_t buffer_size) {
	encoding_name = StringUtil::Lower(encoding_name_to_find);
	auto function = config.GetEncodeFunction(encoding_name_to_find);
	if (!function) {
		auto loaded_encodings = config.GetLoadedEncodedFunctions();
		std::ostringstream error;
		error << "The CSV Reader does not support the encoding: \"" << encoding_name_to_find << "\"\n";
		error << "The currently supported encodings are: " << '\n';
		for (auto &encoding_function : loaded_encodings) {
			error << "*  " << encoding_function.get().GetType() << '\n';
		}
		throw InvalidInputException(error.str());
	}
	// We ensure that the encoded buffer size is an even number to make the two byte lookup on utf-16 work
	idx_t encoded_buffer_size = buffer_size % 2 != 0 ? buffer_size - 1 : buffer_size;
	if (encoded_buffer_size == 0) {
		// This might happen if buffer size = 1
		encoded_buffer_size = 2;
	}
	D_ASSERT(encoded_buffer_size > 0);
	encoded_buffer.Initialize(encoded_buffer_size);
	remaining_bytes_buffer.Initialize(function->GetBytesPerIteration());
	encoding_function = function;
}

idx_t CSVEncoder::Encode(FileHandle &file_handle_input, char *output_buffer, const idx_t decoded_buffer_size) {
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
		encoding_function->GetFunction()(
		    encoded_buffer.Ptr(), encoded_buffer.cur_pos, encoded_buffer.GetSize(), output_buffer, output_buffer_pos,
		    decoded_buffer_size, remaining_bytes_buffer.Ptr(), remaining_bytes_buffer.actual_encoded_buffer_size);
	}
	// 3. a new encoded buffer from the file
	while (output_buffer_pos < decoded_buffer_size) {
		idx_t current_decoded_buffer_start = output_buffer_pos;
		encoded_buffer.Reset();
		auto actual_encoded_bytes =
		    static_cast<idx_t>(file_handle_input.Read(encoded_buffer.Ptr(), encoded_buffer.GetCapacity()));
		encoded_buffer.SetSize(actual_encoded_bytes);
		encoding_function->GetFunction()(
		    encoded_buffer.Ptr(), encoded_buffer.cur_pos, encoded_buffer.GetSize(), output_buffer, output_buffer_pos,
		    decoded_buffer_size, remaining_bytes_buffer.Ptr(), remaining_bytes_buffer.actual_encoded_buffer_size);
		if (output_buffer_pos == current_decoded_buffer_start) {
			return output_buffer_pos;
		}
	}
	return output_buffer_pos;
}

} // namespace duckdb
