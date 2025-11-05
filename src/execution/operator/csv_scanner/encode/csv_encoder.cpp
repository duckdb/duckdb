#include "duckdb/execution/operator/csv_scanner/encode/csv_encoder.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/encoding_function.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

void CSVEncoderBuffer::Initialize(idx_t encoded_size) {
	encoded_buffer_size = encoded_size;
	encoded_buffer = duckdb::unique_ptr<char[]>(new char[encoded_size]);
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

CSVEncoder::CSVEncoder(ClientContext &context_p, const string &encoding_name_to_find, idx_t buffer_size)
    : context(context_p), pass_on_byte(0) {
	auto &config = DBConfig::GetConfig(context_p);
	encoding_name = StringUtil::Lower(encoding_name_to_find);
	auto function = config.GetEncodeFunction(encoding_name_to_find);
	if (!function) {
		// Maybe we can try to auto-load from our encodings extension, if this somehow fails, we just error.
		if (Catalog::TryAutoLoad(context_p, "encodings")) {
			// If it successfully loaded, we can try to get our function again
			function = config.GetEncodeFunction(encoding_name_to_find);
		}
	}
	if (!function) {
		// If at this point we still do not have a function we throw an error.
		auto loaded_encodings = config.GetLoadedEncodedFunctions();
		std::ostringstream error;
		error << "The CSV Reader does not support the encoding: \"" << encoding_name_to_find << "\"\n";
		if (!context_p.db->ExtensionIsLoaded("encodings")) {
			error << "It is possible that the encoding exists in the encodings extension. You can try \"INSTALL "
			         "encodings; LOAD encodings\""
			      << "\n";
		}
		error << "The currently supported encodings are: " << '\n';
		for (auto &encoding_function : loaded_encodings) {
			error << "*  " << encoding_function.get().GetName() << '\n';
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
		encoding_function->GetFunction()(encoded_buffer, output_buffer, output_buffer_pos, decoded_buffer_size,
		                                 remaining_bytes_buffer.Ptr(),
		                                 remaining_bytes_buffer.actual_encoded_buffer_size, encoding_function.get());
	}
	// 3. a new encoded buffer from the file
	while (output_buffer_pos < decoded_buffer_size) {
		idx_t current_decoded_buffer_start = output_buffer_pos;
		vector<char> pass_on_buffer;
		if (encoded_buffer.cur_pos != encoded_buffer.GetSize() &&
		    encoding_function->GetLookupBytes() > encoded_buffer.GetSize() - encoded_buffer.cur_pos) {
			// We don't have enough lookup bytes for it, if that's the case, we need to set off these bytes to the next
			// buffer
			for (idx_t i = encoded_buffer.GetSize() - encoded_buffer.cur_pos; i < encoding_function->GetLookupBytes();
			     i++) {
				pass_on_buffer.push_back(encoded_buffer.Ptr()[i]);
			}
		}
		encoded_buffer.Reset();
		for (idx_t i = 0; i < pass_on_buffer.size(); i++) {
			encoded_buffer.Ptr()[i] = pass_on_buffer[i];
		}
		if (has_pass_on_byte) {
			encoded_buffer.Ptr()[pass_on_buffer.size()] = pass_on_byte;
		}
		auto actual_encoded_bytes = static_cast<idx_t>(
		    file_handle_input.Read(context, encoded_buffer.Ptr() + pass_on_buffer.size() + has_pass_on_byte,
		                           encoded_buffer.GetCapacity() - pass_on_buffer.size() - has_pass_on_byte));
		encoded_buffer.SetSize(actual_encoded_bytes + pass_on_buffer.size() + has_pass_on_byte);
		if (actual_encoded_bytes < encoded_buffer.GetCapacity() - pass_on_buffer.size()) {
			encoded_buffer.last_buffer = true;
			has_pass_on_byte = false;
		} else {
			auto bytes_read = static_cast<idx_t>(file_handle_input.Read(context, &pass_on_byte, 1));
			if (bytes_read == 0) {
				encoded_buffer.last_buffer = true;
				has_pass_on_byte = false;
			} else {
				has_pass_on_byte = true;
			}
		}
		encoding_function->GetFunction()(encoded_buffer, output_buffer, output_buffer_pos, decoded_buffer_size,
		                                 remaining_bytes_buffer.Ptr(),
		                                 remaining_bytes_buffer.actual_encoded_buffer_size, encoding_function.get());
		if (output_buffer_pos == current_decoded_buffer_start) {
			return output_buffer_pos;
		}
	}
	return output_buffer_pos;
}

} // namespace duckdb
