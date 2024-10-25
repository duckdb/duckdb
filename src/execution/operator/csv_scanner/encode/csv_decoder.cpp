#include "duckdb/execution/operator/csv_scanner/encode/csv_decoder.hpp"
#include "duckdb/common/exception.hpp"

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

CSVDecoder::CSVDecoder(const CSVEncoding encoding_p, idx_t buffer_size) : encoding(encoding_p) {
	// Let's enforce that the encoded buffer size is divisible by 2, makes life easier.
	idx_t encoded_buffer_size = buffer_size / GetRatio();
	if (encoded_buffer_size % 2 != 0) {
		encoded_buffer_size = encoded_buffer_size - 1;
	}
	encoded_buffer.Initialize(encoded_buffer_size);
	remaining_bytes_buffer.Initialize(MaxDecodedBytesPerIteration());
	D_ASSERT(encoded_buffer_size > 0);
}

bool CSVDecoder::IsUTF8() const {
	return encoding == CSVEncoding::UTF_8;
}

idx_t CSVDecoder::GetRatio() const {
	switch (encoding) {
	case CSVEncoding::UTF_8:
		return 1;
	case CSVEncoding::UTF_16:
	case CSVEncoding::LATIN_1:
		return 2;
	default:
		throw NotImplementedException("GetRatio() is not implemented for given CSVEncoding type");
	}
}

idx_t CSVDecoder::MaxDecodedBytesPerIteration() const {
	switch (encoding) {
	case CSVEncoding::UTF_8:
		return 0;
	case CSVEncoding::LATIN_1:
		return 1;
	case CSVEncoding::UTF_16:
		return 2;
	default:
		throw NotImplementedException("MaxDecodedBytesPerIteration() is not implemented for given CSVEncoding type");
	}
}

void CSVDecoder::DecodeUTF16(char *decoded_buffer, idx_t &decoded_buffer_start, const idx_t decoded_buffer_size) {
	const auto encoded_buffer_ptr = encoded_buffer.Ptr();

	for (; encoded_buffer.cur_pos < encoded_buffer.GetSize(); encoded_buffer.cur_pos += 2) {
		if (decoded_buffer_start == decoded_buffer_size) {
			// We are done
			return;
		}
		const uint16_t ch =
		    static_cast<uint16_t>(static_cast<unsigned char>(encoded_buffer_ptr[encoded_buffer.cur_pos]) |
		                          (static_cast<unsigned char>(encoded_buffer_ptr[encoded_buffer.cur_pos + 1]) << 8));
		if (ch >= 0xD800 && ch <= 0xDFFF) {
			throw InvalidInputException("CSV File is not utf-16 encoded");
		}
		if (ch <= 0x007F) {
			// 1-byte UTF-8 for ASCII characters
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(ch & 0x7F);
		} else if (ch <= 0x07FF) {
			// 2-byte UTF-8
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0xC0 | (ch >> 6));
			if (decoded_buffer_start == decoded_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				encoded_buffer.cur_pos += 2;
				remaining_bytes_buffer.Ptr()[0] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_buffer.SetSize(1);
				return;
			}
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0x80 | (ch & 0x3F));
		} else {
			// 3-byte UTF-8
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0xE0 | (ch >> 12));
			if (decoded_buffer_start == decoded_buffer_size) {
				// We are done, but we have to store two bytes for the next chunk!
				encoded_buffer.cur_pos += 2;
				remaining_bytes_buffer.Ptr()[0] = static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
				remaining_bytes_buffer.Ptr()[1] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_buffer.SetSize(2);
				return;
			}
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
			if (decoded_buffer_start == decoded_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				encoded_buffer.cur_pos += 2;
				remaining_bytes_buffer.Ptr()[0] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_buffer.SetSize(1);
				return;
			}
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0x80 | (ch & 0x3F));
		}
	}
}

void CSVDecoder::DecodeLatin1(char *decoded_buffer, idx_t &decoded_buffer_start, const idx_t decoded_buffer_size) {
	const auto encoded_buffer_ptr = encoded_buffer.Ptr();
	for (; encoded_buffer.cur_pos < encoded_buffer.GetSize(); encoded_buffer.cur_pos++) {
		if (decoded_buffer_start == decoded_buffer_size) {
			// We are done
			return;
		}
		const unsigned char ch = static_cast<unsigned char>(encoded_buffer_ptr[encoded_buffer.cur_pos]);
		if (ch > 0x7F && ch <= 0x9F) {
			throw InvalidInputException("CSV File is not latin-1 encoded");
		}
		if (ch <= 0x7F) {
			// ASCII: 1 byte in UTF-8
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(ch);
		} else {
			// Non-ASCII: 2 bytes in UTF-8
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0xc2 + (ch > 0xbf));
			if (decoded_buffer_start == decoded_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				encoded_buffer.cur_pos++;
				remaining_bytes_buffer.Ptr()[0] = static_cast<char>((ch & 0x3f) + 0x80);
				remaining_bytes_buffer.SetSize(1);
				return;
			}
			decoded_buffer[decoded_buffer_start++] = static_cast<char>((ch & 0x3f) + 0x80);
		}
	}
}

void CSVDecoder::DecodeInternal(char *decoded_buffer, idx_t &decoded_buffer_start, const idx_t decoded_buffer_size) {
	switch (encoding) {
	case CSVEncoding::UTF_8:
		throw InternalException("DecodeInternal() should not be called with UTF-8");
	case CSVEncoding::UTF_16:
		return DecodeUTF16(decoded_buffer, decoded_buffer_start, decoded_buffer_size);
	case CSVEncoding::LATIN_1:
		return DecodeLatin1(decoded_buffer, decoded_buffer_start, decoded_buffer_size);
	default:
		throw NotImplementedException("DecodeInternal() is not implemented for given CSVEncoding type");
	}
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
		DecodeInternal(output_buffer, output_buffer_pos, nr_bytes_to_read);
	}
	// Otherwise we read a new encoded buffer from the file
	while (output_buffer_pos < nr_bytes_to_read) {
		idx_t current_decoded_buffer_start = output_buffer_pos;
		encoded_buffer.Reset();
		auto actual_encoded_bytes =
		    static_cast<idx_t>(file_handle_input.Read(encoded_buffer.Ptr(), encoded_buffer.GetCapacity()));
		encoded_buffer.SetSize(actual_encoded_bytes);
		DecodeInternal(output_buffer, output_buffer_pos, nr_bytes_to_read);
		if (output_buffer_pos == current_decoded_buffer_start) {
			return output_buffer_pos;
		}
	}
	return output_buffer_pos;
}

} // namespace duckdb
