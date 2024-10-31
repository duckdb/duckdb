#include "duckdb/function/decoding_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

struct DefaultDecodeMethod {
	string name;
	decode_t decode_function;
	idx_t ratio;
	idx_t bytes_per_iteration;
};

void DecodeUTF16(char *encoded_buffer, idx_t &encoded_buffer_current_position, const idx_t encoded_buffer_size,
                 char *decoded_buffer, idx_t &decoded_buffer_current_position, const idx_t decoded_buffer_size,
                 char *remaining_bytes_buffer, idx_t &remaining_bytes_size) {

	for (; encoded_buffer_current_position < encoded_buffer_size; encoded_buffer_current_position += 2) {
		if (decoded_buffer_current_position == decoded_buffer_size) {
			// We are done
			return;
		}
		const uint16_t ch = static_cast<uint16_t>(
		    static_cast<unsigned char>(encoded_buffer[encoded_buffer_current_position]) |
		    (static_cast<unsigned char>(encoded_buffer[encoded_buffer_current_position + 1]) << 8));
		if (ch >= 0xD800 && ch <= 0xDFFF) {
			throw InvalidInputException("File is not utf-16 encoded");
		}
		if (ch <= 0x007F) {
			// 1-byte UTF-8 for ASCII characters
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>(ch & 0x7F);
		} else if (ch <= 0x07FF) {
			// 2-byte UTF-8
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>(0xC0 | (ch >> 6));
			if (decoded_buffer_current_position == decoded_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				encoded_buffer_current_position += 2;
				remaining_bytes_buffer[0] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_size = 1;
				return;
			}
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>(0x80 | (ch & 0x3F));
		} else {
			// 3-byte UTF-8
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>(0xE0 | (ch >> 12));
			if (decoded_buffer_current_position == decoded_buffer_size) {
				// We are done, but we have to store two bytes for the next chunk!
				encoded_buffer_current_position += 2;
				remaining_bytes_buffer[0] = static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
				remaining_bytes_buffer[1] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_size = 2;
				return;
			}
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
			if (decoded_buffer_current_position == decoded_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				encoded_buffer_current_position += 2;
				remaining_bytes_buffer[0] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_size = 1;
				return;
			}
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>(0x80 | (ch & 0x3F));
		}
	}
}

void DecodeLatin1(char *encoded_buffer, idx_t &encoded_buffer_current_position, const idx_t encoded_buffer_size,
                  char *decoded_buffer, idx_t &decoded_buffer_current_position, const idx_t decoded_buffer_size,
                  char *remaining_bytes_buffer, idx_t &remaining_bytes_size) {
	for (; encoded_buffer_current_position < encoded_buffer_size; encoded_buffer_current_position++) {
		if (decoded_buffer_current_position == decoded_buffer_size) {
			// We are done
			return;
		}
		const unsigned char ch = static_cast<unsigned char>(encoded_buffer[encoded_buffer_current_position]);
		if (ch > 0x7F && ch <= 0x9F) {
			throw InvalidInputException("File is not latin-1 encoded");
		}
		if (ch <= 0x7F) {
			// ASCII: 1 byte in UTF-8
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>(ch);
		} else {
			// Non-ASCII: 2 bytes in UTF-8
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>(0xc2 + (ch > 0xbf));
			if (decoded_buffer_current_position == decoded_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				encoded_buffer_current_position++;
				remaining_bytes_buffer[0] = static_cast<char>((ch & 0x3f) + 0x80);
				remaining_bytes_size = 1;
				return;
			}
			decoded_buffer[decoded_buffer_current_position++] = static_cast<char>((ch & 0x3f) + 0x80);
		}
	}
}

void DecodeUTF8(char *encoded_buffer, idx_t &encoded_buffer_current_position, const idx_t encoded_buffer_size,
                char *decoded_buffer, idx_t &decoded_buffer_current_position, const idx_t decoded_buffer_size,
                char *remaining_bytes_buffer, idx_t &remaining_bytes_size) {
	// This function just copies.
	for (; encoded_buffer_current_position < encoded_buffer_size; encoded_buffer_current_position++) {
		if (decoded_buffer_current_position == decoded_buffer_size) {
			// We are done
			return;
		}
		decoded_buffer[decoded_buffer_current_position++] = encoded_buffer[encoded_buffer_current_position];
	}
}

static const DecodingFunction internal_decode_methods[] = {
    {"utf-8", DecodeLatin1, 1, 0}, {"latin-1", DecodeLatin1, 2, 1}, {"utf-16", DecodeUTF16, 2, 2}};

void DecodingFunctionSet::Initialize(DBConfig &config) {
	for (auto &decode_method : internal_decode_methods) {
		config.RegisterDecodeFunction(decode_method);
	}
}

void DBConfig::RegisterDecodeFunction(const DecodingFunction &function) const {
	lock_guard<mutex> l(decoding_functions->lock);
	const auto decode_type = function.GetType();
	if (decoding_functions->functions.find(decode_type) != decoding_functions->functions.end()) {
		throw InvalidInputException("Decoding function with name %s already registered", decode_type);
	}
	decoding_functions->functions[decode_type] = function;
}

optional_ptr<DecodingFunction> DBConfig::GetDecodeFunction(string name) const {
	lock_guard<mutex> l(decoding_functions->lock);
	name = StringUtil::Lower(name);
	// Check if the function is already loaded into the global compression functions.
	if (decoding_functions->functions.find(name) != decoding_functions->functions.end()) {
		return &decoding_functions->functions[name];
	}
	return nullptr;
}

vector<string> DBConfig::GetLoadedDecodeFunctionNames() const {
	lock_guard<mutex> l(decoding_functions->lock);
	vector<string> result;
	for (auto &function : decoding_functions->functions) {
		result.push_back(function.first);
	}
	return result;
}
} // namespace duckdb
