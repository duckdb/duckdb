#include "duckdb/function/encoding_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

struct DefaultEncodeMethod {
	string name;
	encode_t encode_function;
	idx_t ratio;
	idx_t bytes_per_iteration;
};

void DecodeUTF16ToUTF8(const char *source_buffer, idx_t &source_buffer_current_position, const idx_t source_buffer_size,
                       char *target_buffer, idx_t &target_buffer_current_position, const idx_t target_buffer_size,
                       char *remaining_bytes_buffer, idx_t &remaining_bytes_size) {

	for (; source_buffer_current_position < source_buffer_size; source_buffer_current_position += 2) {
		if (target_buffer_current_position == target_buffer_size) {
			// We are done
			return;
		}
		const uint16_t ch =
		    static_cast<uint16_t>(static_cast<unsigned char>(source_buffer[source_buffer_current_position]) |
		                          (static_cast<unsigned char>(source_buffer[source_buffer_current_position + 1]) << 8));
		if (ch >= 0xD800 && ch <= 0xDFFF) {
			throw InvalidInputException("File is not utf-16 encoded");
		}
		if (ch <= 0x007F) {
			// 1-byte UTF-8 for ASCII characters
			target_buffer[target_buffer_current_position++] = static_cast<char>(ch & 0x7F);
		} else if (ch <= 0x07FF) {
			// 2-byte UTF-8
			target_buffer[target_buffer_current_position++] = static_cast<char>(0xC0 | (ch >> 6));
			if (target_buffer_current_position == target_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				source_buffer_current_position += 2;
				remaining_bytes_buffer[0] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_size = 1;
				return;
			}
			target_buffer[target_buffer_current_position++] = static_cast<char>(0x80 | (ch & 0x3F));
		} else {
			// 3-byte UTF-8
			target_buffer[target_buffer_current_position++] = static_cast<char>(0xE0 | (ch >> 12));
			if (target_buffer_current_position == target_buffer_size) {
				// We are done, but we have to store two bytes for the next chunk!
				source_buffer_current_position += 2;
				remaining_bytes_buffer[0] = static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
				remaining_bytes_buffer[1] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_size = 2;
				return;
			}
			target_buffer[target_buffer_current_position++] = static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
			if (target_buffer_current_position == target_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				source_buffer_current_position += 2;
				remaining_bytes_buffer[0] = static_cast<char>(0x80 | (ch & 0x3F));
				remaining_bytes_size = 1;
				return;
			}
			target_buffer[target_buffer_current_position++] = static_cast<char>(0x80 | (ch & 0x3F));
		}
	}
}

void DecodeLatin1ToUTF8(const char *source_buffer, idx_t &source_buffer_current_position,
                        const idx_t source_buffer_size, char *target_buffer, idx_t &target_buffer_current_position,
                        const idx_t target_buffer_size, char *remaining_bytes_buffer, idx_t &remaining_bytes_size) {
	for (; source_buffer_current_position < source_buffer_size; source_buffer_current_position++) {
		if (target_buffer_current_position == target_buffer_size) {
			// We are done
			return;
		}
		const unsigned char ch = static_cast<unsigned char>(source_buffer[source_buffer_current_position]);
		if (ch > 0x7F && ch <= 0x9F) {
			throw InvalidInputException("File is not latin-1 encoded");
		}
		if (ch <= 0x7F) {
			// ASCII: 1 byte in UTF-8
			target_buffer[target_buffer_current_position++] = static_cast<char>(ch);
		} else {
			// Non-ASCII: 2 bytes in UTF-8
			target_buffer[target_buffer_current_position++] = static_cast<char>(0xc2 + (ch > 0xbf));
			if (target_buffer_current_position == target_buffer_size) {
				// We are done, but we have to store one byte for the next chunk!
				source_buffer_current_position++;
				remaining_bytes_buffer[0] = static_cast<char>((ch & 0x3f) + 0x80);
				remaining_bytes_size = 1;
				return;
			}
			target_buffer[target_buffer_current_position++] = static_cast<char>((ch & 0x3f) + 0x80);
		}
	}
}

void DecodeUTF8(const char *source_buffer, idx_t &source_buffer_current_position, const idx_t source_buffer_size,
                char *target_buffer, idx_t &target_buffer_current_position, const idx_t target_buffer_size,
                char *remaining_bytes_buffer, idx_t &remaining_bytes_size) {
	throw InternalException("Decode UTF8 is not a valid function, and should be verified one level up.");
}

void EncodingFunctionSet::Initialize(DBConfig &config) {
	config.RegisterEncodeFunction({"utf-8", DecodeUTF8, 1, 1});
	config.RegisterEncodeFunction({"latin-1", DecodeLatin1ToUTF8, 2, 1});
	config.RegisterEncodeFunction({"utf-16", DecodeUTF16ToUTF8, 2, 2});
}

void DBConfig::RegisterEncodeFunction(const EncodingFunction &function) const {
	lock_guard<mutex> l(encoding_functions->lock);
	const auto decode_type = function.GetType();
	if (encoding_functions->functions.find(decode_type) != encoding_functions->functions.end()) {
		throw InvalidInputException("Decoding function with name %s already registered", decode_type);
	}
	encoding_functions->functions[decode_type] = function;
}

optional_ptr<EncodingFunction> DBConfig::GetEncodeFunction(const string &name) const {
	lock_guard<mutex> l(encoding_functions->lock);
	// Check if the function is already loaded into the global compression functions.
	if (encoding_functions->functions.find(name) != encoding_functions->functions.end()) {
		return &encoding_functions->functions[name];
	}
	return nullptr;
}

vector<reference<EncodingFunction>> DBConfig::GetLoadedEncodedFunctions() const {
	lock_guard<mutex> l(encoding_functions->lock);
	vector<reference<EncodingFunction>> result;
	for (auto &function : encoding_functions->functions) {
		result.push_back(function.second);
	}
	return result;
}
} // namespace duckdb
