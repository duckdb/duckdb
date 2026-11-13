#include "duckdb/common/exception.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/encoding_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

namespace {

void DecodeLatin1ToUTF8(const char *input, idx_t input_size, bool is_last, char *output, idx_t &consumed,
                        idx_t &written) {
	idx_t out = 0;
	for (idx_t i = 0; i < input_size; i++) {
		const auto ch = static_cast<unsigned char>(input[i]);
		if (ch > 0x7F && ch <= 0x9F) {
			throw InvalidInputException("File is not latin-1 encoded");
		}
		if (ch <= 0x7F) {
			output[out++] = static_cast<char>(ch);
		} else {
			output[out++] = static_cast<char>(0xc2 + (ch > 0xbf));
			output[out++] = static_cast<char>((ch & 0x3f) + 0x80);
		}
	}
	consumed = input_size;
	written = out;
}

uint16_t LoadUTF16(const char *input, idx_t pos) {
	return static_cast<uint16_t>(static_cast<unsigned char>(input[pos]) |
	                             (static_cast<unsigned char>(input[pos + 1]) << 8));
}

void DecodeUTF16ToUTF8(const char *input, idx_t input_size, bool is_last, char *output, idx_t &consumed,
                       idx_t &written) {
	idx_t pos = 0;
	idx_t out = 0;
	while (pos < input_size) {
		if (pos + 1 >= input_size) {
			if (is_last) {
				throw InvalidInputException("File is not utf-16 encoded");
			}
			break;
		}
		const uint16_t ch = LoadUTF16(input, pos);
		if (ch >= 0xD800 && ch <= 0xDFFF) {
			if (ch >= 0xDC00) {
				// low surrogate without a preceding high surrogate
				throw InvalidInputException("File is not utf-16 encoded");
			}
			if (pos + 3 >= input_size) {
				if (is_last) {
					throw InvalidInputException("File is not utf-16 encoded");
				}
				// the low surrogate follows in the next buffer
				break;
			}
			const uint16_t low = LoadUTF16(input, pos + 2);
			if (low < 0xDC00 || low > 0xDFFF) {
				throw InvalidInputException("File is not utf-16 encoded");
			}
			const uint32_t code_point = 0x10000 + ((static_cast<uint32_t>(ch - 0xD800)) << 10) + (low - 0xDC00);
			output[out++] = static_cast<char>(0xF0 | (code_point >> 18));
			output[out++] = static_cast<char>(0x80 | ((code_point >> 12) & 0x3F));
			output[out++] = static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
			output[out++] = static_cast<char>(0x80 | (code_point & 0x3F));
			pos += 4;
			continue;
		}
		if (ch <= 0x007F) {
			output[out++] = static_cast<char>(ch);
		} else if (ch <= 0x07FF) {
			output[out++] = static_cast<char>(0xC0 | (ch >> 6));
			output[out++] = static_cast<char>(0x80 | (ch & 0x3F));
		} else {
			output[out++] = static_cast<char>(0xE0 | (ch >> 12));
			output[out++] = static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
			output[out++] = static_cast<char>(0x80 | (ch & 0x3F));
		}
		pos += 2;
	}
	consumed = pos;
	written = out;
}

} // namespace

void BuiltinFunctions::RegisterEncodingFunctions() {
	for (auto &set : {EncodingFunction::Create("latin-1", DecodeLatin1ToUTF8, 1, 2),
	                  EncodingFunction::Create("utf-16", DecodeUTF16ToUTF8, 4, 2)}) {
		CreateScalarFunctionInfo info(set);
		EncodingFunction::SetSchema(info);
		info.internal = true;
		catalog.CreateFunction(transaction, info);
	}
}

} // namespace duckdb
