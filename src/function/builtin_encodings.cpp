#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/encoding_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

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

void EncodeUTF8ToLatin1(const char *input, idx_t input_size, char *output, idx_t &written) {
	idx_t out = 0;
	idx_t pos = 0;
	while (pos < input_size) {
		const auto lead = static_cast<unsigned char>(input[pos]);
		if (lead <= 0x7F) {
			output[out++] = static_cast<char>(lead);
			pos++;
			continue;
		}
		// a two byte sequence U+0080..U+07FF, valid UTF-8 is guaranteed by VARCHAR
		uint32_t code_point = 0;
		idx_t length = 1;
		if (lead >= 0xF0) {
			length = 4;
		} else if (lead >= 0xE0) {
			length = 3;
		} else {
			length = 2;
			code_point = ((lead & 0x1F) << 6) | (static_cast<unsigned char>(input[pos + 1]) & 0x3F);
		}
		if (length != 2 || code_point > 0xFF || code_point <= 0x9F) {
			throw ConversionException("Could not encode the character at byte %llu in latin-1", pos);
		}
		output[out++] = static_cast<char>(code_point);
		pos += length;
	}
	written = out;
}

void EncodeUTF8ToUTF16(const char *input, idx_t input_size, char *output, idx_t &written) {
	idx_t out = 0;
	idx_t pos = 0;
	auto store = [&](uint16_t unit) {
		output[out++] = static_cast<char>(unit & 0xFF);
		output[out++] = static_cast<char>(unit >> 8);
	};
	while (pos < input_size) {
		const auto lead = static_cast<unsigned char>(input[pos]);
		uint32_t code_point;
		idx_t length;
		if (lead <= 0x7F) {
			code_point = lead;
			length = 1;
		} else if (lead < 0xE0) {
			code_point = ((lead & 0x1F) << 6) | (static_cast<unsigned char>(input[pos + 1]) & 0x3F);
			length = 2;
		} else if (lead < 0xF0) {
			code_point = ((lead & 0x0F) << 12) | ((static_cast<unsigned char>(input[pos + 1]) & 0x3F) << 6) |
			             (static_cast<unsigned char>(input[pos + 2]) & 0x3F);
			length = 3;
		} else {
			code_point = ((lead & 0x07) << 18) | ((static_cast<unsigned char>(input[pos + 1]) & 0x3F) << 12) |
			             ((static_cast<unsigned char>(input[pos + 2]) & 0x3F) << 6) |
			             (static_cast<unsigned char>(input[pos + 3]) & 0x3F);
			length = 4;
		}
		if (code_point >= 0x10000) {
			code_point -= 0x10000;
			store(static_cast<uint16_t>(0xD800 + (code_point >> 10)));
			store(static_cast<uint16_t>(0xDC00 + (code_point & 0x3FF)));
		} else {
			store(static_cast<uint16_t>(code_point));
		}
		pos += length;
	}
	written = out;
}

void DecodeUTF16ToUTF8(const char *input, idx_t input_size, bool is_last, char *output, idx_t &consumed,
                       idx_t &written) {
	idx_t pos = 0;
	idx_t out = 0;
	while (pos < input_size) {
		if (pos + 1 >= input_size) {
			if (is_last) {
				throw InvalidInputException("Input is not valid UTF-16");
			}
			break;
		}
		const uint16_t ch = LoadUTF16(input, pos);
		if (ch >= 0xD800 && ch <= 0xDFFF) {
			if (ch >= 0xDC00) {
				// low surrogate without a preceding high surrogate
				throw InvalidInputException("Input is not valid UTF-16");
			}
			if (pos + 3 >= input_size) {
				if (is_last) {
					throw InvalidInputException("Input is not valid UTF-16");
				}
				// the low surrogate follows in the next buffer
				break;
			}
			const uint16_t low = LoadUTF16(input, pos + 2);
			if (low < 0xDC00 || low > 0xDFFF) {
				throw InvalidInputException("Input is not valid UTF-16");
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
	CreateSchemaInfo schema;
	schema.SetQualifiedName(QualifiedName({Identifier(EncodingFunction::SCHEMA)}, Identifier()));
	schema.internal = true;
	schema.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(transaction, schema);

	// Wrap the codecs in lambdas, a std::function holding a function pointer exports its typeinfo
	decode_t latin1_decode = [](const char *input, idx_t input_size, bool is_last, char *output, idx_t &consumed,
	                            idx_t &written) {
		DecodeLatin1ToUTF8(input, input_size, is_last, output, consumed, written);
	};
	encode_t latin1_encode = [](const char *input, idx_t input_size, char *output, idx_t &written) {
		EncodeUTF8ToLatin1(input, input_size, output, written);
	};
	decode_t utf16_decode = [](const char *input, idx_t input_size, bool is_last, char *output, idx_t &consumed,
	                           idx_t &written) {
		DecodeUTF16ToUTF8(input, input_size, is_last, output, consumed, written);
	};
	encode_t utf16_encode = [](const char *input, idx_t input_size, char *output, idx_t &written) {
		EncodeUTF8ToUTF16(input, input_size, output, written);
	};
	for (auto &set : {EncodingFunction::Create("latin-1", latin1_decode, latin1_encode, 1, 2),
	                  EncodingFunction::Create("utf-16", utf16_decode, utf16_encode, 4, 2)}) {
		CreateScalarFunctionInfo info(set);
		EncodingFunction::SetSchema(info);
		info.internal = true;
		catalog.CreateFunction(transaction, info);
	}
}

} // namespace duckdb
